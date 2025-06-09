# -*- coding: utf-8 -*-
"""
Created on Sat Sep 23 08:02:24 2023

@author: ruppert20
"""
from typing import Union, List, Dict
import os
from pandas import pandas as pd
from scipy.stats import median_abs_deviation as mad
import json
from tqdm import tqdm
import re
import dask
from math import sqrt
from ..PreProcessing.data_format_and_manipulation import sanatize_columns, notnull, remove_illegal_characters, deduplicate_and_join, coalesce, get_file_name_components
from ..FileHandling.io import save_data, check_format_series, detect_file_names,\
    check_load_df, get_batches_from_directory, find_files
from ..PreProcessing.standardization_functions import _get_column_type
from ..General.func_utils import debug_inputs
from ..ResourceManagement.parallelization_helper import run_function_in_parallel_v2
tqdm.pandas()

def summarize(ds: Union[pd.Series, pd.DataFrame], dtype: Union[dict, str, None] = None, index: Union[None, pd.Series] = None,
              return_formatted: bool = False, default_one_hot_threshold: int = 5, downcast_dates: bool = True,
              unit: Union[Dict[str, str], str, None] = None,
              stats_by_patient_col:  Union[str, None] = None,
              stats_by_encounter_col: Union[str, None] = None,
              stats_by_or_encounter_col: Union[str, None] = None,
              stats_by_order_col: Union[str, None] = None,
              stats_by_hospital_account_col: Union[str, None] = None,
              include_patient_ids: bool = True,
              include_encounter_ids: bool = False,
              **format_kwargs) -> Union[tuple, dict, pd.Series]:
    """
    Summarize a Pandas DataFrame or Pandas Series.

    Parameters
    ----------
    ds : Union[pd.Series, pd.DataFrame]
        Input pandas dataframe or pandas series.
    dtype : Union[dict, str, None], optional
        A dictionary of desired datatypes per column or fixed data type (str) or None. The default is None.If None, the optimal type will be inferred.
    index : Union[None, pd.Series], optional
        Index of rows in which you wish for statistics from. The default is None, which will include all rows.
    return_formatted : bool, optional
        Whether the formatted series should be returned along with the statistics. The default is False, which will only return the statistics.
    default_one_hot_threshold : int, optional
        The default threshold to differentiate a one_hot_embedded column and a categorically embeded column. The default is 5.
    downcast_dates: bool, optional
        Whether inferred datetime objects should be set as type date if all the timestamps are midnight. The default is True.
    unit: Union[Dict[str, str], str, None], optional
        The unit that corresponds to the particular series. The default is None which will not display a unit.
    **format_kwargs : TYPE
        Key word arguments passed to the check_format_series function from Utilities.FileHandling.io.

    Returns
    -------
    Dictionary (if input is series and return_formatted == false)
    Tuple pd.Series, dict (if input is series and return formatted == True)
    Tuple pd.DataFrame, pd.DataFrame (if input is dataframe and return formatted == True)
    pd.DataFrame (if input is dataframe and return_formatted == false)


    """
    assert isinstance(ds, (pd.Series, pd.DataFrame)), f'The input for paramter, ds must be a pandas series or dataframe; however, a {type(ds)} was found.'
    if isinstance(ds, pd.DataFrame):
        if return_formatted:
            raise NotImplementedError('Need to implement processing of dataframes returning formatted values and statistics')
            
        out: pd.DataFrame = ds.progress_apply(lambda col: summarize(ds=col, dtype=dtype.get(col.name) if isinstance(dtype, dict) else dtype, index=index,
                                                           return_formatted=return_formatted, unit=unit.get(col.name) if isinstance(unit, dict) else unit,
                                                           stats_by_patient_col=stats_by_patient_col, stats_by_encounter_col=stats_by_encounter_col,
                                                           stats_by_or_encounter_col=stats_by_or_encounter_col, include_patient_ids=include_patient_ids,
                                                           stats_by_order_col=stats_by_order_col, include_encounter_ids=include_encounter_ids,
                                                           stats_by_hospital_account_col=stats_by_hospital_account_col, **format_kwargs), axis=0)\
            .apply(_format_summary_for_df).reset_index(drop=False).rename(columns={'index': 'raw_column_name'})

        out['sql_dtype'] = out.apply(_sql_datatype, axis=1)

        out['clean_column_name'] = sanatize_columns(df=pd.DataFrame(columns=out.raw_column_name), preserve_case=False, preserve_decimals=False).columns.tolist()

        out.rename(columns={'dtype': 'ml_dtype'}, inplace=True)

        return out[[x for x in ['raw_column_name', 'clean_column_name', 'sql_dtype', 'ml_dtype', 'value_counts',
                                'value_counts_by_patient', 'value_counts_by_encounter', 'value_counts_by_or_encounter',
                                'vale_counts_by_order', 'value_counts_by_hospital_account', 'n_patients', 'patients', 'n_encounters',
                                'encounters', 'n_or_encounters', 'n_orders', 'n_hospital_accounts',
                                'levels', 'min', 'max', 'mode', 'mean', 'median', 'std', 'mad', 'nunique', 'count'] if x in out.columns]]

    if index is None:
        index: pd.Series = pd.Series([True] * ds.shape[0], index=ds.index)

    dtype: str = dtype if isinstance(dtype, str) else _get_column_type(series=ds, one_hot_threshold=format_kwargs.get('one_hot_threshold', default_one_hot_threshold),
                                                                       downcast_dates=downcast_dates)
    ds: pd.Series = check_format_series(ds=ds,
                                        desired_type=dtype,
                                        **format_kwargs)

    stat_dict: dict = {}
    try:
        stat_dict['mode'] = ds[index].mode(dropna=True).iloc[0]
    except IndexError:
        stat_dict['mode'] = None

    if dtype in ['binary', 'cat_one_hot', 'cat_embedding', 'cat_str', 'str', 'object']:
        # calculate levels and value counts for categorical and one_hot columns
        stat_dict['value_counts'] = ds[index].value_counts(dropna=False)
        stat_dict['levels'] = stat_dict['value_counts'].index.tolist()

        try:
            char_lens: pd.Series = ds[index].str.len()
            stat_dict['min'] = char_lens.min()
            stat_dict['max'] = char_lens.max()
        except:
            pass
        
        if isinstance(stats_by_patient_col, str) and ((stats_by_patient_col == ds.index.name) or (stats_by_patient_col in ds.index.names)) :
            stat_dict['value_counts_by_patient'] = ds[index].fillna('XXXMISSINGXXX').reset_index(level=stats_by_patient_col).groupby(ds.name, group_keys=False)[stats_by_patient_col].nunique().sort_values(ascending=False).rename(index={'XXXMISSINGXXX': 'null'})
        if isinstance(stats_by_encounter_col, str) and ((stats_by_encounter_col == ds.index.name) or (stats_by_encounter_col in ds.index.names)) :
            stat_dict['value_counts_by_encounter'] = ds[index].fillna('XXXMISSINGXXX').reset_index(level=stats_by_encounter_col).groupby(ds.name, group_keys=False)[stats_by_encounter_col].nunique().sort_values(ascending=False).rename(index={'XXXMISSINGXXX': 'null'})
        if isinstance(stats_by_or_encounter_col, str) and ((stats_by_or_encounter_col == ds.index.name) or (stats_by_or_encounter_col in ds.index.names)) :
            stat_dict['value_counts_by_or_encounter'] = ds[index].fillna('XXXMISSINGXXX').reset_index(level=stats_by_or_encounter_col).groupby(ds.name, group_keys=False)[stats_by_or_encounter_col].nunique().sort_values(ascending=False).rename(index={'XXXMISSINGXXX': 'null'})
        if isinstance(stats_by_order_col, str) and ((stats_by_order_col == ds.index.name) or (stats_by_order_col in ds.index.names)) :
            stat_dict['value_counts_by_order'] = ds[index].fillna('XXXMISSINGXXX').reset_index(level=stats_by_order_col).groupby(ds.name, group_keys=False)[stats_by_order_col].nunique().sort_values(ascending=False).rename(index={'XXXMISSINGXXX': 'null'})
        if isinstance(stats_by_hospital_account_col, str) and ((stats_by_hospital_account_col == ds.index.name) or (stats_by_hospital_account_col in ds.index.names)) :
            stat_dict['value_counts_by_hospital_account'] = ds[index].fillna('XXXMISSINGXXX').reset_index(level=stats_by_hospital_account_col).groupby(ds.name, group_keys=False)[stats_by_hospital_account_col].nunique().sort_values(ascending=False).rename(index={'XXXMISSINGXXX': 'null'})
            

    elif dtype in ['float', 'int']:
        # calculate mean, median, std, mad (median absolute deviance), min, and max for numeric types
        stat_dict['mean'] = ds[index].mean(skipna=True)
        stat_dict['median'] = ds[index].median(skipna=True)
        stat_dict['std'] = ds[index].std(skipna=True)
        stat_dict['mad'] = mad(ds[index], nan_policy='omit', scale='normal')
        stat_dict['min'] = ds[index].min(skipna=True)
        stat_dict['max'] = ds[index].max(skipna=True)
    elif dtype in ['datetime', 'date']:
        stat_dict['min'] = ds.dropna().min() if dtype == 'date' else ds[index].min(skipna=True)
        stat_dict['max'] = ds.dropna().max() if dtype == 'date' else ds[index].max(skipna=True)

    stat_dict['nunique'] = ds[index].nunique(dropna=True)
    stat_dict['count'] = ds[index].dropna().shape[0]
    
    if dtype not in ['datetime', 'date']:
        if isinstance(stats_by_patient_col, str) and ((stats_by_patient_col == ds.index.name) or (stats_by_patient_col in ds.index.names)):
            stat_dict['n_patients'] = ds[index].reset_index(level=stats_by_patient_col)[stats_by_patient_col].nunique()
            if include_patient_ids:
                stat_dict['patients'] = ds[index].reset_index(level=stats_by_patient_col)[stats_by_patient_col].dropna().drop_duplicates()
        if isinstance(stats_by_encounter_col, str) and ((stats_by_encounter_col == ds.index.name) or (stats_by_encounter_col in ds.index.names)):
            stat_dict['n_encounters'] = ds[index].reset_index(level=stats_by_encounter_col)[stats_by_encounter_col].nunique()
            if include_encounter_ids:
                stat_dict['encounters'] = ds[index].reset_index(level=stats_by_encounter_col)[stats_by_encounter_col].dropna().drop_duplicates()
        if isinstance(stats_by_or_encounter_col, str) and ((stats_by_or_encounter_col == ds.index.name) or (stats_by_or_encounter_col in ds.index.names)):
            stat_dict['n_or_encounters'] = ds[index].reset_index(level=stats_by_or_encounter_col)[stats_by_or_encounter_col].nunique()
        if isinstance(stats_by_order_col, str) and ((stats_by_order_col == ds.index.name) or (stats_by_order_col in ds.index.names)):
            stat_dict['n_orders'] = ds[index].reset_index(level=stats_by_order_col)[stats_by_order_col].nunique()
        if isinstance(stats_by_hospital_account_col, str) and ((stats_by_hospital_account_col == ds.index.name) or (stats_by_hospital_account_col in ds.index.names)):
            stat_dict['n_hospital_accounts'] = ds[index].reset_index(level=stats_by_hospital_account_col)[stats_by_hospital_account_col].nunique()

    stat_dict['dtype'] = dtype

    if isinstance(unit, str):
        stat_dict['unit'] = unit        

    if return_formatted:
        return ds, stat_dict

    return stat_dict


def _format_summary_for_df(stat_dict: dict) -> pd.Series:
    if pd.isnull(stat_dict):
        return pd.Series({'dtype': 'BLANK'})

    if 'levels' in stat_dict:
        stat_dict['levels'] = 'XXXSEPXXX'.join([str(x) for x in stat_dict['levels']])
    if 'value_counts' in stat_dict:
        stat_dict['value_counts'] = json.dumps(stat_dict['value_counts'].to_dict())
    if 'value_counts_by_patient' in stat_dict:
        stat_dict['value_counts_by_patient'] = json.dumps(stat_dict['value_counts_by_patient'].to_dict())
    if 'value_counts_by_encounter' in stat_dict:
        stat_dict['value_counts_by_encounter'] = json.dumps(stat_dict['value_counts_by_encounter'].to_dict())
    if 'value_counts_by_or_encounter' in stat_dict:
        stat_dict['value_counts_by_or_encounter'] = json.dumps(stat_dict['value_counts_by_or_encounter'].to_dict())
    if 'value_counts_by_hospital_account' in stat_dict:
        stat_dict['value_counts_by_hospital_account'] = json.dumps(stat_dict['value_counts_by_hospital_account'].to_dict())
    if 'patients' in stat_dict:
        stat_dict['patients'] = '|'.join([str(x) for x in stat_dict['patients']])
    if 'encounters' in stat_dict:
        stat_dict['encounters'] = '|'.join([str(x) for x in stat_dict['encounters']])

    return pd.Series(stat_dict)


def _sql_datatype(row: pd.Series) -> str:
    if row['dtype'] == 'binary':
        return 'BIT'
    elif row['dtype'] in ['datetime']:
        return 'DATETIME2(0)'
    elif row['dtype'] in ['date']:
        return 'DATE'
    elif row['dtype'] in ['int', 'int32', 'int64', 'int16', 'int8']:
        return 'INTEGER'
    elif row['dtype'] in ['float', 'float32', 'float16', 'float64', 'decimal']:
        return 'FLOAT'
    elif row['dtype'] in ['cat_one_hot', 'cat_embedding', 'cat_str', 'str', 'object']:
        if pd.isnull(row['max']):
            return 'VARCHAR(?)'
        return f'VARCHAR({int(row["max"])})' if row['max'] > row['min'] else f'CHAR({int(row["max"])})'

def _summarize_labs(df: pd.DataFrame,
                    lab_id_grouping_cols: List[str], file: str,
                    interim_result_dir: str,
                    batch: str,
                    dtype: Union[dict, str, None] = None,
                    units: Union[dict, str, None] = None,
                    **summarization_kwargs):
    if notnull(df.index.names):
        if len([x for x in df.index.names if x in lab_id_grouping_cols]) == len(lab_id_grouping_cols):
            
            lab_summary = df.groupby(level=lab_id_grouping_cols, group_keys=False).progress_apply(summarize, dtype=dtype.get(file) if isinstance(dtype, dict) else dtype,
                                                                              unit=units.get(file) if isinstance(units, dict) else units,
                                                                              **summarization_kwargs)\
                .reset_index(drop=False)
                
            lab_summary.to_pickle(os.path.join(interim_result_dir, f'{file}_{batch}_lab_summary.p'))
            
            return lab_summary



    
def extract_median_timestamp(df: pd.DataFrame, index_col: str, timestamp_col: str) -> pd.DataFrame:
    assert timestamp_col in df.columns
    
    assert (index_col in df.columns) or (index_col in (df.index.names if notnull(df.index.names) else [])) or (index_col == str(df.index.name))
    
    df = check_load_df(df[df.columns.intersection([index_col, timestamp_col])].copy(deep=True), desired_types={timestamp_col: 'datetime'})
    
    if index_col not in df.columns:
        df = df.reset_index(level=index_col, drop=False)
    
    df[index_col] = df[index_col].replace({'XXXMISSINGXXX': None})
    
    # filter based on time interval supported by unix_ns_epoch
    df = df[df[timestamp_col].between('1905-01-01', '2200-01-01')]
    
    return df.dropna(subset=[index_col]).groupby(index_col, group_keys=False).agg({timestamp_col: 'median'}).reset_index(drop=False).rename(columns={timestamp_col: 'median_timestamp'})


def extract_timestamp_stats(df: pd.DataFrame, index_col: str, timestamp_col: str, stats_to_compute: List[str] = ['median', 'min', 'max']) -> pd.DataFrame:
    
    assert timestamp_col in df.columns
    
    assert (index_col in df.columns) or (index_col in (df.index.names if notnull(df.index.names) else [])) or (index_col == str(df.index.name))
    
    assert isinstance(stats_to_compute, list)
    
    df = check_load_df(df[df.columns.intersection([index_col, timestamp_col])].copy(deep=True), desired_types={timestamp_col: 'datetime'})
    
    if index_col not in df.columns:
        df = df.reset_index(level=index_col, drop=False)
    
    df[index_col] = df[index_col].replace({'XXXMISSINGXXX': None})
    
    # filter based on time interval supported by unix_ns_epoch
    df = df[df[timestamp_col].between('1905-01-01', '2200-01-01')]
    
    for i, stat in enumerate(stats_to_compute):
        if i == (len(stats_to_compute) - 1):
            df.rename(columns={timestamp_col: f'{stat}_timestamp'}, inplace=True)
        else:
            df[f'{stat}_timestamp'] = df[timestamp_col].values
        
    
    return df.dropna(subset=[index_col]).groupby(index_col, group_keys=False).agg({f'{stat}_timestamp': stat for stat in stats_to_compute}).reset_index(drop=False)
    

def summarize_directory(directory: str,
                        interim_result_dir: str,
                        max_workers: int,
                        exclusion_patterns: Union[List[str], None] = None,
                        files_to_ignore: Union[List[str], None] = None,
                        # columns_to_ignore: Union[Dict[str, List[str]], List[str], None] = None,
                        stacked_name_value_cols: Union[Dict[str, Dict[str, str]], None] = None,
                        dtype: Union[dict, str, List[str]] = None,
                        units: Union[Dict[str, Dict[str, str]], None] = None,
                        patient_id_col: str = 'patient_deiden_id',
                        encounter_col: str = 'encounter_deiden_id',
                        or_encounter_col: str = 'or_case_num_deiden_id',
                        hospital_account_col: str = 'hospital_account_deiden_id',
                        order_num_cols: List[str] = ['order_num_deiden_id', 'med_order_num_deiden_id'],
                        lab_id_grouping_cols: List[str] = ['loinc_code'],
                        primary_file_name: str = 'clinical_encounters',
                        recursive: bool = False,
                        skip_summary: bool = False,
                        additional_processing_dict: Dict[str, Dict[str, any]] = {'labs': {'function': '_get_augment_lab_lookup',
                                                                                          'lab_lookup_fp': 'path here',
                                                                                          'lab_id_grouping_cols': ['loinc_code']},
                                                                                 'meds': {'function': 'func',
                                                                                          'med_lookup_fp': 'path here'}},
                        batches: Union[None, List[str]] = None,
                        file_type: str = '.parquet',
                        include_patient_ids: bool = True,
                        include_encounter_ids: bool = False,
                        file_loading_kwargs: Union[dict, None] = None,
                        levels_to_display: int = 250,
                        median_timestamp_dict: Union[Dict[str, str], None] = None,
                        dt_col_dict: Union[Dict[str, str], None] = None,
                        **loading_kwargs) -> pd.DataFrame:

    # debug_inputs(function=summarize_directory, kwargs=locals(), dump_fp="summarize_directory.pkl")
    # raise Exception('stop here')
    
    # make list of batches
    if not isinstance(batches, list):
        batches: List[str] = get_batches_from_directory(directory=directory,
                                                        batches=batches,
                                                        file_name='^{}'.format(primary_file_name),
                                                        file_type=file_type,
                                                        independent_sub_batches=True)
    
    # make list of files to analyze
    file_list: List[str] = detect_file_names(directory=directory, recursive=recursive, pattern=f'*{file_type}', omit_file_names=files_to_ignore, regex=False)

    # # make list of columns in each file
    # column_names: pd.DataFrame = get_column_names(directory=directory, pattern=[], recursive=recursive, regex=regex)
    # column_names = column_names.loc[:, ~column_names.columns.duplicated()].copy()

    # out: List[pd.DataFrame] = []
    # additional_processed_files_output: Dict[str, any] = {}
    # lab_summary: pd.DataFrame = None
    stacked_name_value_cols: dict = stacked_name_value_cols if isinstance(stacked_name_value_cols, dict) else {}
    median_timestamp_dict: dict = median_timestamp_dict if isinstance(median_timestamp_dict, dict) else {}
    file_loading_kwargs: dict = file_loading_kwargs if isinstance(file_loading_kwargs, dict) else {}
    
    

    for batch in tqdm(batches, desc=f'Summarizing {len(batches)} Batches'):
        kw_list: List[dict] = [{'additional_processing_dict': additional_processing_dict,
                                'file': file,
                                'batch': batch,
                                'interim_result_dir': interim_result_dir,
                                'median_timestamp_dict': median_timestamp_dict,
                                'directory': directory,
                                'file_type': file_type,
                                'recursive': recursive,
                                'patient_id_col': patient_id_col,
                                'or_encounter_col': or_encounter_col,
                                'file_loading_kwargs': file_loading_kwargs,
                                'loading_kwargs': loading_kwargs,
                                'skip_summary': skip_summary,
                                'order_num_cols': order_num_cols,
                                'hospital_account_col': hospital_account_col,
                                'encounter_col': encounter_col,
                                'dtype': dtype,
                                'units': units,
                                'include_patient_ids': include_patient_ids,
                                'include_encounter_ids': include_encounter_ids,
                                'stacked_name_value_cols': stacked_name_value_cols,
                                'lab_id_grouping_cols': lab_id_grouping_cols,
                                'exclusion_patterns': exclusion_patterns} for file in file_list]

        run_function_in_parallel_v2(function=_sumarize_file, kwargs_list=kw_list,
                                    disp_updates=False,
                                    max_workers=max_workers,
                                    log_name=f'Summarize Batch {batch}')
            
                    
    if skip_summary:
        return
    output: List[pd.DataFrame] = []
    
    for file in tqdm(detect_file_names(directory=interim_result_dir,
                                              recursive=False,
                                              pattern='*.p',
                                              regex=False,
                                              exclusion_patterns=['*detail*', '*lookup*', '*summary*', 'final_audit_report.p']),
                      desc='Creating report'):
        print(file)
        stacked_so: Union[re.Match, None] = re.search(r'^(' + r')_|^('.join(list(stacked_name_value_cols.keys())) + r')_', file)
        if isinstance(stacked_so, re.Match):
            lookup_file_name: str = coalesce(*stacked_so.groups())
        else:
            lookup_file_name: str = file
            
        try:
            if file[-5:] == '1_and':
                file += '_2'
            elif file[-5:] == '3_and':
                file += '_4'
        except:
            pass
            
        try:
            output.append(_combine_reports(*[_format_summary(df=pd.read_pickle(f),
                                levels_to_display=None,
                                date_time_col=dt_col_dict.get(lookup_file_name),
                                stacked_measure_name_col=stacked_name_value_cols.get(lookup_file_name, {}).get('name'),
                                value_col=stacked_name_value_cols.get(lookup_file_name, {}).get('value'),
                                unit_col=None) for f in find_files(directory=interim_result_dir, patterns=[r'{}_[0-9_]+\.p'.format(file)], regex=True)], levels_to_display=levels_to_display))
        except:
            open(f'problem_file_{file}', 'a').close()
        
    output: pd.DataFrame = pd.concat(output)
    output.to_pickle(os.path.join(interim_result_dir, 'final_audit_report.p'))
    return output
            

def run_patient_census(directory: str, file_type: str = '.parquet', max_workers: int = 10, skip_daily_io: bool = True, include_source: bool = True, **logging_kwargs) -> pd.DataFrame:
    pt_report = _make_patient_census(directory=directory, file_type=file_type, max_workers=max_workers, include_source=include_source, **logging_kwargs)
    if not skip_daily_io:
        daily_io_report = _process_dailyio_dask_census(directory=directory, file_type=file_type, include_source=include_source, **logging_kwargs)
    temp = []

    for f, df in pt_report.items():
        df['source_file'] = f
        temp.append(df)
    if not skip_daily_io:    
        daily_io_report['source_file'] = 'daily_io'
        temp.append(daily_io_report)

    final_pt_report = pd.concat(temp, axis=0, ignore_index=True)\
        .groupby('patient_deiden_id', group_keys=False)\
        .agg({'source_batch': deduplicate_and_join, 'source_file': deduplicate_and_join})\
        .reset_index(drop=False)
        
    final_pt_report['num_files'] = final_pt_report['source_file'].apply(lambda x: len(x.split('|')))
    
    return final_pt_report

def run_rapid_census(directory: str, file_type: str, include_source: bool = False, **logging_kwargs):
    return _process_dask_census(directory=directory, file_type=file_type, include_source=include_source, **logging_kwargs).drop_duplicates()


def split_files(source_directory: str, n_splits: int, batch_split_dir: str, save_dir: str):
    for file in tqdm(find_files(directory=source_directory, patterns=[r'clinical_encounters_[0-9]+\.csv'], regex=True, recursive=False), desc='Splitting Encounter Files'):
        save_data(df=check_load_df(file, clean_ids=True),
                  original_path=file,
                  out_dir=save_dir,
                  split_into_n_batches=n_splits,
                  split_by_indentifer_col='patient_deiden_id',
                  split_by_indentifier_batch_dir=batch_split_dir,
                  file_type='.parquet')

    for file in tqdm(find_files(directory=source_directory, patterns=[r'_[0-9]+\.csv'], exclusion_patterns=['clinical_encounters'], regex=True, recursive=False), desc='Splitting Files'):
        print(file)
        save_data(df=check_load_df(file, clean_ids=True),
                  original_path=file,
                  out_dir=save_dir,
                  split_into_n_batches=n_splits,
                  split_by_indentifer_col='patient_deiden_id',
                  split_by_indentifier_batch_dir=batch_split_dir,
                  file_type='.parquet')

    for file in tqdm(find_files(directory=save_dir, patterns=[r'_[0-9]+_[0-9]+\.parquet'], regex=True, recursive=False), desc='Renaming Files'):
        os.rename(file, file[:-11] + str(int(file[-11] + file[-9])) + file[-8:])



def _process_patient_census_file(df: pd.DataFrame, include_source: bool = True) -> pd.DataFrame:
    df = df.dropna().drop_duplicates()
    if include_source:
        df['source_file'] = df['source_file'].apply(lambda x: get_file_name_components(x).batch_numbers[0])
    if isinstance(df, dask.dataframe.core.DataFrame):
        df['patient_deiden_id'] = df['patient_deiden_id'].str.extract(r'_([0-9]+)$', expand=False).astype(int).astype(str)
    
    return df.compute() if isinstance(df, dask.dataframe.core.DataFrame) else df
    

def _process_dailyio_dask_census(directory: str, file_type: str, include_source: bool = True, **logging_kwargs):
    
    return pd.concat([check_load_df(f, usecols=['patient_deiden_id'], tag_source=include_source,
                          max_workers=1, dtype=str, executor_type='ProcessPool', use_dask=True, allow_empty_files=True,
                          func_after_loading=_process_patient_census_file,
                          func_after_loading_kwargs={'include_source': include_source}, **logging_kwargs).drop_duplicates().rename(columns={'source_file': 'source_batch'}) for f in tqdm(find_files(directory=directory, patterns=[r'daily_io_[0-9_]+\{}'.format(file_type)], regex=True), desc='Peforming Patient Census on Daily IO with Dask')], axis=0, ignore_index=True)

def _process_dask_census(directory: str, file_type: str, include_source: bool = True, **logging_kwargs):
    
    return pd.concat([check_load_df(f, usecols=['patient_deiden_id'], tag_source=include_source,
                          max_workers=1, dtype=str, executor_type='ProcessPool', use_dask=True, allow_empty_files=True,
                          func_after_loading=_process_patient_census_file,
                          func_after_loading_kwargs={'include_source': include_source}, **logging_kwargs).drop_duplicates().rename(columns={'source_file': 'source_batch'}) for f in tqdm(find_files(directory=directory, patterns=[r'_[0-9_]+\{}'.format(file_type)], exclusion_patterns=['daily_io', 'provider_info'], regex=True, recursive=False), desc='Peforming Patient Census with Dask')], axis=0, ignore_index=True)


def _make_patient_census(directory: str, file_type: str, regex: bool = True, recursive: bool = False, use_dask: bool = False, max_workers: int = 10, include_source: bool = True, **logging_kwargs) -> pd.DataFrame:
    output: Dict[str, pd.DataFrame] = {}

    for file in tqdm(detect_file_names(directory=directory, recursive=recursive, regex=regex, pattern=r'_[0-9_]+\{}'.format(file_type), omit_file_names=['daily_io', 'provider_info']),
                     desc='Performing Patient Census'):
        output[file] = check_load_df(file, directory=directory, patterns=[r'_[0-9_]+\{}'.format(file_type)], usecols=['patient_deiden_id'], tag_source=include_source,
                                     clean_ids=True, convert_ids_to_sparse_int=True, allow_empty_files=True,
                              engine="pyarrow", max_workers=max_workers, executor_type='ProcessPool',
                              func_after_loading=_process_patient_census_file,
                              func_after_loading_kwargs={'include_source': include_source}, **logging_kwargs).drop_duplicates().rename(columns={'source_file': 'source_batch'})

    return output


def _format_summary(df: pd.DataFrame,
                    levels_to_display: Union[int, None] = None,
                    date_time_col: Union[str, None] = None,
                    stacked_measure_name_col: Union[str, None] = None,
                    value_col: Union[str, None] = None,
                    unit_col: Union[str, None] = None) -> pd.DataFrame:
    df.set_index('clean_column_name', inplace=True)
    
    min_date: str = df.loc[date_time_col, 'min'] if (str(date_time_col) in df.index) and ('min' in df.columns) else None
    max_date: str = df.loc[date_time_col, 'max'] if (str(date_time_col) in df.index) and ('min' in df.columns) else None
    
    df['min_date'] = min_date
    df['max_date'] = max_date
    
    df['indexed_by'] = ', '.join([x[2:] for x in df.columns if re.search(r'^n_', x)] + ([df.loc[date_time_col, 'ml_dtype']] if str(date_time_col) in df.index else []))
    
    if isinstance(levels_to_display, int):
        vc_columns: List[str] = [x for x in df.columns if re.search(r'^value_counts', x)]
        for col in vc_columns:
            non_null_vc_index: pd.Series = df[col].notnull()
            
            if non_null_vc_index.any():
                df.loc[non_null_vc_index, col] = df.loc[non_null_vc_index, col].apply(lambda x: json.dumps(pd.Series(json.loads(x)).sort_values(ascending=False).head(levels_to_display).to_dict()))
                
    if str(date_time_col) in df.index:
        df.drop(index=date_time_col, inplace=True)
        
    if str(value_col) in df.index:
        if str(unit_col) in df.index:
            df.loc[df.index.isin([value_col]), 'units'] = df.loc[df.index.isin([unit_col]), 'value_counts'].values
            df.drop(index=unit_col, inplace=True)
        if str(stacked_measure_name_col) in df.index:
            df.loc[df.index.isin([value_col]), 'label'] = df.loc[df.index.isin([stacked_measure_name_col]), 'levels'].values
            df.drop(index=stacked_measure_name_col, inplace=True)
            df.source_file = df.source_file.apply(lambda x: x.split(f'_{stacked_measure_name_col}_')[0])
        
    return df.drop(columns=['levels', 'mode'], errors='ignore').reset_index(drop=False)

def _combine_reports(*reports: List[pd.DataFrame], levels_to_display: Union[int, None] = None) -> pd.DataFrame:

    master_report: pd.DataFrame = pd.concat(reports, axis=0, ignore_index=True)
    
    master_report: pd.DataFrame = master_report.groupby(['clean_column_name'], group_keys=False).apply(_ag_stats, levels_to_display=levels_to_display)
    
    if 'clean_column_name' not in master_report.columns:
        return master_report.reset_index(drop=False)
    return master_report
    
def _ag_stats(df: pd.DataFrame, levels_to_display: Union[int, None] = None) -> pd.Series:
    out: pd.Series = df.iloc[0, :].copy(deep=True)
    # can't compute these
    out['median'] = None
    out['mode'] = None
    out['levels'] = None
    out['mad'] = None
    
    if df.ml_dtype.isin(['date']).all():
        out['min'] = pd.to_datetime(df['min']).dt.date.min()
        out['max'] = pd.to_datetime(df['max']).dt.date.max()
        
    elif df.ml_dtype.isin(['date', 'datetime']).all():
        out['min'] = pd.to_datetime(df['min']).min()
        out['max'] = pd.to_datetime(df['max']).max()
        
    elif df.ml_dtype.isin(['str', 'object', 'binary', 'cat_embedding', 'cat_one_hot']).all():
        out['min'] = pd.to_numeric(df['min']).min()
        out['max'] = pd.to_numeric(df['max']).max()
        for col in [x for x in df.columns if re.search(r'^value_counts', x)]:
            if isinstance(levels_to_display, int):
                out[col] = json.dumps(df[col].apply(json.loads).apply(pd.Series).T.groupby(level=0, group_keys=False).sum().iloc[:, 0].sort_values(ascending=False).head(levels_to_display).to_dict())
            else:
                out[col] = json.dumps(df[col].apply(json.loads).apply(pd.Series).T.groupby(level=0, group_keys=False).sum().iloc[:, 0].sort_values(ascending=False).to_dict())
        out['nunique'] = len(list(json.loads(out['value_counts']).keys()))
    elif df.sql_dtype.isin(['INTEGER', 'FLOAT']).all():
        out['min'] = pd.to_numeric(df['min']).min()
        out['max'] = pd.to_numeric(df['max']).max()
        out['mean']  = (pd.to_numeric(df['mean']) * pd.to_numeric(df['count'])).sum() / pd.to_numeric(df['count']).sum()
        out['std'] = sqrt((pd.to_numeric(df['std']) * pd.to_numeric(df['std']) / pd.to_numeric(df['count'])).sum())  # sqrt(std1^2/n1 + std2^2/n2 + ... + stdn^2/nn)
        
    
        
    for col in [x for x in df.columns if re.search(r'^n_', x)]:
        out[col] = pd.to_numeric(df[col]).sum()
        
    return out


def _sumarize_file(additional_processing_dict: dict,
              file: str,
              batch: str,
              interim_result_dir: str,
              median_timestamp_dict: dict,
              directory: str,
              file_type: str,
              recursive: bool,
              patient_id_col: str,
              or_encounter_col: str,
              file_loading_kwargs: dict,
              loading_kwargs: dict,
              skip_summary: bool,
              order_num_cols: str,
              hospital_account_col: str,
              encounter_col: str,
              dtype: dict,
              units: dict,
              include_patient_ids: bool,
              include_encounter_ids: bool,
              stacked_name_value_cols: dict,
              lab_id_grouping_cols: List[str],
              exclusion_patterns: List[str]):
    function_name: callable = additional_processing_dict.get(file, {}).get('function')
    if function_name is not None:
        function_name: str = function_name.__name__
    status_file: str = os.path.join(interim_result_dir, f'{file}_{batch}_success_')
    summary_status_file: str = os.path.join(interim_result_dir, f'{file}_summary_{batch}_success_')
    lab_status_file: str = os.path.join(interim_result_dir, f'detailed_lab_summary_{batch}_success_') if file in ['labs', 'labs_clean'] else None
    function_status_file: str = os.path.join(interim_result_dir, f'{file}_{function_name}_{batch}_success_') if isinstance(function_name, str) else None
    ts_stat_status_file: str = os.path.join(interim_result_dir, f'{file}_timestamp_stat_{batch}_success_') if file in median_timestamp_dict else None
    
    # if file in column_names.columns:
        # loading_kwargs['usecols'] = [x for x in column_names[file].dropna().to_list() if x not in (columns_to_ignore.get(file, []) if isinstance(columns_to_ignore, dict) else [])]
    
    if not os.path.exists(status_file):
        df: pd.DataFrame = check_load_df(r'^{}'.format(file), directory=directory,
                                         patterns=[r'_{}\{}'.format(batch, file_type), r'\{}'.format(file_type)],
                                         recursive=recursive, exclusion_patterns=exclusion_patterns,
                                         agg_search_results=True, regex=True, preserve_case=True, preserve_decimals=True, skip_column_name_formatting=True,
                                         allow_empty_files=True, **loading_kwargs, **file_loading_kwargs.get(file, {}))
        
        if isinstance(ts_stat_status_file, str):
            if not os.path.exists(ts_stat_status_file):
                ts_col: str = median_timestamp_dict.get(file)
                if str(ts_col) in df.columns:
                    extract_timestamp_stats(df=df.copy(deep=True), index_col=encounter_col, timestamp_col=median_timestamp_dict.get(file),
                                            stats_to_compute=['median', 'min', 'max'])\
                        .to_parquet(os.path.join(interim_result_dir, f'{file}_{batch}_timestamp_stats.parquet'),
                                    coerce_timestamps='us',
                                    allow_truncated_timestamps=True)
                open(ts_stat_status_file, 'a').close()
        
        if isinstance(df, pd.DataFrame):
            summarization_kwargs: dict = {'stats_by_patient_col': patient_id_col,
                                          'stats_by_encounter_col': encounter_col,
                                          'stats_by_or_encounter_col': or_encounter_col,
                                          'stats_by_hospital_account_col': hospital_account_col,
                                          'stats_by_order_col': df.columns.intersection(order_num_cols).tolist()[0] if len(df.columns.intersection(order_num_cols)) > 0 else None}
            
            id_cols: List[str] = df.columns.intersection(list(summarization_kwargs.values()) + lab_id_grouping_cols).tolist()
            
            if len(id_cols) > 0:
                for col in lab_id_grouping_cols:
                    if col in df.columns:
                        df.loc[:, col] = df.loc[:, col].fillna('XXXMISSINGXXX')
                df: pd.DataFrame = df.set_index(id_cols)
    
            if (not os.path.exists(summary_status_file)) and (not skip_summary):
                if file in (stacked_name_value_cols if isinstance(stacked_name_value_cols, dict) else []):
                    col: str = stacked_name_value_cols.get(file).get('name')
                    for v in df[col].dropna().drop_duplicates():
                        tp: pd.DataFrame = summarize(df[df[col] == v], dtype=dtype.get(file, {}).get(v) if isinstance(dtype, dict) else dtype,
                                                     unit=units.get(file, {}).get(v) if isinstance(units, dict) else units,
                                                     include_patient_ids=include_patient_ids,
                                                     include_encounter_ids=include_encounter_ids,
                                                     **summarization_kwargs)
                        tp.insert(loc=0, column='source_file', value=f'{file}_{col}_{v}')
                        tp.to_pickle(os.path.join(interim_result_dir, f"{remove_illegal_characters(f'{file}_{col}_{v}', preserve_case=False, preserve_decimals=False)}_{batch}.p"))
                        # out.append(tp)
                else:
                    tp: pd.DataFrame = summarize(df, dtype=dtype.get(file) if isinstance(dtype, dict) else dtype,
                                                 unit=units.get(file) if isinstance(units, dict) else units,
                                                 include_patient_ids=include_patient_ids,
                                                 include_encounter_ids=include_encounter_ids,
                                                 **summarization_kwargs)
                    tp.insert(loc=0, column='source_file', value=file)
                    tp.to_pickle(os.path.join(interim_result_dir, f'{file}_{batch}.p'))
                    # out.append(tp)
                open(summary_status_file, 'a').close()  
                
                
            if isinstance(lab_status_file, str) and (not skip_summary):
                if not os.path.exists(lab_status_file):
                    _summarize_labs(df=df,
                                    lab_id_grouping_cols=lab_id_grouping_cols,
                                    file=file,
                                    include_patient_ids=include_patient_ids,
                                    include_encounter_ids=include_encounter_ids,
                                    interim_result_dir=interim_result_dir,
                                    dtype=dtype,
                                    batch=batch,
                                    units=units,
                                    **summarization_kwargs)
                    open(lab_status_file, 'a').close()
            
            if isinstance(function_status_file, str) and (not skip_summary):
                if not os.path.exists(function_status_file):
                    adf: callable = additional_processing_dict[file].pop('function')
                    adf(df=df, interim_result_dir=interim_result_dir, **additional_processing_dict[file])
                    
                    open(function_status_file, 'a').close()
                
                        
            open(status_file, 'a').close()   

    