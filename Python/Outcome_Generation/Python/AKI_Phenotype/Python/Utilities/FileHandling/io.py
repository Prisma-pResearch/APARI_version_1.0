# -*- coding: utf-8 -*-
"""
Created on Wed Nov 10 12:40:19 2021.

@author: ruppert20
"""
import copy
import os
import re
import numpy as np
import pandas as pd
try:
    import cudf
    import dask_cudf
    import cupy as cp
    from dask_cuda import LocalCUDACluster
except ImportError or ModuleNotFoundError:
    pass
import dask.dataframe as dd
import glob
import math
from sqlalchemy.engine.base import Engine
from datetime import datetime as dt
import fnmatch
import json
import types
from ..Logging.log_messages import log_print_email_message as logm
import subprocess
from dask.distributed import Client
import sqlite3 as sq
import yaml
from tqdm import tqdm
from typing import Union, List
import pickle


# import custom modules
from ..PreProcessing.data_format_and_manipulation import ensure_columns as ensc
from ..PreProcessing.data_format_and_manipulation import force_datetime, _format_time, apply_date_censor, get_file_name_components,\
    file_components, tokenize_id, sanatize_columns, prepare_table_for_upload, convert_to_from_bytes, getDataStructureLib, convert_to_lib, check_format_series, extract_batch_numbers
from ..Database.database_updates import get_min_max_id_from_table, log_database_update
from ..FileHandling.h5_helper import read_h5_dataset, write_h5
from ..ResourceManagement.parallelization_helper import run_function_in_parallel_v2
from ..Encryption.file_encryption import load_encrypted_dict, encrypt_and_save_dict, CryptoYAML
from ..Database.connect_to_database import omop_engine_bundle


def load_data(file_path_query: str,
              pid: str = 'patient_deiden_id',
              eid: str = 'encounter_deiden_id',
              directory: Union[str, None] = None,
              patterns: List[str] = [r'_clean_[0-9_]+_optimized_ids\.csv', r'_clean_[0-9_]+\.csv', r'_[0-9_]+_optimized_ids\.csv', r'_[0-9]+_[0-9]+\.csv', r'_[0-9]+\.csv', r'\.csv', r'_[0-9]+_chunk_[0-9]+\.csv'],
              engine: Union[Engine, omop_engine_bundle, None] = None,
              na_values: List[any] = ['', -999, '-999', 'Nan', 'nan', '?', ' ', 'NULL',
                                      '??', '-999.0', 'MISSING OR INVALID DATA FORMATION'],
              recursive: bool = False,
              regex: bool = True,
              max_workers: int = 4,
              inside_parallel_process: bool = True,
              allow_empty_files: bool = False,
              skip_logging: bool = False,
              censor_date: Union[str, None] = None,
              rename_columns: dict = None,
              combine_date_time_cols: dict = None,
              return_selected_cols_if_empty: bool = False,
              agg_search_results: bool = False,
              exclusion_patterns: list = None,
              update_interval: int = 10,
              disp_updates: bool = True,
              list_running_futures: bool = True,
              hide_progress_bar: bool = False,
              executor_type: str = 'ThreadPool',
              clean_ids: bool = False,
              **kwargs):
    """
    Load data from file or server in a standardized manner.

    Parameters
    ----------
    file_path_query : str
        DESCRIPTION.
    pid : str, optional
        DESCRIPTION. The default is 'encounter_deiden_id'.
    eid : str, optional
        DESCRIPTION. The default is 'patient_deiden_id'.
    directory : str, optional
        DESCRIPTION. The default is None.
    patterns : list, optional
        DESCRIPTION. The default is ['*_[0-9]_[0-9].csv', '*_[0-9].csv', '*.csv'].
    engine : Union[omop_engine_bundle, Engine, None], optional
        DESCRIPTION. The default is None.
    na_values : list, optional
        DESCRIPTION. The default is ['', -999, '-999', 'Nan', 'nan', '?',
                                     '??', '-999.0', 'MISSING OR INVALID DATA FORMATION'].
    recursive : bool, optional
        DESCRIPTION. The default is False.
    regex : bool, optional
        DESCRIPTION. The default is False.
    max_workers : int, optional
        DESCRIPTION. The default is 4.
    return_log : bool, optional
        DESCRIPTION. The default is False.
    inside_parallel_process : bool, optional
        DESCRIPTION. The default is True.
        DESCRIPTION. The default is False.
    **kwargs : TYPE
        DESCRIPTION.

    Returns
    -------
    TYPE
        DESCRIPTION.

    """
    logging_kwargs: dict = {x: kwargs.get(x, False if x == 'display' else None) for x in ['log_name', 'log_dir', 'display']}
    if not isinstance(directory, str):
        if not skip_logging:
            logm(message=f'loading {file_path_query}', **logging_kwargs)
        df = _load_file(file_path_query=file_path_query, pid=pid, eid=eid, engine=engine,
                        na_values=na_values, **kwargs, patterns=patterns, logging_kwargs=logging_kwargs)

    else:

        # check patterns in order and stop when there is atleast one hit
        file_list = find_files(directory=directory,
                               patterns=[file_path_query + pattern for pattern in patterns],
                               recursive=recursive,
                               exclusion_patterns=exclusion_patterns,
                               agg_results=agg_search_results,
                               regex=regex)

        kwargs_list: list = []

        data_arr: list = []

        # set of kwargs dicts for each file
        for file in file_list:
            temp = kwargs.copy()
            temp.update({'file_path_query': file,
                         'pid': pid,
                         'eid': eid,
                         'na_values': na_values,
                         'skip_logging': skip_logging,
                         'clean_ids': clean_ids,
                         'log_name': ((logging_kwargs.get('log_name') + '.') if isinstance(logging_kwargs.get('log_name'), str) else '') + os.path.basename(file)})
            kwargs_list.append(temp)

        if len(kwargs_list) > 0:
            try:
                temp_log_name: str = get_file_name_components(kwargs_list[0].get('file_path_query')).file_name
            except:
                temp_log_name: str = file_path_query
        else:
            temp_log_name: str = file_path_query

        # load in parallel if workers are greater than 1
        result = run_function_in_parallel_v2(function=load_data,
                                             kwargs_list=kwargs_list,
                                             max_workers=max_workers,
                                             update_interval=update_interval,
                                             disp_updates=disp_updates,
                                             list_running_futures=list_running_futures,
                                             return_results=True,
                                             log_name=logging_kwargs.get('log_name', temp_log_name),
                                             executor_type=executor_type,
                                             show_progress_bar=(len(kwargs_list) > 1) and (not hide_progress_bar),
                                             debug=(max_workers == 1))

        for f in result:
            data = f['future_result']
            file = f['file_path_query']
            if check_df(df=data, allow_empty_files=allow_empty_files):
                logm(message=f'loaded {data.shape[0]} rows from {os.path.basename(file)}', **logging_kwargs)
                data_arr.append(data)
            else:
                logm(message=f'{os.path.basename(file)} was empty', **logging_kwargs, warning=True)

        # check contents of list
        if len(data_arr) == 0:
            logm(message=f'all {file_path_query} files were empty', warning=True, **logging_kwargs)
            df = pd.DataFrame(columns=kwargs.pop('usecols')) if return_selected_cols_if_empty else None
        else:
            typedict = getDataStructureLib(data_arr[0])
            if typedict['type'] == 'dataframe':
                df = typedict['lib'].concat(data_arr, axis=0, sort=False, ignore_index=True)

            if not skip_logging:
                logm(message=f'done loading {file_path_query} data', **logging_kwargs)

    # rename columns
    if isinstance(rename_columns, dict):
        df.rename(columns=rename_columns, inplace=True)

    if isinstance(combine_date_time_cols, dict):
        df[combine_date_time_cols['label']] = force_datetime((df[combine_date_time_cols['date']].astype(str)
                                                              + ' '
                                                              + df[combine_date_time_cols['time']].apply(_format_time)).values)

    # perform date censoring and format imporper hospital account ids
    if (getDataStructureLib(df)['type'] == 'dataframe') and ('dask' not in str(type(df))):
        if (df.shape[0] > 0) and (isinstance(censor_date, str) or isinstance(censor_date, dict)):
            df = apply_date_censor(censor_date=censor_date if isinstance(censor_date, str) else censor_date['censor_date'],
                                   df=df, df_time_index=None if isinstance(censor_date, str) else censor_date['col'])
        if clean_ids:
            if 'hospital_account_deiden_id' in df.columns:
                missing_hosp_account: pd.Series = df.hospital_account_deiden_id.str.contains(r'_None$', regex=True, case=False, na=False)
                if missing_hosp_account.any():
                    df.loc[missing_hosp_account, 'hospital_account_deiden_id'] = None
                del missing_hosp_account

                if 'encounter_deiden_id' in df.columns:
                    missing_encounter_idx: pd.Series = df.encounter_deiden_id.str.contains(r'_None$', regex=True, case=False, na=False)
                    if missing_encounter_idx.any():
                        df.loc[missing_encounter_idx, 'encounter_deiden_id'] = None
                    del missing_encounter_idx

    return df


def save_data(df: any,
              out_path: Union[str, None] = None,
              original_path: str = None,
              suffix_label: str = None,
              prefix_label: str = None,
              split_into_n_batches: int = None,
              out_dir: str = None,
              file_type: str = '.csv',
              fillna_value: any = -999,
              inside_parallel_process: bool = True,
              mb_size_limit: float = None,
              index: bool = False,
              max_simultaneous_writes: int = 1,
              tuples: list = None,
              engine: Union[omop_engine_bundle, Engine] = None,
              keep_all_batch_nums: bool = False,
              split_by_indentifer_col: str = None,
              split_by_indentifier_batch_dir: str = None,
              id_dict: dict = None,
              group_dataset_df_dict_list: list = None,
              update_interval: int = 10,
              disp_updates: bool = True,
              list_running_futures: bool = True,
              keep_generators_seperate: bool = False,
              show_progress_bar: bool = False,
              rows_for_progress_indicator: int = 1000,
              executor_type: str = 'ThreadPool',
              save_success_dir: Union[str, None] = None,
              **kwargs):
    """
    Save data to file or database.

    Parameters
    ----------
    df: any, **required**
        Object to be saved.
        Note: Pandas, dask, and CUDF dataframes have the most support,
        but it does also work eith strings, dicts, and all other types depending on the desired output format.
    out_path: Union[str, None], optional
        Output file path. The default is None.
    original_path: str, optional
        Original file path to be modified via suffix and/or prefix labels and/or out_dir. The default is None.
    suffix_label: str, optional
        Label to be added to the end of the output file name before the batch numbe (if present). The default is None.
    prefix_label: str, optional
        Label to be added to the begning of the output file name. The default is None.
    split_into_n_batches: int, optional
        Number of batches a desired datframe should be split into before writing. The default is None.
    out_dir: str, optional
        A folder path desined to replace the folder path from the origianl file path. The default is None.
    file_type: str, optional
        The file type of the output file. The default is ".csv"
    fillna_value: any, optional
        Value to be used to replace None/NaNs in a dataframe. The default is -999.
    inside_parallel_process: bool, optional
        Legacy kwarg that will be removed in future releases. The default is True.
    mb_size_limit: float, optional
        The maximum size allowed for an output file. This only works with dataframes.
        This will split the input df into n chunks of size smaller than the specified limit. The default is None.
    index: bool, optional
        Whether the index of a dataframe should be saved in the output file. The default is False.
    max_simultaneous_writes: int, optional
        The maximum number of simultaneous writes achieved using a threadpool executor for files that are split into batches via other kwargs. The default is 1.
    tuples: list, optional
        List of tuples in the form of [(item1: any, path1: str), (item2: any, path2: str)]. The default is None.
    engine: SQLALchemy database engine or SQLite connection, optional
        SQLALCHEMY database engine or sqlite database connection to save input df in. The default is None.
        **Note: requires input to be a dataframe, a destination table, and a destination schema.
    keep_all_batch_nums: bool, optional
        Whether all batch numbers should be preserved in the output file path for paths that require modification. The default is False.
    split_by_indentifer_col: str, optional
        Column used to split a dataframe into seperate files ensuring that the values in the specified column are unique and consistant accross batches. The default is None.
    split_by_indentifier_batch_dir: str, optional
        Directory which stores the mapping between values in the split_by_identifier_col and individual batches.
    id_dict: dict, optional
        Dictionary of batches with values inside the map identifer column as a list in the dictionary. The default is None.
    group_dataset_df_dict_list: list, optional
        List of dictionaries to be passed to the h5 helper to be written. The default is None.
    update_interval: int, optional
        Number of seconds between updates for jobs which required multiple batches to be written. The default is 10 seconds.
    disp_updates: bool, optional
        Whether updates should be displayed at the specified interval for jobs that require multiple batches to be written. The defaul is True.
    list_running_futures: bool, optional
        Whether running threads should be displayed at the specified interval for jobs that require multiple batches to be written. The defaul is True.
    keep_generators_seperate: bool, optional
        Whether generator objects should be saved as autoincrementing batches or in one file. The default is False.
    show_progress_bar: bool, optional
        Whether individual file writes should show a progress bar. The default is False.
        **Note: Overall this decreases speed as a result of the overhead to split and then to open/close the same file repeatedly,
        but is available for people who are inpatient and want to see a progress indicator.
    rows_for_progress_indicator: int, optional
        The number of rows per sub batch to split a single file into for the progress bar. The default is 1000.
    **kwargs
        keyword arguments to pass to the underlying saving funciton. e.g. pd.to_csv

    Returns
    -------
    Union[None, pd.DataFrame]
        Succesfful runs returns None, in the event of an error uploading to a SQL database a problem dataframe may be returned.

    """
    if isinstance(out_path, str):
        file_type = get_file_name_components(out_path).file_type

    typedict = getDataStructureLib(df)

    logging_kwargs: dict = {x: kwargs.get(x, False if x == 'display' else None) for x in ['log_name', 'log_dir', 'display']}

    if show_progress_bar and (typedict['type'] == 'dataframe'):
        df = np.array_split(df, math.floor(df.shape[0] / rows_for_progress_indicator))

    if (typedict['type'] == 'dataframe') and (not show_progress_bar):

        # check if the file should be uploaded to sql
        if isinstance(engine, Engine) or isinstance(engine, sq.Connection) or isinstance(engine, omop_engine_bundle):
            # write to file
            return _write_file(index=index, df=df, file_type=None,
                               out_file_path=None, engine=engine.engine if isinstance(engine, omop_engine_bundle) else engine, **kwargs)

        if isinstance(out_path, str):
            file_info: file_components = get_file_name_components(out_path)
            out_dir = file_info.directory
            file_type = file_info.file_type
        elif isinstance(original_path, str):
            file_info: file_components = get_file_name_components(original_path)
        else:
            raise Exception('either out_path or original_path are required')

        if len(file_info.batch_numbers) > 0:
            if keep_all_batch_nums:
                batch_num = '_'.join([str(x) for x in file_info.batch_numbers])
            else:
                batch_num = str(file_info.batch_numbers[0])
        else:
            batch_num = None

        if (not isinstance(out_path, str)) or isinstance(prefix_label, str) or isinstance(suffix_label, str):
            out_path = make_file_path(file_name=file_info.file_name,
                                      prefix=prefix_label,
                                      suffix=suffix_label,
                                      batch_number=batch_num,
                                      directory=out_dir)
        else:
            out_path = out_path.replace(file_type, '')

        if isinstance(mb_size_limit, (int, float)):

            split_into_n_batches: int = math.ceil(convert_to_from_bytes(value=df
                                                                        .memory_usage(deep=True, index=False).sum(),
                                                                        unit='MB') / mb_size_limit)

        if isinstance(split_into_n_batches, int):
            if isinstance(split_by_indentifer_col, str):
                id_dir: str = split_by_indentifier_batch_dir if isinstance(split_by_indentifier_batch_dir, str) else os.path.dirname(out_path)
                dfs = split_by_identifier(df=df, split_by_indentifer_col=split_by_indentifer_col, id_dir=id_dir, id_dict=id_dict,
                                          split_into_n_batches=split_into_n_batches, batch_num=batch_num, **logging_kwargs)
            elif typedict['cpul']:
                logm(message='Splitting files randomly', **logging_kwargs)
                dfs = {i: v for i, v in enumerate(np.array_split(df, split_into_n_batches))}
            else:
                logm(message='Splitting files randomly', **logging_kwargs)
                dfs = {i: v for i, v in enumerate(cp.array_split(df, split_into_n_batches))}
        else:
            dfs = {0: df}

        kwargs_list: list = []

        for i, f in dfs.items():
            temp_k: dict = kwargs.copy()
            temp_k.update({'out_file_path': (out_path + file_type) if len(dfs) == 1 else out_path + '_{}'.format(i) + file_type,
                           'index': index,
                           'file_type': file_type,
                           'save_success_fp': os.path.join(save_success_dir,
                                                           f'{file_info.file_name}_{"_".join([str(x) for x in file_info.batch_numbers])}' + ('_{i}' if len(dfs) == 1 else '') + '_success') if isinstance(save_success_dir, str) else None,
                           'df': f})
            kwargs_list.append(temp_k)

        del df, dfs

    elif isinstance(group_dataset_df_dict_list, list):
        write_h5(fp=out_path, group_dataset_df_dict_list=group_dataset_df_dict_list, index=index, **kwargs)
        return
    elif isinstance(tuples, list):

        kwargs_list: list = []

        for item, path in tuples:
            temp_k: dict = kwargs.copy()
            temp_k.update({'out_file_path': path,
                           'index': index,
                           'file_type': file_type,
                           'df': item})
            kwargs_list.append(temp_k)

        del tuples
    elif isinstance(df, (dict, str)):
        _write_file(out_file_path=out_path, index=index, file_type=file_type,
                    df=df, **kwargs)
        return
    elif isinstance(df, (types.GeneratorType, list, pd.io.parsers.readers.TextFileReader)):
        first_df: bool = True
        if keep_generators_seperate:
            assert isinstance(out_path, str), 'Out Path is required when "keep_generators_seperate" is set to True'
            assert bool(re.search(r'\.csv$|\.xlsx$', out_path)), 'Only .xlsx and .csv files are initially supported'

        kwargs_list: list = []
        for i, btch in enumerate(df):

            kw = kwargs.copy()
            kw['index'] = index
            kw['file_type'] = file_type
            kw['df'] = btch
            kw['engine'] = engine
            kw['fillna_value'] = fillna_value
            kw['log_name'] = ((logging_kwargs.get('log_name') + '.') if isinstance(logging_kwargs.get('log_name'), str) else '') + (os.path.basename(out_path) if isinstance(out_path, str) else '')
            kw['log_dir'] = logging_kwargs.get('log_dir')

            if isinstance(engine, Engine):
                save_data(**kw)
            else:
                if re.search(r'\.csv|\.xlsx', out_path) and (not first_df):
                    kw['header'] = False
                    kw['mode'] = 'a'

                if isinstance(df, list):
                    kw['out_file_path'] = out_path
                    kw['display'] = False
                    kwargs_list.append(kw)
                    first_df = False
                elif isinstance(df, (types.GeneratorType, pd.io.parsers.readers.TextFileReader)):
                    if keep_generators_seperate:
                        kw['out_path'] = re.sub(r'\.xlsx$', f'_{i}.xlsx', re.sub(r'\.csv$', f'_{i}.csv', out_path))
                    else:
                        kw['out_path'] = out_path
                    kw['original_path'] = original_path
                    kw['suffix_label'] = suffix_label
                    kw['prefix_label'] = prefix_label
                    kw['split_into_n_batches'] = split_into_n_batches
                    kw['out_dir'] = out_dir
                    kw['inside_parallel_process'] = inside_parallel_process
                    kw['mb_size_limit'] = mb_size_limit
                    kw['max_simultaneous_writes'] = max_simultaneous_writes
                    kw['keep_all_batch_nums'] = keep_all_batch_nums

                    save_data(**kw)
                    first_df = keep_generators_seperate

        if isinstance(df, types.GeneratorType):
            return

    run_function_in_parallel_v2(function=_write_file,
                                kwargs_list=kwargs_list,
                                max_workers=max_simultaneous_writes,
                                update_interval=update_interval,
                                disp_updates=disp_updates,
                                list_running_futures=list_running_futures,
                                log_name=logging_kwargs.get('log_name'),
                                executor_type=executor_type,
                                show_progress_bar=(len(kwargs_list) > 1) or show_progress_bar,
                                debug=(max_simultaneous_writes == 1) or show_progress_bar)

    return


def _write_file(out_file_path: str, index: bool, file_type: str,
                df: Union[pd.DataFrame, str, dict, CryptoYAML, dd.DataFrame, any],
                engine: Engine = None, fillna_value: str = None,
                copyFirst: bool = True, de_depulicate: bool = False,
                log_name: str = None,
                log_dir: str = None,
                display: bool = False,
                save_success_fp: Union[str, None] = None,
                **kwargs):

    typedict: dict = getDataStructureLib(df)

    if not (isinstance(engine, Engine) or isinstance(engine, sq.Connection)):
        logm(message=f'Writting {os.path.basename(out_file_path)}',
             log_name=log_name, log_dir=log_dir, display=display)

    if typedict['type'] == 'dataframe':
        if copyFirst:
            df = df.copy(deep=True)

        if (de_depulicate and (not index)):
            df = df.drop_duplicates()

        if typedict['lib'].notnull(fillna_value):
            df.fillna(value=fillna_value, inplace=True)

        if isinstance(engine, Engine) or isinstance(engine, sq.Connection):
            logm(message=f'Uploading {kwargs.get("dest_table", "table")}',
                 log_name=log_name, log_dir=log_dir, display=display)

            problem_df = upload_table(engine=engine, df=df, index=index, **kwargs)
            if isinstance(problem_df, pd.DataFrame):
                if kwargs.get('failure_fp', '-999') != '-999':
                    problem_df.to_csv(kwargs.get('failure_fp').replace('.pkl', '.csv'), index=False)
                else:
                    logm(problem_df, error=True, log_name=log_name, log_dir=log_dir, display=display)

            return problem_df
        elif file_type in ['.csv', '.txt', '.tsv']:
            if file_type == '.tsv':
                kwargs['sep'] = r'\t'
            df.to_csv(out_file_path, index=index, **kwargs)
        elif file_type == '.xlsx':
            if isinstance(df, pd.DataFrame) or isinstance(df, dd.DataFrame):
                df.to_excel(out_file_path, index=index, **kwargs)
            elif (typedict['type'] == 'dataframe') and (not typedict['cpul']):
                df.to_pandas().to_excel(out_file_path, index=index, **kwargs)
        elif file_type in ['.pickle', '.p', '.pkl']:
            if not isinstance(df, pd.DataFrame):
                df = df.to_pandas()
            df.to_pickle(out_file_path, **kwargs)
        elif file_type in ['.parquet']:
            df.to_parquet(out_file_path, **kwargs)
        elif file_type in ['.h5']:
            write_h5(fp=out_file_path, dataframe=df, **kwargs)
        else:
            message = 'Unsupported file format, currently only .csv, .txt, .tsv, .xlsx, .p, .pkl, .pickle, .parquet, .h5 are supported'
            logm(message=message, error=True, log_name=log_name, log_dir=log_dir, display=display)
            raise Exception(message)

    elif (typedict['lib'] == dict) and (file_type in ['.json', '.yml', '.yaml', '.json_aes']):
        if file_type in ['.json']:
            json.dump(obj=df, fp=open(out_file_path, 'w'), indent=4)
        elif file_type in ['.json_aes']:
            encrypt_and_save_dict(dictionary_file_path=out_file_path,
                                  key_dir=kwargs.get('key'))
        else:
            yaml.safe_dump(data=df, stream=open(out_file_path, 'w'))
    elif (typedict['lib'] == str) and (file_type in ['.txt', '.sql']):
        with open(out_file_path, 'w') as f:
            f.write(df)
    elif (typedict['lib'] == dict) and (file_type in ['.xlsx', '.xls']):
        writer = pd.ExcelWriter(out_file_path, engine='xlsxwriter')

        for sheet_name, data in df.items():
            if not isinstance(data, pd.DataFrame):
                data = data.to_pandas()
            data.to_excel(writer, sheet_name=sheet_name, index=index, **kwargs)

        writer.close()
    elif (type(df) == CryptoYAML) and (file_type in ['.yaml_aes', '.yml_aes']):
        df.write()
    elif file_type in ['.h5', '.hd5']:
        write_h5(fp=out_file_path, dataframe=df, **kwargs)
    elif file_type in ['.pickle', '.p', '.pkl']:
        pickle.dump(df, open(out_file_path, 'wb'))
    else:
        message = 'Unsupported file format, currently only .csv, .txt, .sql, .tsv, .xlsx, .p, .pkl, .pickle, .parquet, .h5 are supported'
        logm(message=message, error=True, log_name=log_name, log_dir=log_dir, display=display)
        raise Exception(message)

    if isinstance(save_success_fp, str):
        open(save_success_fp, 'a').close()
    logm(message=f'Done writting {os.path.basename(out_file_path)}', log_name=log_name, log_dir=log_dir, display=display)
    return


def _load_file(file_path_query: str,
               pid: str,
               eid: str,
               na_values: list = None,
               engine: Engine = None,
               pid_map: pd.DataFrame = None,
               pids_to_drop: list = None,
               eids_to_drop: list = None,
               drop_override: bool = False,
               preserve_decimals: bool = False,
               preserve_case: bool = False,
               post_filter_columns: bool = False,
               use_col_intersection: bool = False,
               tag_source: bool = False,
               format_identifiers: bool = False,
               label_source_row: bool = False,
               load_all_chunks: bool = False,
               ignore_id_errors: bool = False,
               make_unique_enc_or_case_ids: bool = False,
               id_prefix_to_append: str = None,
               format_boolean_bytes_as_ints: bool = True,
               use_dask: bool = False,
               use_gpu: bool = False,
               output_lib: str = None,
               log_name: str = None,
               log_dir: str = None,
               display: bool = False,
               execute_in_transaction: bool = False,
               query_folder: str = None,
               patterns: list = None,
               logging_kwargs: dict = {},
               show_progress_bar: bool = False,
               rows_for_progress_indicator: int = 1000,
               raw_txt: bool = False,
               df_query_str: str = None,
               compute_query: bool = True,
               **kwargs) -> pd.DataFrame:
    temp = None

    if use_gpu:
        try:
            expected_type: type = dask_cudf.DataFrame if use_dask else cudf.DataFrame
            loading_lib = dask_cudf if use_dask else cudf
        except NameError:
            use_gpu: bool = False
            expected_type: type = dd.DataFrame if use_dask else pd.DataFrame
            loading_lib = dd if use_dask else pd
    else:
        expected_type: type = dd.DataFrame if use_dask else pd.DataFrame
        loading_lib = dd if use_dask else pd

    if use_gpu:
        try:
            na_values.remove(-999)  # cannot handle integer values in na_values for cudf.read_csv
        except ValueError:
            pass

    # load data/retrieve data
    if isinstance(engine, Engine) or isinstance(engine, omop_engine_bundle) or isinstance(engine, sq.Connection) or isinstance(query_folder, str):

        if bool(re.search(r'\.sql$', file_path_query)):
            sql = _load_file(file_path_query=file_path_query, pid=pid, eid=eid)

            replacements: dict = kwargs.pop('replacements', None)

            # use standard replacements, but allow for user overrides
            if isinstance(engine, omop_engine_bundle):
                replacements2: dict = {'ReSuLtS_ScHeMa': engine.results_schema,
                                       'DaTa_ScHeMa': engine.data_schema,
                                       'VoCaB_ScHeMa': engine.vocab_schema,
                                       'LoOkUp_ScHeMa': engine.lookup_schema,
                                       'OpErAtIoNaL_ScHeMa': engine.operational_schema}
                if isinstance(replacements, dict):
                    replacements2.update(replacements)
                    replacements = replacements2
                else:
                    replacements = replacements2

            if isinstance(engine.engine, Engine) if isinstance(engine, omop_engine_bundle) else isinstance(engine, Engine):
                if (engine.engine.name if isinstance(engine, omop_engine_bundle) else engine.name) == 'mysql':
                    sql = sql.replace('[', '').replace(']', '')

            if isinstance(replacements, dict):
                for k, v in replacements.items():
                    sql = sql.replace(k, v)

            return_raw_query: bool = kwargs.pop('return_raw_query', None)

            if isinstance(return_raw_query, bool):
                if return_raw_query:
                    return sql
        else:
            sql = file_path_query

        if isinstance(engine, omop_engine_bundle):
            engine = engine.engine

        if isinstance(engine, Engine):
            if engine.name == 'mysql':
                sql = sql.replace('[', '').replace(']', '')

        # handle %s
        sql = re.sub('(?<=[^%])%', '%%', sql).replace('%%%', '%%')

        if execute_in_transaction:
            update_kwargs: dict = kwargs.get('update_kwargs', {})
            assert isinstance(update_kwargs.get('schema'), str), 'schema must be defined in update_kwargs dictionary'
            assert isinstance(update_kwargs.get('note'), str), 'note must be defined in update_kwargs dictionary'
            log_database_update(file_name=update_kwargs.get('file_name', 'Not specified'),
                                batch=update_kwargs.get('batch', 'Not specified'),
                                dest_table=update_kwargs.get('dest_table', 'Not specified'),
                                note=update_kwargs.get('note'),
                                engine=engine,
                                schema=update_kwargs.get('schema'),
                                table_name=update_kwargs.get('table', 'database_update'),
                                execute_query=True,
                                min_id=None,
                                max_id=None,
                                crud_query=sql,
                                process_start=update_kwargs.get('process_start', None),
                                process_end=update_kwargs.get('process_end', None),
                                upload_start=None, status_message=None,
                                upload_end=None,
                                github_release=update_kwargs.get('github_release', 'Not specified'),
                                log_name=log_name, log_dir=log_dir, display=display)
            return

        if isinstance(query_folder, str):
            engine = query_folder_with_sql(sql_query=sql, query_folder=query_folder,
                                           patterns=patterns, load_only=True,
                                           tag_source=tag_source,
                                           label_source_row=label_source_row,
                                           load_all_cols=kwargs.pop('load_all_cols', False),
                                           **logging_kwargs)
            tag_source: bool = False
            label_source_row: bool = False

        df = pd.read_sql(sql=sql, con=engine, **kwargs)

        if format_boolean_bytes_as_ints and isinstance(df, pd.DataFrame):
            for c in df.columns:
                fvi: int = df[c].first_valid_index()
                if isinstance(fvi, int):
                    if str(type(df.loc[fvi, c])) == "<class 'bytes'>":
                        nn = df[c].notnull()
                        df.loc[nn, c] = df.loc[nn, c].apply(bool.from_bytes, byteorder='big').astype(int)

        if use_gpu or use_dask:
            df = loading_lib.from_pandas(df)

    elif bool(re.search(r'\.json$|\.json_aes$|\.sql$|\.yaml$|\.yml$|\.yaml_aes$|\.yml_aes$', file_path_query)):
        if bool(re.search(r'\.json$', file_path_query)):
            return json.load(open(file_path_query, 'r'))
        elif bool(re.search(r'\.json_aes$', file_path_query)):
            return load_encrypted_dict(encrypted_dict_file_path=file_path_query, key_dir=kwargs.get('key', kwargs.get('key_dir')))
        elif bool(re.search(r'\.yaml$|\.yml$', file_path_query)):
            return yaml.safe_load(open(file_path_query, 'rb'))
        elif bool(re.search(r'\.yaml_aes$|\.yml_aes$', file_path_query)):
            return CryptoYAML(filepath=file_path_query, key=kwargs.get('key'), keyfile=kwargs.get('key_dir', kwargs.get('keyfile')))
        elif bool(re.search(r'\.sql$', file_path_query)):
            with open(file_path_query, newline=None) as f:
                sql = f.read()

            replacements: dict = kwargs.pop('replacements', None)

            if isinstance(replacements, dict):
                for k, v in replacements.items():
                    sql = sql.replace(k, v)

            return sql

    elif bool(re.search(r'\.txt$', file_path_query)) and raw_txt:
        with open(file_path_query, newline=None) as f:
            text = f.read()

        return text

    elif bool(re.search(r'\.csv$|\.txt$|\.tsv$', file_path_query)):

        kwargs['dtype'] = kwargs.get('dtype', object)

        try:
            if show_progress_bar:
                kwargs['chunksize'] = rows_for_progress_indicator
                with tqdm(total=_count_lines_enumrate(file_path_query)) as pbar:
                    df_l: list = []
                    for df in loading_lib.read_csv(file_path_query, low_memory=False, na_values=na_values, **kwargs):
                        df_l.append(df)
                        pbar.update(df.shape[0])
                df = pd.concat(df_l, axis=0, ignore_index=True)
            else:
                df = loading_lib.read_csv(file_path_query, low_memory=False, na_values=na_values, **kwargs)
        except ValueError:
            # pull usecols from kwargs list
            usecols = kwargs.pop('usecols', None)

            if use_col_intersection:
                kwargs['usecols'] = list(set(pd.read_csv(file_path_query,
                                                         low_memory=False,
                                                         nrows=0,
                                                         sep=kwargs.get('sep', ','))
                                             .columns.tolist())
                                         .intersection(set(usecols + [pid.upper() if isinstance(pid, str) else '',
                                                                      eid.upper() if isinstance(eid, str) else ''])))

                df = loading_lib.read_csv(file_path_query, low_memory=False, **kwargs)
            else:
                df = df = loading_lib.read_csv(file_path_query, low_memory=False, na_values=na_values, **kwargs)
                post_filter_columns: bool = True

    elif bool(re.search(r'\.xlsx$|\.xls$', file_path_query)):
        kwargs['dtype'] = kwargs.get('dtype', object)
        df = pd.read_excel(file_path_query, na_values=na_values, engine='openpyxl', **kwargs)
    elif bool(re.search(r'\.sas7bdat$', file_path_query)):
        temp = pd.read_sas(filepath_or_buffer=file_path_query, **kwargs)
    elif bool(re.search(r'\.hd5$|.h5', file_path_query)):
        temp = read_h5_dataset(fp=file_path_query, **kwargs)
    elif bool(re.search(r'\.parquet$', file_path_query)):
        temp = loading_lib.read_parquet(filepath_or_buffer=file_path_query, **kwargs)
    elif bool(re.search(r'\.pickle$|\.pkl$|\.p$', file_path_query)):
        temp = pd.read_pickle(filepath_or_buffer=file_path_query, **kwargs)
    elif bool(re.search(r'\.feather$|\.fth$|\.f$', file_path_query)):
        temp = loading_lib.read_feather(file_path_query, **kwargs)
    elif bool(re.search(r'\.log$', file_path_query)):
        with open(file_path_query, 'r') as f:
            return f.readlines()
    else:
        raise Exception('unsupported file type')

    if isinstance(temp, pd.DataFrame) and (str(type(temp)) != str(expected_type)):
        temp = loading_lib.from_pandas(temp)

    if isinstance(temp, expected_type):
        df = temp.replace({k: None for k in na_values})
    elif kwargs.get('chunksize', '-999') != '-999':

        if load_all_chunks:
            out: list = []
            for i, c in enumerate(df):
                logm(message=f'Loading Chunk {i}', log_name=log_name, log_dir=log_dir, display=display)
                out.append(c)
            logm(message='Merging Chunks', log_name=log_name, log_dir=log_dir, display=display)
            return loading_lib.concat(out, axis=0, sort=False)
        return df
    elif not (temp is None):
        return temp

    if format_identifiers:
        # format identifier columns
        if ((eid in df.columns) and (eid != 'merged_enc_id')):
            df.loc[:, eid] = df.loc[:, eid].apply(tokenize_id, token_index=-1, ignore_errors=ignore_id_errors).values

        if 'or_case_key_deiden_id' in df.columns:
            df.loc[:, 'or_case_key_deiden_id'] = df.or_case_key_deiden_id.apply(tokenize_id, token_index=-1, ignore_errors=ignore_id_errors).values

        if 'or_case_num_deiden_id' in df.columns:
            # df.loc[:, 'or_case_num_deiden_id'] = df.loc[:, 'or_case_num_deiden_id'].replace({'nan': None, 'None': None})
            df.loc[:, 'or_case_num_deiden_id'] = df.or_case_num_deiden_id.apply(tokenize_id, token_index=-1, ignore_errors=ignore_id_errors).values

        if isinstance(pid, list):
            for col in pid:
                df.loc[:, col] = df.loc[:, col].apply(tokenize_id, token_index=-1, ignore_errors=ignore_id_errors).values
            if isinstance(pid_map, expected_type):
                df = df.merge(pid_map, how='left', on=pid).drop(columns=pid)

                if df.patient_deiden_id.isnull().any():
                    raise Exception('There are patient ids not in the id_dict, please fix before continuing')
        elif pid in df.columns:
            df.loc[:, pid] = df.loc[:, pid].apply(tokenize_id, token_index=-1, ignore_errors=ignore_id_errors).values

            if isinstance(pid_map, dict):
                df.loc[:, pid].replace(pid_map, inplace=True)

    if isinstance(id_prefix_to_append, str):
        df.loc[:, pid] = f'{id_prefix_to_append}_' + df.loc[:, pid].astype(str)

    # if make_unique_enc_or_case_ids:
    #     df.loc[:, eid] = df.loc[:, pid].astype(str) + "_" + df.loc[:, eid].astype(str)

    # if 'or_case_num_deiden_id' in df.columns:
    #     if not df.or_case_num_deiden_id.str.contains('_').any():
    #         df.loc[:, 'or_case_num_deiden_id'] = df.loc[:, eid].astype(str) + "_" + df.loc[:, 'or_case_num_deiden_id'].astype(str)

    if isinstance(pids_to_drop, list) and (pid in df.columns):
        if drop_override or (get_batch_num(file_path_query) < calculate_number_of_batches(os.path.dirname(file_path_query)) - 1):
            logm(message=f'dropping ids from {os.path.basename(file_path_query)}', warning=True,
                 log_name=log_name, log_dir=log_dir, display=display)
            df = df[~df[pid].isin(pids_to_drop)]

    if isinstance(eids_to_drop, list) and (eid in df.columns):
        logm(message=f'dropping encounter ids from {os.path.basename(file_path_query)}', warning=True,
             log_name=log_name, log_dir=log_dir, display=display)
        df = df[~df[eid].isin(eids_to_drop)]

    if tag_source and (df.shape[0] > 0) and ('source_file' not in df.columns):
        df['source_file'] = f'{os.path.basename(os.path.dirname(file_path_query))}/{os.path.basename(file_path_query)}'

    if getDataStructureLib(df)['type'] == 'dataframe':
        df: loading_lib = sanatize_columns(df.copy(), preserve_case=preserve_case, preserve_decimals=preserve_decimals)

        if post_filter_columns:
            df = df[usecols]

        if label_source_row and ('source_row' not in df.columns):
            df = df.reset_index(drop=False)\
                .rename(columns={'index': 'source_row'})

        dcols: list = kwargs.pop('parse_dates', None)
        if isinstance(dcols, list):
            df = force_datetime(ds=df, date_cols=dcols)

        if isinstance(df_query_str, str):
            df = df.query(df_query_str)

            if compute_query and ('dask' in str(type(df))):
                df = df.compute()

        if isinstance(output_lib, str):
            df = convert_to_lib(ds=df, desired_lib=output_lib, reset_index=True)

    return df


def _count_lines_enumrate(file_name):
    fp = open(file_name, 'r')
    for line_count, line in enumerate(fp):
        pass
    return line_count


def get_batch_num(file_name: str) -> int:
    """
    Retrieve batch numbers from file name.

    Actions: finds trailing numbers
    """
    return int(re.search(r'_[0-9]+\.', os.path.basename(file_name)).group(0)[1:-1])


def calculate_number_of_batches(dir_path: str) -> int:
    """Calculate the number of batches in a given directory."""
    listOfFiles = find_files(directory=dir_path, patterns=r'_[0-9]+\.csv$', recursive=False, regex=True)

    file_list = []
    for entry in listOfFiles:
        file_list.append(int(get_batch_num(entry)))

    return max(file_list) + 1


def find_files(directory: str, patterns: str, recursive: bool = True, regex: bool = False,
               agg_results: bool = False, exclusion_patterns: list = None) -> list:
    """
    Generate list of files based on pattern.

    Parameters
    ----------
    directory : str
        DESCRIPTION.
    pattern : str
        DESCRIPTION.
    recursive : bool, optional
        DESCRIPTION. The default is True.
    regex : bool, optional
        DESCRIPTION. The default is False.

    Returns
    -------
    list
        DESCRIPTION.

    """
    out: list = []

    for pattern in [patterns] if isinstance(patterns, str) else patterns:

        file_list = glob.glob(os.path.join(directory, '**' if recursive and regex else '*' if not recursive and regex else os.path.join('**', pattern) if recursive else pattern),
                              recursive=recursive)

        if regex:
            for file in [f for f in file_list if re.search(pattern, os.path.basename(f), flags=re.IGNORECASE)]:

                if isinstance(exclusion_patterns, list):

                    if any([bool(re.search(expat, os.path.basename(file), flags=re.IGNORECASE)) for expat in exclusion_patterns]):
                        continue
                out.append(file)

        elif len(file_list) > 0:

            if isinstance(exclusion_patterns, list):
                for file in file_list:
                    if any([bool(re.search(glob2regex(expat), os.path.basename(file), flags=re.IGNORECASE)) for expat in exclusion_patterns]):
                        continue
                    out.append(file)
            else:
                out = out + file_list

        if ((len(out) > 0) and (not agg_results)):
            return out

    return out


def glob2regex(glob_str: str) -> str:
    """Translate unix expression to regex."""
    return fnmatch.translate(glob_str).replace(']', ']+')


def check_df(df, allow_empty_files: bool = False) -> bool:
    """Verify object is a pandas dataframe and whether or not there are any rows contained within."""
    if getDataStructureLib(df)['type'] == 'dataframe':

        if allow_empty_files:
            return True

        if df.shape[0] > 0:
            return True

    return False


def make_file_path(file_name: str, prefix: str = None, suffix: str = None,
                   batch_number: int = None, directory: str = None,
                   file_type: str = None) -> str:
    """Derive a file path from input variables."""
    name_list = []

    if isinstance(prefix, str):
        name_list.append(prefix)

    name_list.append(file_name)

    if isinstance(suffix, str):
        name_list.append(suffix)

    if isinstance(batch_number, int) or isinstance(batch_number, str):
        name_list.append(str(batch_number))

    temp = '_'.join(name_list)

    if isinstance(file_type, str):
        temp += file_type

    if isinstance(directory, str):
        return os.path.join(directory, temp)

    return temp


def upload_table(engine: Engine, dest_table: str, df: pd.DataFrame,
                 dest_schema: str = None, success_fp: str = None, failure_fp: str = None,
                 truncate_to_fit: bool = False,
                 fallback_engine: Engine = None, if_exists: str = 'append',
                 debug: bool = False, index: bool = False,
                 dtypes: dict = None,
                 skip_character_check: bool = False,
                 chunksize: int = 1000,
                 log_update: bool = False,
                 update_kwargs: dict = {},
                 cache_before_upload_path: str = None,
                 **kwargs) -> pd.DataFrame:
    """
    Upload data to SQL server with type safety.

    Parameters
    ----------
    engine : Engine
        DESCRIPTION.
    dest_table : str
        DESCRIPTION.
    df : pd.DataFrame
        DESCRIPTION.
    success_fp : str
        DESCRIPTION.
    failure_fp : str
        DESCRIPTION.
    dest_schema : str, optional
        DESCRIPTION. The default is 'OMOP'.
    truncate_to_fit : bool, optional
        DESCRIPTION. The default is False.
    fallback_engine : Engine, optional
        DESCRIPTION. The default is None.
    log_update: bool, optional
        Whether the upload should be logged in the database update table. The default is False.
    update_kwargs: dict, optional
        The dictionary can contain the following keys:
            *"github_release": the release tag for the code used to make the table
            *"table": the name of the database update table
            *"schema": the schema that contains the database update table
            *"note": the note associated with the upload
            *"batch": the batch(es) that the source file originated in
            *"file_name": the name of the source file

    Raises
    ------
    Exception
        DESCRIPTION.

    Returns
    -------
    None.

    """
    if isinstance(success_fp, str):
        if os.path.exists(success_fp):
            return

    if pd.isnull(dest_schema) and ('.' in dest_table):
        dest_schema: str = '.'.join(dest_table.split('.')[:-1])
        dest_table: str = dest_table.split('.')[-1]

    if log_update:
        log_database_update(file_name=update_kwargs.get('file_name', 'Unkown Source File Name'),
                            batch=update_kwargs.get('batch', 'Unknown Source Batch'),
                            dest_table=dest_table,
                            note=update_kwargs.get('note', 'Initial Upload'),
                            engine=engine,
                            schema=update_kwargs.get('schema', dest_schema),
                            table_name=update_kwargs.get('table', 'database_update'),
                            execute_query=False,
                            min_id=get_min_max_id_from_table(engine=engine, table=dest_table, schema=dest_schema, id_type='max', add_one=True),
                            max_id=None,
                            process_start=update_kwargs.get('process_start', None),
                            process_end=update_kwargs.get('process_end', None),
                            upload_start=None, status_message='Preparing to Upload table',
                            upload_end=None,
                            github_release=update_kwargs.get('github_release', 'Unknown'), crud_query=None)

    logging_kwargs: dict = {x: kwargs.get(x, False if x == 'display' else None) for x in ['log_name', 'log_dir']}

    logm(message='Checking for failed previous attempts', **logging_kwargs)

    if isinstance(failure_fp, str):
        cached_file_type: str = 'failed'
        failure_files = find_files(directory=os.path.dirname(failure_fp),
                                   patterns=[os.path.basename(failure_fp).replace('.pkl', r'_[0-9]+\.pkl'),
                                             os.path.basename(failure_fp).replace('.pkl', r'_chunk_[0-9]+\.pkl')],
                                   recursive=False,
                                   regex=True)

        if (len(failure_files) == 0) and isinstance(cache_before_upload_path, str):
            failure_files = find_files(directory=os.path.dirname(cache_before_upload_path),
                                       patterns=[r'{}_chunk_[0-9]+\.p'.format(os.path.basename(cache_before_upload_path).replace('.p', '')),
                                                 r'{}\.p'.format(os.path.basename(cache_before_upload_path).replace('.p', ''))],
                                       recursive=False,
                                       regex=True)

            if len(failure_files) > 0:
                cached_file_type: str = 'cached'
                cache_before_upload_path: str = None

    else:
        failure_files: list = []
        cached_file_type: str = 'None'

    if len(failure_files) > 0:

        logm(message=f'Loading {len(failure_files)} {cached_file_type} files', warning=True, **logging_kwargs)

        batches: list = [pd.read_pickle(f) for f in failure_files]

    else:

        logm(message='Checking data size', **logging_kwargs)

        df_size = convert_to_from_bytes(value=df.memory_usage(deep=True, index=False).sum(), unit='GB')

        logm(message=f'Splitting into {math.ceil(df_size/2)} chunks', **logging_kwargs)

        # divide dataframe into 2 GB chunks
        batches = np.array_split(df, max(math.ceil(df_size / 2), 1))

    batch_outcome: list = []

    problem_dfs: list = []

    for i, chunk in tqdm(zip([get_batch_num(x) for x in failure_files], batches) if len(failure_files) > 0 else enumerate(batches),
                         total=len(batches),
                         desc=f'Uploading to {dest_schema}.{dest_table}' if (cached_file_type == 'cached') else f'Cacheing before Upload to: {dest_schema}.{dest_table}'
                         if isinstance(cache_before_upload_path, str) else f'Processing and Uploading to: {dest_schema}.{dest_table}'):

        if isinstance(failure_fp, str):
            chunk_failure_fp: str = failure_fp.replace('.pkl', f'_{i}.pkl')
            chunk_debug_fp: str = chunk_failure_fp.replace('.pkl', '_staged.pkl')

        try:
            tm: str = f'Preparing chunk {i+1} of {len(batches)} for upload by comparing dataframe to table constraints'
            if log_update:
                log_database_update(file_name=update_kwargs.get('file_name', 'Unkown Source File Name'),
                                    batch=update_kwargs.get('batch', 'Unknown Source Batch'),
                                    dest_table=dest_table,
                                    note=update_kwargs.get('note', 'Initial Upload'),
                                    engine=engine,
                                    schema=update_kwargs.get('schema', dest_schema),
                                    table_name=update_kwargs.get('table', 'database_update'),
                                    execute_query=False,
                                    min_id=None,
                                    max_id=None,
                                    process_start=None, process_end=None, upload_start=None, status_message=tm,
                                    upload_end=None, github_release=None, crud_query=None)
            logm(message=tm, display=True, **logging_kwargs)

            if cached_file_type == 'cached':
                stage, dtypes2, problems_df = chunk, prepare_table_for_upload(df=None,
                                                                              get_dtypes_only=True,
                                                                              engine=engine,
                                                                              table=dest_table,
                                                                              schema=dest_schema,
                                                                              truncate_to_fit=truncate_to_fit,
                                                                              dtypes=dtypes,
                                                                              skip_character_check=skip_character_check,
                                                                              **kwargs), pd.DataFrame()

            else:
                stage, dtypes2, problems_df = prepare_table_for_upload(df=chunk.copy(),
                                                                       engine=engine,
                                                                       table=dest_table,
                                                                       schema=dest_schema,
                                                                       truncate_to_fit=truncate_to_fit,
                                                                       dtypes=dtypes,
                                                                       skip_character_check=skip_character_check,
                                                                       **kwargs)

            if problems_df.shape[0] > 0:
                logm(message=problems_df, display=True, error=True, **logging_kwargs)
                problem_dfs.append(problems_df)
                raise Exception(f'DataFrame was has {problems_df.shape[0]} format problems')
            elif isinstance(cache_before_upload_path, str):
                file_info: file_components = get_file_name_components(cache_before_upload_path)

                save_data(df=stage,
                          out_path=os.path.join(file_info.directory,
                                                f'{file_info.file_name}_{"_".join([str(x) for x in file_info.batch_numbers])}_chunk_{i}.p'))
                batch_outcome.append(True)
                continue

            tm: str = f'Uploading chunk {i+1} of {len(batches)} to server'
            logm(message=tm, display=True, **logging_kwargs)
            if log_update:
                log_database_update(file_name=update_kwargs.get('file_name', 'Unkown Source File Name'),
                                    batch=update_kwargs.get('batch', 'Unknown Source Batch'),
                                    dest_table=dest_table,
                                    note=update_kwargs.get('note', 'Initial Upload'),
                                    engine=engine,
                                    schema=update_kwargs.get('schema', dest_schema),
                                    table_name=update_kwargs.get('table', 'database_update'),
                                    execute_query=False,
                                    min_id=None,
                                    max_id=None,
                                    process_start=None, process_end=None, upload_start=dt.now() if i == 0 else None, status_message=tm,
                                    upload_end=None, github_release=None, crud_query=None)

            with engine.begin() as conn:
                stage.to_sql(name=dest_table,
                             con=conn,
                             schema=dest_schema,
                             if_exists=if_exists,
                             index=index,
                             chunksize=chunksize,
                             dtype=dtypes2)
                # label success
                batch_outcome.append(True)

                if isinstance(failure_fp, str):
                    # cleanup old files
                    if os.path.exists(chunk_failure_fp):
                        os.remove(chunk_failure_fp)

                    if os.path.exists(chunk_debug_fp):
                        os.remove(chunk_debug_fp)

        except Exception as exc:
            logm(message=exc, display=True, error=True, **logging_kwargs)
            if isinstance(failure_fp, str):
                chunk.to_pickle(chunk_failure_fp)
                if debug:
                    stage.to_pickle(chunk_debug_fp)
                logm(message=chunk_failure_fp, display=True, **logging_kwargs)
            problem_dfs.append(pd.DataFrame({'file': [os.path.basename(failure_fp) if isinstance(failure_fp, str) else dest_table],
                                             'chunk': [i],
                                             'excecption': [exc]}))
            batch_outcome.append(False)

    if all(batch_outcome):
        tm: str = f'{len(batches)} of {len(batches)} successfully {"cached" if isinstance(cache_before_upload_path, str) else "uploaded to server"}'
        if isinstance(success_fp, str):
            open(success_fp, 'a').close()

    else:
        tm: str = f'{len(batch_outcome)-sum(batch_outcome)} of {len(batches)} failed to {"cache" if isinstance(cache_before_upload_path, str) else "upload to server"}'
        logm(message=tm, display=True, **logging_kwargs)

    if log_update:
        log_database_update(file_name=update_kwargs.get('file_name', 'Unkown Source File Name'),
                            batch=update_kwargs.get('batch', 'Unknown Source Batch'),
                            dest_table=dest_table,
                            note=update_kwargs.get('note', 'Initial Upload'),
                            engine=engine,
                            schema=update_kwargs.get('schema', dest_schema),
                            table_name=update_kwargs.get('table', 'database_update'),
                            execute_query=False,
                            min_id=None,
                            max_id=get_min_max_id_from_table(engine=engine, table=dest_table, schema=dest_schema, id_type='max'),
                            process_start=None, process_end=None, status_message=tm,
                            upload_end=dt.now(), github_release=None, crud_query=None)

    if len(problem_dfs) > 0:
        return pd.concat(problem_dfs, axis=0, sort=False)

    else:
        return None


def detect_file_names(directory: str, recursive: bool = True, pattern: str = r'_[0-9]+\.csv',
                      omit_file_names: list = None, drop_duplicates: bool = True, regex: bool = True) -> list:
    r"""
    Get a list of files in a given directory.

    Parameters
    ----------
    directory : str
        DESCRIPTION.
    recursive : bool, optional
        DESCRIPTION. The default is True.
    pattern : str, optional
        DESCRIPTION. The default is r'_[0-9]+\.csv'.
    omit_file_names : list, optional
        DESCRIPTION. The default is None.
    drop_duplicates : bool, optional
        DESCRIPTION. The default is True.

    Returns
    -------
    list
        DESCRIPTION.

    """
    files = pd.Series(find_files(directory=directory,
                                 patterns=pattern,
                                 recursive=recursive,
                                 regex=regex))\
        .apply(lambda entry: re.sub(pattern, '', os.path.basename(entry)))

    if drop_duplicates:
        files = files.drop_duplicates()
    if isinstance(omit_file_names, list):
        files = files[~files.isin(omit_file_names)]
    return sorted(files.to_list())


def get_column_names(directory: str, pattern: str = r'_0\.csv$', recursive: bool = False, regex: bool = True) -> pd.DataFrame:
    """Produce a list of all of the columns in each csv file in a given directory."""
    column_lists: list = []

    pattern: list = pattern if isinstance(pattern, list) else [pattern]

    for entry in find_files(directory=directory, patterns=pattern, recursive=recursive, regex=regex, agg_results=True):

        out_fn: str = [re.sub(p, '', os.path.basename(entry)) for p in pattern if bool(re.search(p, os.path.basename(entry)))][0]

        column_lists.append(pd.DataFrame({out_fn: pd.read_csv(entry, nrows=0, low_memory=False).columns}))

    return pd.concat(column_lists, axis=1)


def split_file(file_path: str, lines_per_chunk: int, out_dir: str, **kwargs):
    """
    Split file by n number of lines.

    Parameters
    ----------
    file_path : str
        file to be split.
    lines_per_chunk : int
        number of lines per chunk.
    out_dir : str
        directory to save chunked files to.
    kwargs: dict
        arguments for the load_data function/underlying pandas read functions.

    Returns
    -------
    None.

    """
    fc = get_file_name_components(file_path)

    file_prefix = str(fc.file_name) + (('_' + '_'.join([str(x) for x in fc.batch_numbers])) if len(fc.batch_numbers) > 0 else '')

    i: int = 0

    for chunk in load_data(file_path_query=file_path, preserve_case=True, chunksize=lines_per_chunk, **kwargs):

        out_path: str = os.path.join(out_dir, f"{file_prefix}_{i}{fc.file_type}")

        save_data(df=chunk, out_path=out_path, keep_all_batch_nums=True)

        i += 1


def configureDaskClient():
    """Configure Dask Client."""
    cmd = "hostname --all-ip-addresses"
    process = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()
    IPADDR = str(output.decode()).split()[0]

    cluster = LocalCUDACluster(ip=IPADDR)
    return Client(cluster)


def check_load_df(input_v, default_kwargs: dict = {}, return_original_input: bool = False,
                  ds_type: str = None, desired_types: dict = None, ensure_cols: list = None,
                  prefix_dict: dict = None, **inputkwargs):
    """
    Convienece method to load a dataframe using the load_data function if needed, else, to format exisiting dataframe.

    Parameters
    ----------
    input_v : str, dask dataframe, pandas dataframe, cudf dataframe
        if input is str, it will load the dataframe, otherwise it will format existing.
    default_kwargs : dict, optional
        default kwargs to be passed to check_load_df function. The default is {}.
    return_original_input : bool, optional
        whether the original input should be returned or not. The default is False.
    ds_type : str, optional
        desired libarary that the output should be in [pandas, cudf, dask]. The default is None.
    desired_types : dict, optional
        dictionary of types to convert columns to. The default is None.
    ensure_cols : list, optional
        columns to ensure are in the dataframe by creating empty ones if necessary. The default is None.
    **inputkwargs : TYPE
        keywords to be passed to the load_data function.

    Returns
    -------
    DataFrame or tuple (input_v, DataFrame)
        Will only return tuple if return original input is set to True.

    """
    kwargs = copy.deepcopy(default_kwargs)
    kwargs.update(inputkwargs)
    logging_kwargs: dict = {x: kwargs.get(x, False if x == 'display' else None) for x in ['log_name', 'log_dir', 'display']}
    if getDataStructureLib(input_v)['type'] == 'dataframe':
        orig_input = None
    elif isinstance(input_v, str):
        kwargs['file_path_query'] = input_v
        orig_input = input_v
        input_v = load_data(**kwargs)
    else:
        logm(message='input_v must be a dataframe or a string, but a {} was passed.'.format(getDataStructureLib(input_v)['type']),
             error=True, raise_exception=True, **logging_kwargs)

    if getDataStructureLib(input_v)['type'] == 'dataframe':
        # convert to different library if necessary
        if isinstance(ds_type, str):
            input_v = convert_to_lib(ds=input_v, desired_lib=ds_type, reset_index=True)

        # check convert coloumns are in correct format
        date_cols: list = kwargs.pop('parse_dates', [])

        if isinstance(desired_types, dict):
            for c, t in desired_types.items():
                if c in input_v:
                    if t == 'format_sparse_int':
                        input_v.loc[:, c] = check_format_series(ds=input_v[c].copy(deep=True), desired_type='str', format_sparse_int=True)
                    else:
                        input_v.loc[:, c] = check_format_series(ds=input_v[c].copy(deep=True), desired_type='datetime' if (c in date_cols) else t)
                else:
                    logm(f'The column {c} was not formatted because it was not in the source dataframe', warning=True)

        elif isinstance(date_cols, list):
            if len(date_cols) > 0:
                input_v = force_datetime(input_v, date_cols=list(set(date_cols)))

        # filter columns
        selected_cols: list = kwargs.pop('usecols', input_v.columns.tolist()) or input_v.columns.tolist()

        if kwargs.pop('tag_source', False) and ('source_file' not in selected_cols):
            selected_cols.append('source_file')
        if kwargs.pop('label_source_row', False) and ('source_row' not in selected_cols):
            selected_cols.append('source_row')

        if not kwargs.pop('preserve_case', False):
            selected_cols = [str(x).lower() for x in selected_cols]
        if kwargs.pop('use_col_intersection', False):
            input_v = input_v[input_v.columns.intersection(selected_cols)]
        else:
            input_v = input_v[selected_cols]

        if isinstance(ensure_cols, list):
            input_v = ensc(df=input_v, cols=ensure_cols)

        if isinstance(prefix_dict, dict):
            for col, prefix in prefix_dict.items():
                input_v.loc[input_v[col].notnull(), col] = prefix + input_v.loc[input_v[col].notnull(), col].astype(str)

    # perform date censoring and format imporper hospital account ids
    if (getDataStructureLib(input_v)['type'] == 'dataframe') and ('dask' not in str(type(input_v))) and not isinstance(orig_input, str):
        censor_date = inputkwargs.pop('censor_date', None)
        if (input_v.shape[0] > 0) and (isinstance(censor_date, str) or isinstance(censor_date, dict)):
            input_v = apply_date_censor(censor_date=censor_date if isinstance(censor_date, str) else censor_date['censor_date'],
                                        df=input_v, df_time_index=None if isinstance(censor_date, str) else censor_date['col'])

        clean_ids = inputkwargs.pop('clean_ids', False)
        if clean_ids:
            if 'hospital_account_deiden_id' in input_v.columns:
                missing_hosp_account: pd.Series = input_v.hospital_account_deiden_id.str.contains(r'_None$', regex=True, case=False, na=False)
                if missing_hosp_account.any():
                    input_v.loc[missing_hosp_account, 'hospital_account_deiden_id'] = None
                del missing_hosp_account

                if 'encounter_deiden_id' in input_v.columns:
                    missing_encounter_idx: pd.Series = input_v.encounter_deiden_id.str.contains(r'_None$', regex=True, case=False, na=False)
                    if missing_encounter_idx.any():
                        input_v.loc[missing_encounter_idx, 'encounter_deiden_id'] = None
                    del missing_encounter_idx

    if return_original_input:
        return input_v, orig_input

    return input_v


def _split_by_indentifer(input_df: pd.DataFrame, id_col: str, split_into_n_groups: int, id_dict: dict = None):
    """
    Split dataframe by identifier column.

    Parameters
    ----------
    input_df : pd.DataFrame
        DESCRIPTION.
    id_col : str
        DESCRIPTION.
    split_into_n_groups : int
        DESCRIPTION.
    id_dict : dict, optional
        DESCRIPTION. The default is None.

    Returns
    -------
    dict
        DESCRIPTION.
    or
    tuple (id_dict: dict, arrya_dict: dict)

    """
    assert isinstance(id_col, str), f'id_col must be of type str, found type: {type(id_col)}'
    assert getDataStructureLib(input_df)['type'] == 'dataframe', f'input_df must be a dataframe, but found: {type(input_df)}'
    assert id_col in input_df.columns, f'The id_col: {id_col} was not in the input dataframe!'

    out: dict = {}
    if isinstance(id_dict, dict):
        cum_ids: list = []
        for k, ids in id_dict.items():
            out[k] = input_df[input_df[id_col].isin(ids)].copy(deep=True)
            cum_ids += ids

        new_ids: list = list(set(input_df[id_col].dropna().unique().tolist()) - set(cum_ids))
        assert len(new_ids) == 0, f'There were {len(new_ids)} in the {id_col} column that were not in the id_dict. This included the following ids: {new_ids}'
        return out
    else:
        id_dict = {i: v.tolist() for i, v in enumerate(np.array_split(input_df[id_col].dropna().unique(), split_into_n_groups))}

        return id_dict, _split_by_indentifer(input_df=input_df, id_col=id_col, split_into_n_groups=split_into_n_groups, id_dict=id_dict)


def split_by_identifier(df: pd.DataFrame, split_by_indentifer_col: str, id_dir: str = None, id_dict: dict = None,
                        split_into_n_batches: int = None, batch_num: str = None, **logging_kwargs):
    """
    Split dataframe by a specified column into batches.

    Actions:
        1. Attempt to load id_dict if one is not provided.
        2. Create a new id_dict if one is not provided or found in id_dir.
        3. Split input file using id_dict where each id only belongs to one sub_batch.
        4. Return list of dataframes.

    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe to be split.
    split_by_indentifer_col : str
        column to use for the split.
    id_dir : str, optional
        Directory to save the split definition to. Only requried if there is not a pre-existing id_dict. The default is None.
    id_dict : dict, optional
        dictionary of {key(int): id_list(list)} to specify the split. The default is None.
    split_into_n_batches : int, optional
        number of batches to split source file into. Only required if there is not a pre-existing id_dict. The default is None.
    batch_num : str, optional
        Batch number for the id_dict. The default is None.
    **logging_kwargs : TYPE
        Keywords such as log_name to be passed to the log_print_email_message function from the Utils.log_messages module.

    Returns
    -------
    dictionary
        dictionary of pandas dataframes.

    """
    assert getDataStructureLib(df)['type'] == 'dataframe', f'df, must be a dataframe; however, a {type(df)} was found'
    assert isinstance(id_dict, dict) or isinstance(id_dir, str), 'id_dir is required if no id_dict is provided'

    if not isinstance(id_dict, dict):
        id_files: list = find_files(directory=id_dir, patterns=[r'{}_id_batch_{}_[0-9]+\.csv'.format(split_by_indentifer_col, batch_num),
                                                                r'{}_id_batch_[0-9]+\.csv'.format(split_by_indentifer_col)],
                                    regex=True)
        if len(id_files) > 0:
            logm(message=f'Splitting using Existing {split_by_indentifer_col}_id_batch_defintions', **logging_kwargs)
            id_dict: dict = {}
            for f in id_files:
                try:
                    id_dict[get_file_name_components(f).batch_numbers[-1]] = _load_file(f, eid=None, pid=None, header=None).iloc[:, 0].tolist()
                except pd.errors.EmptyDataError:  # cover edge case for when there may be more batches then unique identifiers in a file
                    id_dict[get_file_name_components(f).batch_numbers[-1]] = []

    assert isinstance(id_dict, dict) or isinstance(split_into_n_batches, int), 'split_into_n_batches is required if no id_dict is provided'

    temp = _split_by_indentifer(input_df=df, id_col=split_by_indentifer_col, split_into_n_groups=split_into_n_batches, id_dict=id_dict)
    if isinstance(temp, tuple):
        logm(message=f'Creating new {split_by_indentifer_col}_id_batch_defintions', **logging_kwargs)
        for group, ids in temp[0].items():
            pd.Series(ids).to_csv(os.path.join(id_dir,
                                               f'{split_by_indentifer_col}_id_batch_{(batch_num + "_" + str(group)) if isinstance(batch_num, str) else group}.csv'),
                                  index=False, header=None)
        return temp[1]
    else:
        return temp


def get_batches_from_directory(directory: str, batches: list = None, file_name: str = '^encounters_clean', independent_sub_batches: bool = True) -> list:
    """
    Determine unique batches in directory using a key file.

    Parameters
    ----------
    directory : str
        directory to search.
    batches : list, optional
        list of batches to start with as a basis for a target search. The default is None.
    file_name : str, optional
        file_name to search for in directory. The default is '^encounters_clean'.
    independent_sub_batches : bool, optional
        Whether sub batches should be treated indendenelty of not. The default is True.

    Returns
    -------
    list
        DESCRIPTION.

    """
    if isinstance(batches, list) and independent_sub_batches:
        if all([bool(re.search(r'[0-9]+[subsetchnk_]+[0-9]+', x)) for x in batches]):
            return batches
        return extract_batch_numbers(file_list=[item for sublist in [find_files(directory=directory,
                                                                                patterns=[r'{}_{}_[0-9]+\.csv'.format(file_name, x), r'{}_{}_chunk_[0-9]+\.csv'.format(file_name, x),
                                                                                          r'{}_{}_subset_[0-9]+\.csv'.format(file_name, x), r'{}_{}\.csv'.format(file_name, x)],
                                                                                regex=True) for x in batches] for item in sublist],
                                     independent_sub_batches=independent_sub_batches)
    elif isinstance(batches, list):
        return batches
    else:
        return extract_batch_numbers(file_list=find_files(directory=directory,
                                                          patterns=[r'{}_[0-9]+_chunk_[0-9]+\.csv'.format(file_name),
                                                                    r'{}_[0-9]+_subset_[0-9]+\.csv'.format(file_name),
                                                                    r'{}_[0-9]+\.csv'.format(file_name),
                                                                    r'{}_[0-9]+_[0-9]+\.csv'.format(file_name)],
                                                          regex=True),
                                     independent_sub_batches=independent_sub_batches)


def query_folder_with_sql(sql_query: str, query_folder: str, db_fp: str = None,
                          patterns: list = [r'_clean_[0-9_]+_optimized_ids\.csv', r'_clean_[0-9_]+\.csv', r'_[0-9_]+_optimized_ids\.csv', r'_[0-9_]+\.csv', r'\.csv'],
                          return_db_connection: bool = False, load_all_cols: bool = False,
                          overwrite_existing_tables: bool = False, load_only: bool = False,
                          replacements: dict = None, tag_source: bool = False,
                          label_source_row: bool = False, **logging_kwargs) -> pd.DataFrame:
    """
    Run SQLite Queries on .csv files on the file system.

    Parameters
    ----------
    sql_query : str
        SQLite Query.
    query_folder : str
        folder with the .csv files to query.
    db_fp : str, optional
        a file_path to store the created database. The default is None.
    return_db_connection : bool, optional
        Whether or not to return the database connection. The default is False.
    load_all_cols : bool, optional
        load all the columns in each specified file or just the ones necessary for the query. The default is False.
    overwrite_existing_tables : bool, optional
        Whether to overwrite existing tables in the database by a given name or to fail when writing to them. The default is False.

    Returns
    -------
    pd.DataFrame or tuple(pd.DataFrame, sqlite3.Connection)
        result of the query with or without the associated sqlite3 connection object.

    """
    if re.search(r'\.sql$', sql_query, re.IGNORECASE):
        sql_query = check_load_df(sql_query, replacements=replacements, **logging_kwargs)
    table_map: dict = _check_columns(table_map=_parse_columns(sql_query,
                                                              table_map=_parse_tables(sql_query)),
                                     dir_fp=query_folder, patterns=patterns,
                                     label_source_row=label_source_row, tag_source=tag_source)

    conn = sq.connect(db_fp if isinstance(db_fp, str) else ':memory:')

    _load_data_into_db(table_map=table_map, dir_fp=query_folder, load_all_cols=load_all_cols,
                       con=conn, overwrite_existing=overwrite_existing_tables, patterns=patterns,
                       label_source_row=label_source_row, tag_source=tag_source, **logging_kwargs)

    if load_only:
        return conn

    result: pd.DataFrame = pd.read_sql(sql_query, con=conn)

    if return_db_connection:
        return result, conn

    conn.close()

    return result


def _parse_tables(sql_query: str) -> dict:
    from sql_metadata import Parser
    table_map: dict = {}

    for k in Parser(sql_query).tables:
        table_map[k] = {'file_name': k}

    return table_map


def _parse_columns(sql_query: str, table_map: dict) -> dict:
    from sql_metadata import Parser
    for vals in [x.split('.') for x in Parser(sql_query).columns]:
        if len(vals) == 2:
            assert vals[0] in table_map, f'Unknown Table {vals[0]} referenced in SELECT statement. Known tables include {table_map.keys()}'
            table_map[vals[0]]['columns'] = table_map[vals[0]].get('columns', []) + [vals[1]]
        elif len(table_map) == 1:
            table_map[list(table_map.keys())[0]]['columns'] = table_map[list(table_map.keys())[0]].get('columns', []) + [vals[0]]
        else:
            if 'In_Explicit_Cols_XXXX' in table_map:
                table_map['In_Explicit_Cols_XXXX']['columns'] = table_map['In_Explicit_Cols_XXXX']['columns'] + [vals[0]]
            else:
                table_map['In_Explicit_Cols_XXXX'] = {'columns': [vals[0]]}

    return table_map


def _check_columns(table_map: dict, dir_fp: str, patterns: list, label_source_row: bool, tag_source: bool) -> dict:
    # get list of columns in each table
    for table in [x for x in table_map if x != 'In_Explicit_Cols_XXXX']:
        fname = table_map.get(table).get('file_name')
        files: list = find_files(directory=dir_fp,
                                 patterns=[r'^{}{}'.format(fname, x) for x in patterns],
                                 recursive=False, regex=True,
                                 agg_results=False, exclusion_patterns=None)
        assert len(files) > 0, f'No table by the name {fname} was found in the directory'
        table_map[table]['actual_cols'] = check_load_df(files[0], nrows=1, label_source_row=label_source_row, tag_source=tag_source).columns

    # assign and inexplicit cols to tables
    for col in table_map.get('In_Explicit_Cols_XXXX', {}).get('columns', []):
        found: dict = {}
        for f in [x for x in table_map if x != 'In_Explicit_Cols_XXXX']:
            if col in table_map.get(f).get('actual_cols'):
                found[f] = col
        assert len(found) < 2, f'The column name: {col} is ambiguous and was found in {list(found.keys())}, Please specify the source table'
        assert len(found) == 1, f'The column: {col} was not found in any table'
        for k, v in found.items():
            table_map[k]['columns'] = table_map[k].get('columns', []) + [v]
    table_map.pop('In_Explicit_Cols_XXXX', None)

    # deduplicate columns and verify they are in the source
    for table in table_map:
        table_map[table]['columns'] = list(set(table_map.get(table).get('columns', [])))

        if '*' in table_map.get(table).get('columns'):
            table_map[table]['columns'] = table_map.get(table).get('actual_cols').tolist()

        missing_cols: list = pd.Index(table_map.get(table).get('columns')).difference(table_map.get(table).get('actual_cols')).tolist()
        assert len(
            missing_cols) == 0, f"There were {len(missing_cols)} missing columns in table {table} that included: {missing_cols}. The columns in that table are: {table_map.get(table).get('actual_cols').tolist()}"

    return table_map


def _load_data_into_db(table_map: dict, dir_fp: str, load_all_cols: bool, con: sq.Connection, overwrite_existing: bool, patterns: list,
                       label_source_row: bool, tag_source: bool, **logging_kwargs):
    for table in table_map:
        check_load_df(input_v=r'^{}'.format(table_map.get(table).get('file_name')),
                      directory=dir_fp,
                      patterns=patterns,
                      recursive=False, regex=True,
                      usecols=None if load_all_cols else table_map.get(table).get('columns'),
                      dtype=None,
                      allow_empty_files=True,
                      label_source_row=label_source_row,
                      tag_source=tag_source,
                      use_col_intersection=True,
                      ds_type='pandas',
                      **logging_kwargs)\
            .to_sql(name=table_map.get(table).get('file_name'),
                    con=con,
                    if_exists='replace' if overwrite_existing else 'fail',
                    chunksize=1000,
                    index=False)


def split_file_using_batch_definitions(df: pd.DataFrame, batch_def_dir: str, split_by_indentifer_col: str, out_path: str, **logging_kwargs):
    """
    Split file using batch definitions.

    Parameters
    ----------
    df : pd.DataFrame
        DESCRIPTION.
    batch_def_dir : str
        DESCRIPTION.
    split_by_indentifer_col : str
        DESCRIPTION.
    out_path : str
        DESCRIPTION.
    **logging_kwargs : TYPE
        DESCRIPTION.

    Returns
    -------
    None.

    """
    assert split_by_indentifer_col in df.columns, f'split_by_indentifer_col: {split_by_indentifer_col} was not found in the input df. The following columns were: {df.columns.tolist()}'
    # load and format defintions
    batch_def_df = load_data(f'{split_by_indentifer_col}_id_batch', patterns=[r'_[0-9_]+\.csv'], directory=batch_def_dir, header=None, tag_source=True, **logging_kwargs)
    batch_def_df.columns = [split_by_indentifer_col, 'batch']
    batch_def_df.batch = batch_def_df.batch.apply(lambda x: '_'.join([str(y) for y in get_file_name_components(x).batch_numbers]))

    # warn if ids not in definitions
    missing_ids: list = pd.Index(df[split_by_indentifer_col].unique()).difference(batch_def_df[split_by_indentifer_col]).tolist()
    if len(missing_ids) > 0:
        logm(message=f'There were {len(missing_ids)} ids in the input dataframe that were not in the batch_definition files. Including: {missing_ids}',
             **logging_kwargs)

    # identify direcotry, file_type, and file name from out_path
    out_c = get_file_name_components(out_path)

    # split and write file
    for batch in batch_def_df.batch.unique():
        save_data(df=df[df[split_by_indentifer_col].isin(batch_def_df.loc[batch_def_df.batch == batch, split_by_indentifer_col])],
                  out_path=os.path.join(out_c.directory, f'{out_c.file_name}_{batch}{out_c.file_type}'),
                  **logging_kwargs)


def make_if_not_exists(fp: str):
    """
    Make a folder path if it does not exist.

    Parameters
    ----------
    fp : str
        folder path to create.

    Returns
    -------
    None.

    """
    if not os.path.exists(fp):
        os.makedirs(fp)
