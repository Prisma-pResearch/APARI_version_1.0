# -*- coding: utf-8 -*-
"""
Created on Tue Jun 27 10:48:58 2023

@author: ruppert20
"""
import os
import pandas as pd
import re
from tqdm import tqdm
from typing import Union, List, Dict
from .standardization_functions_config_helper import process_df_with_pre_processing_instructions
from .standardization_functions import process_df_v2
from ..FileHandling.io import check_load_df, save_data
from .data_format_and_manipulation import move_cols_to_front_back_sort
from ..Logging.log_messages import log_print_email_message as logm


class Standardized_data:
    """Class for manipulating data before and after standardization with pre_process_df."""

    def __init__(self,
                 instruction_fp: str,
                 raw_df: Union[pd.DataFrame, str] = None,
                 processed_df: Union[pd.DataFrame, str, None] = None,
                 raw_loading_kwargs: Union[dict, None] = None,
                 processed_loading_kwargs: Union[dict, None] = None,
                 ensure_processed_types: bool = True,
                 save_fp: Union[str, None] = None,
                 save_kwargs: Union[dict, None] = None,
                 post_resample_df: Union[pd.DataFrame, None] = None,
                 pre_resample_df: Union[pd.DataFrame, None] = None,
                 sparse_int_ids: List[str] = ['person_id', 'subject_id', 'visit_occurrence_id', 'visit_detail_id'],
                 run_standardization: bool = False,
                 **standardization_kwargs):

        # extract logging kwargs
        logging_kwargs: dict = {x: standardization_kwargs.get(x, False if x == 'display' else None) for x in ['log_name', 'log_dir', 'display']}

        # ensure either source or final product is present
        assert isinstance(raw_df, (pd.DataFrame, str)) or isinstance(processed_df, (pd.DataFrame, str))

        # load source if indicated
        self.raw_df: pd.DataFrame = check_load_df(raw_df, **(raw_loading_kwargs or logging_kwargs)) if isinstance(raw_df, (pd.DataFrame, str)) else None

        # ensure instruction fp is provided
        assert isinstance(instruction_fp, str)
        self.instruction_fp = instruction_fp

        # try to load processed df, else try to make it if indicated
        if isinstance(processed_df, (pd.DataFrame, str)):
            self.processed_df = check_load_df(processed_df, **(processed_loading_kwargs or logging_kwargs))
            save_candidate: bool = False
        elif run_standardization:
            save_candidate: bool = True
            assert isinstance(self.raw_df, pd.DataFrame), f'The raw df must be of type pd.Dataframe; however, one of type {type(self.raw_df)} was found'

            if isinstance(standardization_kwargs.get('helper_instruct_fp', None), str):
                self.processed_df = process_df_with_pre_processing_instructions(df=self.raw_df.copy(deep=True), instruction_fp=instruction_fp, **standardization_kwargs)
            else:
                self.processed_df = process_df_v2(df=self.raw_df.copy(deep=True), instruction_fp=instruction_fp, **standardization_kwargs)
        else:
            save_candidate: bool = False

        assert os.path.exists(instruction_fp)
        assert os.path.isfile(instruction_fp)

        # load instructions
        self.instruction_df = check_load_df(instruction_fp).query('(output_dtype != "parameter") & (drop_column.isnull() | drop_column.isin([False]))', engine='python')
        self.type = 'time_series' if self.instruction_df.output_dtype.isin(['time_index']).any() else 'static'
        self.time_index_col: Union[str, None] = self.instruction_df.loc[self.instruction_df.output_dtype == 'time_index', 'column_name'].iloc[0] if (self.instruction_df.output_dtype == 'time_index').any() else None

        if self.type == 'time_series':
            self.pre_resample_instruction_df = self.instruction_df.query('pre_resample.isin([1, "1"])', engine='python')
            self.instruction_df = self.instruction_df.query('pre_resample.isin([0, "0"])', engine='python')

        # save data if requested
        if save_candidate:
            if isinstance(self.processed_df, tuple):
                self.pre_resample_df = self.processed_df[2]
                self.post_resample_df = self.processed_df[1]
                self.processed_df = self.processed_df[0]

                if isinstance(save_fp, str):
                    save_data(self.processed_df, out_path=save_fp, **logging_kwargs, **(save_kwargs or ({'dataset': 'ts'} if '.h5' in save_fp else {'index': True} if '.csv' in save_fp else {})))
                    save_data(self.post_resample_df, out_path=save_fp, suffix_label='post_resampled', **logging_kwargs, **(save_kwargs or ({'dataset': 'ts'} if '.h5' in save_fp else {'index': True} if '.csv' in save_fp else {})))
                    save_data(self.pre_resample_df, out_path=save_fp, suffix_label='pre_resampled', **logging_kwargs, **(save_kwargs or ({'dataset': 'ts'} if '.h5' in save_fp else {'index': True} if '.csv' in save_fp else {})))
            else:
                save_data(self.processed_df, out_path=save_fp, **logging_kwargs, **(save_kwargs or ({'dataset': 'static'} if '.h5' in save_fp else {'index': True} if '.csv' in save_fp else {})))

        # exctract variable metadata
        self.str_columns: pd.Series = self.instruction_df.loc[self.instruction_df.output_dtype == 'str', 'column_name']
        self.object_columns: pd.Series = self.instruction_df.loc[self.instruction_df.output_dtype == 'object', 'column_name']
        self.float_columns: pd.Series = self.instruction_df.loc[self.instruction_df.output_dtype == 'float', 'column_name']
        self.int_columns: pd.Series = self.instruction_df.loc[self.instruction_df.output_dtype == 'int', 'column_name']
        self.raw_cat_one_hot_columns: pd.Series = self.instruction_df.loc[self.instruction_df.output_dtype == 'cat_one_hot', 'column_name']
        self.one_hot_columns: pd.Series = self.instruction_df.loc[self.instruction_df.output_dtype == 'cat_one_hot', ['column_name', 'levels']]\
            .apply(lambda row: [f'{row.column_name}_{x}'.lower().replace(' ', '_') for x in row.levels.split('XXXXSEPXXXX')], axis=1).explode() if (self.instruction_df.output_dtype == 'cat_one_hot').any() else pd.Series(dtype=object)
        self.cat_embedding_columns: pd.Series = self.instruction_df.loc[self.instruction_df.output_dtype == 'cat_embedding', 'column_name']
        self.binary_columns: pd.Series = pd.concat([self.instruction_df.loc[(self.instruction_df.output_dtype == 'binary') & ~self.instruction_df.column_name.str.contains(r'_missing_ind$', regex=True, na=False), 'column_name'],
                                                    self.one_hot_columns], ignore_index=True)
        self.datetime_columns: pd.Series = self.instruction_df.loc[self.instruction_df.output_dtype.isin(['datetime', 'timestamp']), 'column_name']
        self.numeric_columns: pd.Series = self.instruction_df.loc[self.instruction_df.output_dtype.isin(['float', 'int']), 'column_name']
        self.raw_categorical_columns: pd.Series = self.instruction_df.loc[self.instruction_df.output_dtype.isin(['cat_embedding', 'cat_one_hot']), 'column_name']
        self.categorical_columns: pd.Series = pd.concat([self.one_hot_columns, self.cat_embedding_columns], ignore_index=True)
        self.index_columns: pd.Series = self.instruction_df.loc[self.instruction_df.output_dtype == 'index_column', 'column_name']
        self.id_column: Union[str, None] = self.instruction_df.loc[self.instruction_df.output_dtype == 'id_index', 'column_name'].iloc[0] if (self.instruction_df.output_dtype == 'id_index').any() else None
        self.binary_indicators: pd.Series = self.instruction_df.column_name[self.instruction_df.column_name.str.contains('_missing_ind', na=False, regex=False)]
        self.indicator_map: dict = {x: x.replace('_missing_ind', '') for x in self.binary_indicators}
        self.reverse_indicator_map: dict = {x.replace('_missing_ind', ''): x for x in self.binary_indicators}

        if self.type == 'time_series':
            self.column_source_map: dict = {}
            for col in [x for x in self.processed_df.columns if (x not in self.one_hot_columns) and (x not in [self.id_column, self.time_index_col])]:
                search_result = re.search(r'^' + '|^'.join(self.pre_resample_instruction_df.column_name.tolist()), col)

                self.column_source_map[col] = search_result.group(0) if bool(search_result) else col
        else:
            self.column_source_map: dict = {x: x.replace('_missing_ind', '') for x in self.processed_df.columns if not self.one_hot_columns.isin([x]).any()}

        if (self.instruction_df.output_dtype == 'cat_one_hot').any():
            self.column_source_map.update(self.instruction_df.loc[self.instruction_df.output_dtype == 'cat_one_hot', ['column_name', 'levels']]
                                          .set_index('column_name')
                                          .apply(lambda row: [f'{row.name}_{y}'.lower() for y in row.levels.split('XXXXSEPXXXX')], axis=1)
                                          .explode().rename('levels').str.replace(' ', '_').reset_index(drop=False).set_index('levels').column_name.to_dict())

        # determine dtypes
        self.dtypes = {k: self.instruction_df.loc[self.instruction_df.column_name == v, 'output_dtype'].iloc[0] for k, v in self.column_source_map.items()}
        self.raw_dtypes = {k: self.instruction_df.loc[self.instruction_df.column_name == v, 'source_dtype'].iloc[0] for k, v in self.column_source_map.items()}

        if ensure_processed_types and not save_candidate:
            self.processed_df = check_load_df(self.processed_df, desired_types={k: 'sparse_int' if k in sparse_int_ids else v for k, v in self.dtypes.items() if v != 'index_column'})

        # load the pre/post resample dataframes if indicated
        self.pre_resample_df = check_load_df(pre_resample_df, **(processed_loading_kwargs or logging_kwargs)) if pre_resample_df else None
        self.post_resample_df = check_load_df(post_resample_df, **(processed_loading_kwargs or logging_kwargs)) if post_resample_df else None

        # ensure id column is easily accessible
        if isinstance(self.processed_df, pd.DataFrame):
            if self.id_column not in self.processed_df:
                self.processed_df = self.processed_df.reset_index(drop=False)

        if isinstance(self.pre_resample_df, pd.DataFrame):
            if self.id_column not in self.pre_resample_df:
                self.pre_resample_df = self.pre_resample_df.reset_index(drop=False)

        if isinstance(self.post_resample_df, pd.DataFrame):
            if self.id_column not in self.post_resample_df:
                self.post_resample_df = self.post_resample_df.reset_index(drop=False)

        # make list of ids contained within
        self.ids: pd.Series = self.processed_df[self.id_column]

    def get_ids(self, id_list: Union[list, pd.Series, None] = None) -> pd.DataFrame:

        cols: list = [self.id_column, self.time_index_col] if self.type == 'time_series' else [self.id_column]

        if isinstance(id_list, (list, pd.Series)):
            return self.processed_df.loc[self.processed_df.subject_id.isin(id_list), cols].sort_values(cols, ascending=True).reset_index(drop=True).copy(deep=True)

        return self.processed_df[cols].sort_values(cols, ascending=True).reset_index(drop=True).copy(deep=True)

    def get_enriched_instructions(self):
        return pd.Series(self.column_source_map, name='source_column_name')\
            .reset_index(drop=False).rename(columns={'index': 'column_name'})\
            .merge(self.instruction_df.rename(columns={'column_name': 'source_column_name'}),
                   how='left',
                   on='source_column_name')

    def get_data_type(self,
                      data_types: Union[str, List[str]],
                      source: str = 'processed',
                      include_id: bool = True,
                      include_index: bool = False,
                      include_indicators: bool = False,
                      id_list: Union[list, pd.Series, None] = None,
                      set_ids_to_index: bool = True) -> pd.DataFrame:
        """
        Retrieve data of specified types from raw or processed dataframes.

        Parameters
        ----------
        data_types : Union[str, List[str]]
            datatypes to retrieve.
        source : str, optional
            whether the data should come from the proccessed_df, raw, or both. Both is usefull for debugging standardization code. The default is 'processed'.
        include_id : bool, optional
            whether the id column should be included. The default is True.
        include_index : bool, optional
            whether index columns should be included. The default is False.
        include_indicators : bool, optional
            whether indicators derived from the selected data_types should be included.. The default is False.

        Returns
        -------
        pd.DataFrame
            Pandas dataframe with requested information.

        """
        # check source validity
        assert source in ['processed', 'raw', 'both']

        # check type validity
        assert isinstance(data_types, (str, list))
        type_dict: dict = {'str': self.str_columns,
                           'object': self.object_columns,
                           'float': self.float_columns,
                           'int': self.int_columns,
                           'cat_one_hot': self.raw_cat_one_hot_columns if source == 'raw' else pd.concat([self.raw_cat_one_hot_columns, self.one_hot_columns], ignore_index=True) if source == 'both' else self.one_hot_columns,
                           'cat_embedding': self.cat_embedding_columns,
                           'binary': self.binary_columns,
                           'datetime': self.datetime_columns,
                           'numeric': self.numeric_columns,
                           'categorical': self.categorical_columns if source == 'processed' else
                           pd.concat([self.raw_cat_one_hot_columns, self.cat_embedding_columns]) if source == 'raw'
                           else pd.concat([self.cat_embedding_columns, self.raw_cat_one_hot_columns, self.one_hot_columns], ignore_index=True),
                           'index': self.index_columns,
                           'id': pd.Series([self.id_column, self.time_index_col])}

        data_types: List[str] = list(type_dict.keys()) if data_types == 'all' else [data_types] if isinstance(data_types, str) else data_types

        assert all([x in type_dict for x in data_types]), f'The following data_type(s) are not found: {[x for x in data_types if x not in type_dict]}. Please choose from the following options: {list(type_dict.keys())}'

        out_types: List[str] = data_types + (['id'] if (include_id or (source == 'both') or isinstance(id_list, (pd.Series, list))) and ('id' not in data_types) else []) + (['index'] if include_index and ('index' not in data_types) else [])
        out_cols: List[str] = []
        for dt in out_types:
            out_cols += type_dict.get(dt).dropna().tolist()

        if include_indicators:
            for col in out_cols:
                if col in self.reverse_indicator_map:
                    out_cols.append(self.reverse_indicator_map.get(col))

        idx_id_col_list: List[str] = type_dict['id'].dropna().tolist() + type_dict['index'].tolist()

        if source == 'raw':
            out: pd.DataFrame = self.raw_df[out_cols]
        elif source == 'processed':
            out: pd.DataFrame = self.processed_df[out_cols]
        else:
            out: pd.DataFrame = self.raw_df[self.raw_df.columns.intersection(out_cols)].rename(columns={x: f'{x}_raw' for x in out_cols if x not in idx_id_col_list + self.raw_cat_one_hot_columns.tolist()})\
                .merge(self.processed_df[self.processed_df.columns.intersection(out_cols)],
                       how='inner',
                       on=list(set(idx_id_col_list).intersection(out_cols)))

        out: pd.DataFrame = move_cols_to_front_back_sort(df=out, to_front=list(set(idx_id_col_list).intersection(out_cols)), sort_middle=True)

        if isinstance(id_list, (list, pd.Series)):
            out = out.loc[out.subject_id.isin(id_list), :]

        if set_ids_to_index:
            out.set_index(type_dict['id'].dropna().tolist(), inplace=True)

            if self.type == 'time_series':
                out.sort_index(level=[self.id_column, self.time_index_col], inplace=True, ascending=True)
            else:
                out.sort_index(level=[self.id_column], inplace=True, ascending=True)

        return out.copy(deep=True)


def build_dataset(datasets: Dict[str, Union[dict, Standardized_data]],
                  cohort_df: Union[pd.DataFrame, str],
                  subject_id_col: str,
                  out_fp: str,
                  y: Union[pd.DataFrame, None] = None,
                  drop_dtypes: List[str] = ['object', 'datetime', 'timestamp'],
                  **logging_kwargs):

    ready_data: Dict[str, Union[Standardized_data, pd.DataFrame]] = {k: v if isinstance(v, Standardized_data) else Standardized_data(**v) for k, v in datasets.items()}

    if isinstance(y, pd.DataFrame):
        ready_data['xxxxYxxxx'] = y.rename(columns={subject_id_col: 'subject_id'})

    cohort_df: pd.DataFrame = check_load_df(cohort_df, ds_type='pandas').rename(columns={subject_id_col: 'subject_id'})

    assert isinstance(cohort_df, pd.DataFrame)
    assert 'cohort' in cohort_df.columns
    groups: list = cohort_df.cohort.str.extract(r'(Development|Test|Validation|Train)', expand=False).unique().tolist()

    if (('Test' in groups) and ('Development' in groups)):
        groups.append(r'Development|Test')

    # Load transformation instructions
    observed_types: set = set()
    id_pool: set = set(cohort_df.subject_id.unique().tolist())
    og_size: int = len(id_pool)
    time_series_key: str = None

    for k, v in ready_data.items():

        observed_ids: set = set(v.ids if isinstance(v, Standardized_data) else y['subject_id'])

        if isinstance(v, Standardized_data):
            observed_types.update(set(v.dtypes.values()))

        if isinstance(v, Standardized_data):
            if v.type == 'time_series':
                time_series_key: str = k

        missing_ids: list = list(id_pool.difference(observed_ids))

        if len(missing_ids) > 0:

            logm(f'dropping {len(missing_ids)} missing ids due to their abasence from {k.replace("xxxxYxxxx", "y")}', **logging_kwargs)

            cohort_df = cohort_df[~cohort_df.subject_id.isin(missing_ids)]

    if og_size != cohort_df.shape[0]:
        logm(f'{(og_size-cohort_df.shape[0])/og_size:.2%} of the cohort was reduced', **logging_kwargs)

    dsets_to_write: list = []

    observed_types = observed_types.difference(['index_column', 'id_index', 'time_index'] + drop_dtypes)

    if ('int' in observed_types) or ('float' in observed_types):
        observed_types.add('numeric')

        observed_types = observed_types.difference(['int', 'float'])

    with tqdm(total=int(len(datasets) * len(groups) * len(observed_types)) + (int(len(groups)) if isinstance(y, pd.DataFrame) else 0), desc='Making Dataset') as pbar:

        for group in groups:
            ids: list = cohort_df.loc[cohort_df.cohort.str.contains(group), 'subject_id']

            if isinstance(time_series_key, str):
                logm(message=f'Calculating {group} Sequence lengths', display=True)
                ts_idx_info = ready_data[time_series_key].get_ids(id_list=ids)\
                    .drop(columns=[ready_data[time_series_key].time_index_col])\
                    .reset_index(drop=False)\
                    .rename(columns={'index': 'start_idx'})\
                    .groupby(ready_data[time_series_key].id_column, group_keys=False)\
                    .start_idx\
                    .agg({'first', 'count'})\
                    .rename(columns={'first': 'start_idx',
                                     'count': 'ts_seqlens'})

                print(ts_idx_info.head(4))
                print(ts_idx_info.tail(4))

                dsets_to_write.append({'group': f'{group}', 'df': ts_idx_info[['start_idx']], 'dataset': 'ts_start_idx'})
                dsets_to_write.append({'group': f'{group}', 'df': ts_idx_info[['ts_seqlens']], 'dataset': 'ts_seqlens'})

            for dsn, rdf in ready_data.items():

                if dsn == 'xxxxYxxxx':
                    dsets_to_write.append({'group': group, 'df': rdf[rdf['subject_id'].isin(ids)].set_index('subject_id').sort_index(level=['subject_id'], ascending=True), 'dataset': 'y'})
                    pbar.update(1)
                else:
                    for tp in observed_types:
                        tdf: pd.DataFrame = rdf.get_data_type(data_types=tp,
                                                              id_list=ids,
                                                              include_indicators=True)

                        missing_ind_cols: List[str] = [x for x in tdf.columns if '_missing_ind' in x]

                        if len(missing_ind_cols) > 0:
                            dsets_to_write.append({'group': f'{group}',
                                                  'df': tdf[missing_ind_cols],
                                                   'dataset': f'{dsn}_{tp}_missing_ind'})
                            tdf.drop(columns=missing_ind_cols, inplace=True)

                        if tdf.shape[1] == 0:
                            pbar.update(1)
                            continue

                        dsets_to_write.append({'group': f'{group}',
                                               'df': tdf,
                                               'dataset': f'{dsn}_{tp}'})
                        pbar.update(1)

    save_data(df=None, out_path=out_fp,
              replace_groups=True,
              group_dataset_df_dict_list=dsets_to_write, use_pandas=False, **logging_kwargs)
