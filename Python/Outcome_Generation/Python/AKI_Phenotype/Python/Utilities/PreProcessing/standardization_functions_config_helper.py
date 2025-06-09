# -*- coding: utf-8 -*-
"""
Created on Mon Jun 19 16:51:41 2023

@author: ruppert20
"""
import pandas as pd
import inspect
from typing import Dict
import json
from .standardization_functions import process_df_v2
from .data_format_and_manipulation import coalesce
from ..FileHandling.io import check_load_df


def process_df_with_pre_processing_instructions(helper_instruct_fp: str, **kwargs) -> pd.DataFrame:

    instruct_df = check_load_df(helper_instruct_fp, sheet_name='Pre-Processing Instructions', dtype=None)
    standardization_dict: dict = _load_config_dict(helper_instruct_fp)

    instruct_df = instruct_df.where(pd.notnull(instruct_df), None)
    instruct_parameters_idx: pd.Series = instruct_df.column_name == 'process_df_parameters'

    if instruct_parameters_idx.sum() == 1:
        instruct_df_parameters: dict = instruct_df[instruct_parameters_idx].iloc[0, :].to_dict()
    else:
        instruct_df_parameters: dict = {}

    final_kwargs: Dict[str, any] = {}
    for k, v in inspect.signature(process_df_v2).parameters.items():
        if k in ['df', 'train_ids', 'start', 'end', 'default_na_values']:
            final_kwargs[k] = kwargs.get(k, v)
        elif k in ['time_index_col', 'id_index']:
            t_idx: pd.Series = instruct_df.output_dtype == k
            final_kwargs[k] = kwargs.get(k, instruct_df.loc[t_idx, 'column_name'].iloc[0] if t_idx.any() else v.default if v.default is not inspect.Parameter.empty else None)
        elif k == 'index_cols':
            i_idx: pd.Series = (instruct_df.output_dtype == 'index_column') & (~instruct_df.column_name.str.contains("missing_ind", na=True))
            final_kwargs[k] = kwargs.get(k, instruct_df.loc[i_idx, 'column_name'].tolist() if i_idx.any() else v.default if v.default is not inspect.Parameter.empty else None)
        elif k == 'master_config_dict':
            # candidate_dict: dict = instruct_df.query('column_name != "process_df_parameters"').set_index('column_name').apply(_process_column_instructs, axis=1).to_dict()
            # candidate_dict.update(kwargs.get(k, {}))
            final_kwargs[k] = instruct_df.query('column_name != "process_df_parameters"').set_index('column_name').apply(_process_column_instructs, standard_dict=standardization_dict, axis=1).to_dict()
        elif k == 'logging_kwargs':
            for y in ['log_name', 'display', 'log_dir']:
                if y in kwargs:
                    final_kwargs[y] = kwargs.get(y)
        else:
            try:
                final_kwargs[k] = coalesce(kwargs.get(k, instruct_df_parameters.get(k)), v.default if v.default is not inspect.Parameter.empty else None)
            except ValueError:
                print(k)
                print(v)
                raise Exception('stop here')

    return process_df_v2(**final_kwargs)


def _process_column_instructs(row: pd.Series, standard_dict: dict) -> dict:

    out: dict = {}

    for k, v in row.to_dict().items():

        if k in ['drop_column', 'output_dtype', 'ensure_col', 'standardization_values', 'na_values', 'case_standardization',
                 'missing_value', 'other_value', 'missingness_threshold', 'min_categorical_count', 'lower_limit_percentile',
                 'upper_limit_percentile', 'fill_lower_upper_bound_percentile', 'fill_upper_lower_bound_percentile', 'scale_values',
                 'one_hot_embedding_threshold']:

            candidate_value: any = v.split('XXXXSEPXXXX') if 'XXXXSEPXXXX' in str(v) else coalesce(standard_dict.get(row.name) if (k == 'standardization_values') else None, json.loads(v) if ((k == 'standardization_values') and ('{' in str(v))) else v)

            if candidate_value:
                out[k] = candidate_value

    return out


def _load_config_dict(helper_instruct_fp: str) -> dict:
    temp = check_load_df(helper_instruct_fp, sheet_name='standardization_values', desired_types={'concept_id': 'sparse_int'})

    out: dict = {}

    for v in temp.column_name.unique():
        out_t: dict = {}

        for _, row in temp[temp.column_name == v].iterrows():
            out_t[row.concept_id] = row.standardized_value

        out[v] = out_t

    return out
