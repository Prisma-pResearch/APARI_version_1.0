# -*- coding: utf-8 -*-
"""
Created on Sun Jun 25 19:47:00 2023

@author: ruppert20
"""
import pandas as pd
from typing import List, Dict, Union
from .statistical_comparisons import summarize_groups
from ..FileHandling.variable_specification_utilities import load_variables_from_var_spec
from ..PreProcessing.standardization_functions_config_helper import _load_config_dict
from ..FileHandling.io import check_load_df, save_data
from ..PreProcessing.data_format_and_manipulation import coalesce


def make_tables_from_var_spec(instruction_fp: str,
                              var_file_link_fp: str,
                              source_dir: str,
                              generated_data_dir: str,
                              out_path: Union[str, None] = None,
                              subject_id_type: str = 'visit_detail_id',
                              id_col_priority: list = ['subject_id', 'visit_detail_id', 'visit_occurrence_id', 'person_id'],
                              groups: Dict[str, str] = {'Devleopment': 'Development|Test', 'Validation': 'Validation'}):

    assert (out_path is None) or ('.xlsx' == str(out_path[-5:])), 'Out paths must be in excel (.xlsx) format if one is provided'
    summary_instructions: pd.DataFrame = check_load_df(instruction_fp, sheet_name='Summary Table Instructions').dropna(subset=['table_name', 'table_field_name'], how='any')
    project_variable_df: pd.DataFrame = pd.read_excel(instruction_fp, sheet_name='Variables').dropna(subset=['Project']).rename(columns={'Project': 'project_name'})
    project_variable_df['effective_name'] = project_variable_df.apply(lambda row: coalesce(row.result_field_name, row.variable_name, row.cdm_field_name), axis=1)
    generated_data_link_df: pd.DataFrame = pd.read_excel(instruction_fp, sheet_name='Generated_Variables (Do Not Mod')
    source_data_link: pd.DataFrame = pd.read_excel(var_file_link_fp)
    source_data_link['effective_name'] = source_data_link.apply(lambda row: coalesce(row.result_field_name, row.variable_name, row.cdm_field_name), axis=1)
    standardization_dict: dict = _load_config_dict(instruction_fp)
    master_cohort_df: pd.DataFrame = check_load_df('', patterns=['*_master_cohort_definition.csv'], regex=False, directory=source_dir, usecols=id_col_priority + ['cohort'], dtype=str, use_col_intersection=True)

    group_dfs: Dict[str, pd.DataFrame] = {k: master_cohort_df[master_cohort_df.cohort.str.contains(v, regex=True, case=False, na=False)].copy(deep=True).reset_index(drop=True) for k, v in groups.items()}

    results: dict = {}

    for table in summary_instructions.table_name.unique():
        table_idx: pd.Series = (summary_instructions.table_name == table)
        table_results: dict = {}

        for section in summary_instructions.table_section_header[table_idx].unique():
            section_idx: pd.Series = (summary_instructions.table_section_header.isnull() if pd.isnull(section) else (summary_instructions.table_section_header == section)) & table_idx
            section_results: dict = {}

            for field_name in summary_instructions.table_field_name[section_idx].unique():
                field_idx: pd.Series = (summary_instructions.table_field_name == field_name) & section_idx

                variable: str = summary_instructions.variable_name[field_idx].iloc[0]

                assert project_variable_df.effective_name.isin([variable]).any() or generated_data_link_df.column_name[generated_data_link_df.project_name.isin(project_variable_df.project_name.unique())].isin([variable]).any(), f'The requested variable: {variable} could not be found'

                project_var_idx: pd.Series = source_data_link.effective_name.isin([variable])
                if project_var_idx.any():

                    data: pd.DataFrame = load_variables_from_var_spec(variables_columns=[variable],
                                                                      dir_dict=None,
                                                                      var_spec_key=var_file_link_fp,
                                                                      data_source_key=source_dir,
                                                                      id_vars=['subject_id'],
                                                                      project=source_data_link.project[project_var_idx].iloc[0],
                                                                      mute_duplicate_var_warnings=True,
                                                                      cdm_tables=None,
                                                                      coalesce_fields=None,
                                                                      allow_empty_returns=False,
                                                                      filter_variables=True,
                                                                      aggregation_function_filter=None,
                                                                      paritition_filter=None,
                                                                      index_type_filter=None)
                else:
                    data: pd.DataFrame = check_load_df('', patterns=[generated_data_link_df.source_file[generated_data_link_df.column_name == variable].iloc[0] + '*.csv'], regex=False,
                                                       usecols=id_col_priority + [variable],
                                                       use_col_intersection=True,
                                                       directory=generated_data_dir,
                                                       recursive=True)
                try:
                    link_cols: List[str] = [x for x in id_col_priority if x in data.columns]
                except AttributeError as e:
                    print(variable, field_name)
                    raise Exception(e)

                assert len(link_cols) > 0, f'Unable to link source data with cohort for variable: {variable}'

                link_col: str = link_cols[0]

                config_dict = summary_instructions.loc[field_idx, ['base_group', 'unit', 'dtype', 'iid', 'use_standardization_values', 'precision_number_of_levels']].dropna(axis=1).iloc[0, :].to_dict()

                if config_dict.pop('use_standardization_values', False):
                    config_dict['standardization_dict'] = standardization_dict.get(variable)

                max_levels: int = config_dict.pop('precision_number_of_levels', 5)

                section_results[field_name] = summarize_groups(input_v={k: v[[subject_id_type if link_col == 'subject_id' else link_col]]
                                                                        .merge(data.drop(columns=[x for x in id_col_priority if x != link_col], errors='ignore')
                                                                               .rename(columns={'subject_id': subject_id_type, variable: field_name}),
                                                                               how='inner', on=subject_id_type if link_col == 'subject_id' else link_col)
                                                                        .drop(columns=[subject_id_type, link_col], errors='ignore')for k, v in group_dfs.items()},
                                                               group_col=None,
                                                               config_dict={field_name: config_dict},
                                                               max_levels_to_display=max_levels).reset_index(drop=False).rename(columns={'index': 'field_name'})

            table_results[section] = section_results
        results[table] = table_results

    if isinstance(out_path, str):
        save_data(_compile_results(results), out_path=out_path)
    return _compile_results(results)


def _compile_results(input_dict: dict) -> dict:
    out_d: Dict[str, pd.DataFrame] = {}

    for tbn, tbv in input_dict.items():

        output_l: List[pd.DataFrame] = []

        for sn, sv in tbv.items():
            section_l: List[pd.DataFrame] = []
            for fn, fv in sv.items():
                section_l.append(fv)

            header: pd.DataFrame = fv.iloc[[0], :].copy(deep=True)
            header['field_name'] = sn

            header[[x for x in header.columns if x != 'field_name']] = None

            output_l.append(pd.concat([header] + section_l, axis=0, sort=False, ignore_index=True))

        out_d[tbn] = pd.concat(output_l, axis=0, sort=False, ignore_index=True).drop_duplicates(subset=['field_name'], keep='first')

    return out_d
