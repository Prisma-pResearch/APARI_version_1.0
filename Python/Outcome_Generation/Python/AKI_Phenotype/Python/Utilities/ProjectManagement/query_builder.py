# -*- coding: utf-8 -*-
"""
Created on Sun May 21 13:20:35 2023

@author: ruppert20
"""
import pandas as pd
import re
import os
from typing import List, Dict, Union
from datetime import datetime as dt
from ..PreProcessing.data_format_and_manipulation import deduplicate_and_join, coalesce
from ..FileHandling.io import save_data, check_load_df, find_files
from ..PreProcessing.standardization_functions import process_df_v2
from ..Database.connect_to_database import omop_engine_bundle


def validate_and_run_query_builder(spec_fp: str,
                                   engine_bundle: omop_engine_bundle,
                                   cohort_definition_id: int,
                                   dir_dict: Dict[str, str],
                                   mode: str = 'audit',
                                   additional_variables_to_generate: Union[List[str], None] = None,
                                   quick_audit_n: Union[int, None] = None,
                                   save_audit_queries: bool = False) -> list:
    """
    Validate Variable Specification and Run Query Builder.

    Parameters
    ----------
    spec_fp : str
        file path to the variable specification excel document.
    engine_bundle : omop_engine_bundle
        A SQLALCHEMY database engine bundled with OMOP database schemas and lookup tables.
    cohort_definition_id : int
        One of the cohort ids used in the project. It does not matter which one. It is only used to lookup the subject_id_type
    dir_dict : Dict[str, str]
        A dictionary of file paths. Must have the following keys:
            *SQL
            *audit_source
            *stat_files
    mode : str, optional
        Whether data should be audited (audit) or SQL Queries built (data_retrieval) or both (both). The default is 'audit'.
    quick_audit_n : Union[int, None], optional
        Audit the first n samples in each table matching the criterion. The default is None which will audit the entire table.
    save_audit_queries : bool, optional
        Whether the audit queries should be saved to the audit_source directory or not. The default is False, which will save the results of the audit queries, but not the queries themselves.
    additional_variables_to_generate: Union[List[str], None], optional
        List of ancillary projects to generate data for

    Returns
    -------
    List[str]
        List of SQL query file paths.

    """
    assert mode in ['audit', 'data_retrieval', 'both'], f"Unsupported mode: {mode}, please choose from ['audit', 'data_retrieval', 'both']"

    query_complete_fp: str = os.path.join(dir_dict.get('status_files'), 'SQL_builder_success__')
    audit_complete_fp: str = os.path.join(dir_dict.get('status_files'), 'SQL_audit_success__')

    if ((mode == 'both') and os.path.exists(query_complete_fp) and os.path.exists(audit_complete_fp)) or ((mode == 'data_retrieval') and os.path.exists(query_complete_fp)):
        return find_files(directory=dir_dict.get('SQL'),
                          patterns=[r'.*\.sql'],
                          exclusion_patterns=[r'eligibility_criteria\.sql'],
                          regex=True, agg_results=True, recursive=False)
    elif (mode == 'audit') and os.path.exists(audit_complete_fp):
        return []

    variable_df: pd.DataFrame = check_load_df(spec_fp, sheet_name='Variables')

    if isinstance(additional_variables_to_generate, list):
        gen_variable_df: pd.DataFrame = check_load_df(spec_fp, sheet_name='Ancillary_Projects (Do Not Mod)')

        for v in additional_variables_to_generate:
            assert gen_variable_df.project.isin([v]).any(), f'Unable to find the requested project: {v}. Please choose from the following: gen_variable_df.project_name.unique().tolist()'

        variable_df = pd.concat([variable_df, gen_variable_df[gen_variable_df.project.isin(additional_variables_to_generate)]], axis=0, ignore_index=True)

    variable_df.dropna(subset=['variable_name', 'cdm_field_name'], how='all', inplace=True)

    assert variable_df.project.notnull().all(), f'Variable Specification document has {variable_df.project.isnull()} rows missing project names'

    # TODO: add additional validation assertions

    return _build_queries(df=variable_df,
                          engine_bundle=engine_bundle,
                          cohort_definition_id=cohort_definition_id,
                          dir_dict=dir_dict,
                          query_complete_fp=query_complete_fp,
                          audit_complete_fp=audit_complete_fp,
                          mode=mode,
                          quick_audit_n=quick_audit_n,
                          save_audit_queries=save_audit_queries)


def _build_queries(df: pd.DataFrame,
                   engine_bundle: omop_engine_bundle,
                   cohort_definition_id: int,
                   dir_dict: Dict[str, str],
                   query_complete_fp: str,
                   audit_complete_fp: str,
                   mode: str = 'audit',
                   quick_audit_n: Union[int, None] = None,
                   save_audit_queries: bool = False) -> List[str]:
    """
    Build SQL Queries for downloading data and/or Audit Data based on a variable specificaton.

    Parameters
    ----------
    df : pd.DataFrame
        Pandas DataFrame containing the variable specification for the project.
    engine_bundle : omop_engine_bundle
        A SQLALCHEMY database engine bundled with OMOP database schemas and lookup tables.
    cohort_definition_id : int
        One of the cohort ids used in the project. It does not matter which one. It is only used to lookup the subject_id_type
    dir_dict : Dict[str, str]
        A dictionary of file paths. Must have the following keys:
            *SQL
            *audit_source
            *stat_files
    mode : str, optional
        Whether data should be audited (audit) or SQL Queries built (data_retrieval) or both (both). The default is 'audit'.
    quick_audit_n : Union[int, None], optional
        Audit the first n samples in each table matching the criterion. The default is None which will audit the entire table.
    save_audit_queries : bool, optional
        Whether the audit queries should be saved to the audit_source directory or not. The default is False, which will save the results of the audit queries, but not the queries themselves.
    query_complete_fp: Union[str, None], Optional
        Query Success file path
    audit_complete_fp: str, Optional
        Audit Success file path

    Returns
    -------
    List[str]
        List of SQL query file paths.

    """
    assert mode in ['audit', 'data_retrieval', 'both'], f"Unsupported mode: {mode}, please choose from ['audit', 'data_retrieval', 'both']"

    raw_df, computed_df = _format_metadata(df=check_load_df(df), engine_bundle=engine_bundle, cohort_definition_id=cohort_definition_id)

    if mode in ['audit', 'both']:
        if not os.path.exists(audit_complete_fp):
            raw_df.groupby('cdm_table', as_index=False).apply(retrieve_audit_data, engine_bundle=engine_bundle, dir_dict=dir_dict, quick_audit_n=quick_audit_n, save_query=save_audit_queries)

            analyze_audit_data(dir_dict=dir_dict, raw_df=raw_df.copy(deep=True), engine_bundle=engine_bundle)

            open(audit_complete_fp, 'a').close()

    if mode in ['data_retrieval', 'both']:

        if not os.path.exists(query_complete_fp):
            save_data(build_data_queries(df=raw_df, engine_bundle=engine_bundle, query_save_folder=dir_dict.get('SQL')),
                      out_path=os.path.join(dir_dict.get('intermediate_data'), 'variable_file_linkage.xlsx'))

            open(query_complete_fp, 'a').close()

        return find_files(directory=dir_dict.get('SQL'),
                          patterns=[r'.*\.sql'],
                          exclusion_patterns=[r'eligibility_criteria\.sql'],
                          regex=True, agg_results=True, recursive=False)

    return []


def get_subject_id(cohort_definition_id: int, engine_bundle: omop_engine_bundle) -> str:
    result: pd.DataFrame = pd.read_sql(f'''SELECT
                                            CASE
                                                WHEN subject_concept_id = 1147314 THEN 'person_id'
                                                WHEN subject_concept_id = 1147332 THEN 'visit_occurrence_id'
                                                WHEN subject_concept_id = 1147637 THEN 'visit_detail_id'
                                                WHEN subject_concept_id = 1147301 THEN 'procedure_occurrence_id'
                                                WHEN subject_concept_id = 1147333 THEN 'condition_occurrence_id'
                                                WHEN subject_concept_id = 1147339 THEN 'drug_exposure_id'
                                                WHEN subject_concept_id = 1147305 THEN 'device_exposure_id'
                                                ELSE 'error' END [subject_id]
                                        FROM
                                            {engine_bundle.results_schema}.COHORT_DEFINITION CD
                                        WHERE
                                            cohort_definition_id = {cohort_definition_id}''', con=engine_bundle.engine)

    assert result.shape[0] == 1, f'The cohort_definition_id: {cohort_definition_id} was not found'

    subject_id_type: str = result.iloc[0, 0]

    assert subject_id_type != 'error', 'Invalid Subject Id Type. The subject_concept_id for the cohort must be either Person, Visit_Occurrence, Visit_Detail, Procedure_Occurrence, Condition_Occurrence, Device_Exposure, or Drug_Exposure'

    return subject_id_type


def get_table_and_concept_class(df: pd.DataFrame, engine_bundle: omop_engine_bundle) -> pd.DataFrame:

    df.to_sql(name='tmp_table_concept_class_lookup', schema=engine_bundle.operational_schema, if_exists='replace', con=engine_bundle.engine, index=True)

    dl_joins: str = '\n'.join([f'LEFT JOIN {engine_bundle.lookup_schema}.{engine_bundle.drug_lookup_table} DL{i} on S.variable_name = DL{i}.{col}' for i, col in enumerate(df.drug_lookup_col.dropna().unique().tolist())])
    dl_classes: str = ','.join([f'DL{i}.drug_concept_class' for i in range(df.drug_lookup_col.dropna().nunique())])

    result: pd.DataFrame = pd.read_sql(f'''SELECT
                                                [index] [join_idx],
                                                STRING_AGG(concept_class_id, ',') [concept_class_id],
                                                LOWER([cdm_table]) [cdm_table]
                                            FROM (
                                                    SELECT DISTINCT
                                                        [index],
                                                        COALESCE(L.concept_class_id, {dl_classes if len(dl_classes) > 0 else 'NULL'}) [concept_class_id],
                                                        COALESCE(CONVERT(VARCHAR, S.[cdm_table]),
                                                                         CASE WHEN L.domain_id IN ('Procedure', 'Condition') THEN CONCAT(L.domain_id, '_Occurrence')
                                                                      WHEN L.domain_id IN ('Drug', 'Device') OR (COALESCE({dl_classes if len(dl_classes) > 0 else 'NULL'}, NULL) IS NOT NULL) THEN CONCAT (COALESCE(L.domain_id, 'drug'), '_exposure') ELSE L.Domain_id END,
                                                                         IFC.TABLE_NAME) [cdm_table]
                                                    FROM
                                                        {engine_bundle.operational_schema}.tmp_table_concept_class_lookup S
                                                        LEFT JOIN {engine_bundle.lookup_schema}.{engine_bundle.lookup_table} L on S.variable_name = L.variable_name
                                                        {dl_joins}
                                                        INNER JOIN {engine_bundle.database}.INFORMATION_SCHEMA.COLUMNS IFC on (IFC.COLUMN_NAME = S.cdm_field_name
                                                                                                                              AND IFC.TABLE_SCHEMA = 'CDM'
                                                                                                                              AND (COALESCE(CONVERT(VARCHAR, S.[cdm_table]), CASE WHEN L.domain_id IN ('Procedure', 'Condition') THEN CONCAT(L.domain_id, '_Occurrence')
                                                                      WHEN L.domain_id IN ('Drug', 'Device') THEN CONCAT (L.domain_id, '_exposure') ELSE L.Domain_id END) = IFC.TABLE_NAME OR L.domain_id IS NULL))
                                                        ) F
                                                    GROUP BY
                                                        [index], [cdm_table];''',
                                       con=engine_bundle.engine)

    assert result.cdm_table.notnull().all(), 'There is one or more field/variable pairs without a table. Please check the field list against the CDM specification'

    return df.drop(columns=['cdm_table', 'concept_class_id'], errors='ignore').merge(result, left_index=True, right_on=['join_idx'], how='left')


table_abbrev_dict: Dict[str, str] = {'visit_detail': 'vd',
                                     'visit_occurrence': 'vo',
                                     'procedure_occurrence': 'po',
                                     'drug_exposure': 'drugE',
                                     'device_exposure': 'devE',
                                     'measurement': 'm',
                                     'observation': 'o',
                                     'condition_era': 'ce',
                                     'provider': 'prov',
                                     'location': 'loc',
                                     'death': 'death',
                                     'person': 'p',
                                     'payer_plan_period': 'ppp',
                                     'drug_era': 'drugERA',
                                     'dose_era': 'doseERA',
                                     'care_site': 'cs',
                                     'condition_occurrence': 'co',
                                     'cost': 'cost',
                                     'location_history': 'lh',
                                     'cohort': 'C'}
table_time_index_dict: Dict[str, str] = {'measurement': 'measurement_datetime',
                                         'observation': 'observation_datetime',
                                         'death': 'death_date',
                                         'person': 'birth_datetime'}
table_start_time_index_dict: Dict[str, str] = {**{x: f'{x.replace("_occurrence", "")}_start_datetime'.replace('procedure_start_datetime', 'procedure_datetime') for x in ['visit_occurrence', 'visit_detail', 'procedure_occurrence', 'drug_exposure', 'device_exposure']},
                                               **{x: f'{x.replace("_occurrence", "")}_start_date'.replace("location_history_", "") for x in ['condition_era', 'drug_era', 'dose_era', 'payer_plan_period', 'condition_occurrence', 'location_history']}}
table_end_time_index_dict: Dict[str, str] = {**{x: f'{x.replace("_occurrence", "")}_end_datetime' for x in ['visit_occurrence', 'visit_detail', 'procedure_occurrence', 'drug_exposure', 'device_exposure']},
                                             **{x: f'{x.replace("_occurrence", "")}_end_date'.replace("location_history_", "") for x in ['condition_era', 'drug_era', 'dose_era', 'payer_plan_period', 'condition_occurrence', 'location_history']}}

table_start_time_index_dict2: Dict[str, str] = {**{x: f'{x.replace("_occurrence", "")}_start_date'.replace('procedure_start_date', 'procedure_date') for x in ['visit_occurrence', 'visit_detail', 'procedure_occurrence', 'drug_exposure', 'device_exposure']},
                                                **{x: f'{x.replace("_occurrence", "")}_start_datetime' for x in ['condition_occurrence']}}
table_end_time_index_dict2: Dict[str, str] = {**{x: f'{x.replace("_occurrence", "")}_end_date' for x in ['visit_occurrence', 'visit_detail', 'procedure_occurrence', 'drug_exposure', 'device_exposure']},
                                              **{x: f'{x.replace("_occurrence", "")}_end_datetime' for x in ['condition_occurrence']}}


def _format_metadata(df: pd.DataFrame, engine_bundle: omop_engine_bundle, cohort_definition_id: int) -> pd.DataFrame:

    df = get_table_and_concept_class(df=df, engine_bundle=engine_bundle)

    df['row_id'] = pd.Series(range(df.shape[0])).astype(str).values

    generated_var_idx: pd.Series = df.computation_source.notnull()

    compuatation_fields: pd.DataFrame = df[generated_var_idx].copy(deep=True)

    df = df[~generated_var_idx].copy(deep=True)

    df['subject_id'] = get_subject_id(cohort_definition_id=cohort_definition_id, engine_bundle=engine_bundle)
    df['subject_id_source_table'] = df.subject_id.str.replace(r'_id$', '', regex=True)
    df['subject_id_source_table_abbrev'] = df.subject_id_source_table.apply(table_abbrev_dict.get)
    df['cdm_table_abbrev'] = df.cdm_table.str.lower().apply(table_abbrev_dict.get)
    df['use_subset'] = True
    df['index_type_table_abbrev'] = df.index_type.str.lower().apply(table_abbrev_dict.get)
    try:
        df['partition_seq'] = pd.to_numeric(df['partition_seq'], errors='raise')
    except:
        raise Exception('Non-numeric values found in column: partition_seq. Please resolve before continuing')

    # format columns
    df['location_history_available'] = pd.read_sql(f'''SELECT COLUMN_NAME FROM {engine_bundle.database}.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'LOCATION_HISTORY' AND TABLE_SCHEMA = '{engine_bundle.data_schema.split(".")[-1]}';''', con=engine_bundle.engine).shape[0] > 0
    df['field_name'] = df.apply(_format_selection, axis=1)

    variable_field_idx: pd.Series = df.concept_class_id.isnull()
    if variable_field_idx.any():
        df.loc[variable_field_idx, 'variable_name'] = None

    df.lookup_join_type.fillna('INNER', inplace=True)

    missing_start_idx: pd.Series = df.start_reference_point.isnull()
    if missing_start_idx.any():
        df.loc[missing_start_idx, 'start_reference_point'] = df.loc[missing_start_idx, 'index_type'].apply(lambda x: table_start_time_index_dict.get(x, 'unbounded'))

    missing_end_idx: pd.Series = df.end_reference_point.isnull()
    if missing_end_idx.any():
        df.loc[missing_end_idx, 'end_reference_point'] = df.loc[missing_end_idx, 'index_type'].apply(lambda x: table_end_time_index_dict.get(x, 'unbounded'))

    return df, compuatation_fields


def _solve_filter(row: pd.Series, engine_bundle: omop_engine_bundle) -> pd.Series:

    # determine time index for joined table
    tbl_time_index: str = table_time_index_dict.get(row.cdm_table, table_start_time_index_dict.get(row.cdm_table))

    # determine start column
    start_col: str = row.start_reference_point if isinstance(row.start_reference_point, str) else table_start_time_index_dict.get(row.index_type, 'unbounded')

    # switch datetime field to date field if cdm_table_time_index is a date
    if bool(re.search(r'date$', start_col)) and not bool(re.search(r'date$', tbl_time_index)):
        start_col_str: str = start_col.replace('datetime', 'date')
    else:
        start_col_str: str = start_col

    # cdm field for start key
    if row.index_type != 'person':

        # extract hours/days and set filter statement
        if (start_col != 'unbounded') and isinstance(row.start_reference_point_time_delta, str):
            try:
                start_time_delta: str = re.search(r'([0-9]+)\s(day|hour)', row.start_reference_point_time_delta, re.IGNORECASE).groups(0)
            except AttributeError:
                raise Exception(f'Unable to parse timedelta: {row.start_reference_point_time_delta} from the field {start_col} for the row corresponding to the field/variable pair {row.fields}: {row.variables}')

            lower_filter: str = f"DATEADD({start_time_delta[1]}, {'-' if '-' in row.start_reference_point_time_delta else ''}{start_time_delta[0]}, {start_col_str})"
        elif pd.isnull(row.start_reference_point_time_delta) and (start_col != 'unbounded'):
            lower_filter: str = start_col
            start_time_delta: str = None
        else:
            start_time_delta: str = start_col
    else:
        start_time_delta: str = start_col

    end_col = row.end_reference_point if isinstance(row.end_reference_point, str) else table_end_time_index_dict.get(row.index_type, 'unbounded')

    # switch datetime field to date field if cdm_table_time_index is a date
    if bool(re.search(r'date$', end_col)) and not bool(re.search(r'date$', tbl_time_index)):
        end_col_str: str = end_col.replace('datetime', 'date')
    else:
        end_col_str: str = end_col

    # cdm field for start key
    if row.index_type != 'person':

        # extract hours/days and set filter statement
        if (end_col != 'unbounded') and isinstance(row.end_reference_point_time_delta, str):
            try:
                end_time_delta: str = re.search(r'([0-9]+)\s(day|hour)', row.end_reference_point_time_delta, re.IGNORECASE).groups(0)
            except AttributeError:
                raise Exception(f'Unable to parse timedelta: {row.end_reference_point_time_delta} from the field {end_col} for the row corresponding to the field/variable pair {row.fields}: {row.variables}')

            upper_filter: str = f"DATEADD({end_time_delta[1]}, {'-' if '-' in row.end_reference_point_time_delta else ''}{end_time_delta[0]}, {end_col_str})"
        elif pd.isnull(row.end_reference_point_time_delta) and (end_col != 'unbounded'):
            upper_filter: str = end_col
            end_time_delta: str = None
        else:
            end_time_delta: str = end_col
    else:
        end_time_delta: str = end_col

    # establish list of final filters
    filters: List[str] = []

    if row.exclude_0_concepts == 'Y':
        filters += [f'{row.cdm_table_abbrev}.' + re.search(row.cdm_table_abbrev + r'.([A-z_]+)', x).groups(0)[0] + ' <> 0' for x in row.fields.split('||') if 'concept_id' in x]

    # establish id linkage
    # if ((pd.isnull(start_time_delta) and pd.isnull(end_time_delta))
    #     or (('-' not in str(row.start_reference_point_time_delta)) and ('-' not in str(row.end_reference_point_time_delta)) and (row.index_type != 'person'))):
    if (pd.isnull(start_time_delta) and pd.isnull(end_time_delta)) and (row.index_type != 'person') and (row.cdm_table not in ['condition_occurrence']) and ((end_col in [table_end_time_index_dict.get(row.index_type), table_end_time_index_dict2.get(row.index_type)]) and (start_col in [table_start_time_index_dict.get(row.index_type), table_start_time_index_dict2.get(row.index_type)])):
        # if (row.table != row.index_type) and (row.table not in ['payer_plan_period', 'cost']):
        # filters.append(f'{row.table_abbrev}.{row.index_type}_id = {row.subject_id_source_table_abbrev}.{row.index_type}_id')
        index_bounded: bool = True
    else:
        # filters.append(f'{row.table_abbrev}.person_id = {row.subject_id_source_table_abbrev}.person_id')
        index_bounded: bool = False

    # add temporal component
    if (((start_col == 'unbounded') and (end_col == 'unbounded')) or (row.cdm_table == 'person') or ((end_col in [table_end_time_index_dict.get(row.index_type), table_end_time_index_dict2.get(row.index_type)]) and (start_col in [table_start_time_index_dict.get(row.index_type), table_start_time_index_dict2.get(row.index_type)]))):
        pass   # No additional filters needed, already filtered to index

    elif (start_col in ['unbounded']):
        filters.append(f'{row.cdm_table_abbrev}.{tbl_time_index} <= {upper_filter}')

    elif (end_col in ['unbounded']):
        filters.append(f'{row.cdm_table_abbrev}.{tbl_time_index} >= {lower_filter}')

    else:
        filters.append(f'{row.cdm_table_abbrev}.{tbl_time_index} BETWEEN {lower_filter} AND {upper_filter}')

    # add variable filter
    if pd.notnull(row.concept_class_id):
        if isinstance(row.drug_lookup_col, str):
            filters.append(f'{row.drug_lookup_col} IN ({row.variables})')
        else:
            filters.append(f'variable_name IN ({row.variables})')

    if isinstance(row.additional_where_filter, str):
        filters.append(f'({row.additional_where_filter})')

    if isinstance(row.drug_route_filter, str):
        criteria: str = "'" + "', '".join([x.strip() for x in row.drug_route_filter.split(',')]) + "'"

        filters.append(f'{table_abbrev_dict.get(row.cdm_table)}.route_concept_id IN (SELECT DISTINCT concept_id FROM {engine_bundle.lookup_schema}.{engine_bundle.lookup_table} WHERE variable_name IN ({criteria}))')

    return pd.Series({'filters': (filters[0]) if len(filters) == 1 else ('(\n\t' + '\n\tAND\n\t'.join(filters) + '\n\t)') if len(filters) > 1 else None,
                      'index_bound': index_bounded,
                      'start_col': start_col,
                     'end_col': end_col})


def retrieve_audit_data(dfg: pd.DataFrame, engine_bundle: omop_engine_bundle, dir_dict: dict, quick_audit_n: Union[int, None] = None, save_query: bool = False):

    table: str = dfg.cdm_table.iloc[0]

    sel_line: str = f'SELECT TOP {quick_audit_n}' if isinstance(quick_audit_n, int) else 'SELECT'

    t = dfg.drop_duplicates(subset=['cdm_field_name', 'variable_name']).query(f'~cdm_field_name.isin({(list(table_time_index_dict.values()) + list(table_start_time_index_dict.values()) + list(table_end_time_index_dict.values()))})', engine='python')
    if dfg.cdm_table.isin(['procedure_occurrence', 'drug_exposure', 'device_exposure', 'measurement', 'observation', 'condition_era', 'drug_era', 'dose_era', 'condition_occurrence']).all():
        for _, row in dfg.query('~cdm_field_name.isin(["unit_concept_id"])', engine='python').iterrows():
            variable: str = coalesce(row.result_field_name, row.variable_name, row.cdm_field_name)
            fn: str = f'{row.cdm_table}__{row.cdm_field_name}__{variable}'
            success_path: str = os.path.join(dir_dict.get('stat_files'), f'{fn}__success')
            lookup_left_jn: str = f"LEFT JOIN {engine_bundle.lookup_schema}.{engine_bundle.lookup_table} Lc on Lc.concept_id = {row.cdm_table_abbrev}.value_as_concept_id" if row.field_name == 'value_as_concept_id' else ''

            lookup_join_col: str = 'modifier_concept_id' if row.variable_name == 'primary_procedure' else f'{row.cdm_table_abbrev}.{row.cdm_table.replace("_occurrence", "").replace("_exposure", "")}_concept_id'

            if not os.path.exists(success_path):
                qry: str = f'''{sel_line}
                                                person_id,
                                                visit_occurrence_id,
                                                {row.cdm_field_name} [{variable}]
                                                {',unit_concept_id' if row.cdm_field_name == 'value_as_number' else ',lc.concept_id [{variable}_variable_name]' if lookup_left_jn != '' else ''}
                                            FROM
                                                {engine_bundle.data_schema}.{row.cdm_table} {row.cdm_table_abbrev}
                                            INNER JOIN {engine_bundle.lookup_schema}.{engine_bundle.lookup_table} L on (L.concept_id = {lookup_join_col} AND L.variable_name = '{row.variable_name}')
                                            {lookup_left_jn}'''
                if save_query:
                    save_data(qry, os.path.join(dir_dict.get('audit_source'), f'{fn}.sql'))
                save_data(check_load_df(qry,
                                        engine=engine_bundle.engine,
                                        chunksize=1000),
                          out_path=os.path.join(dir_dict.get('audit_source'), f'{fn}.csv'))

                open(success_path, mode='a').close()
    elif table in ['provider']:
        raw_fields: List[str] = [x for x in t.cdm_field_name.drop_duplicates().tolist() if 'date' not in x]

        fn: str = f'{table}'
        success_path: str = os.path.join(dir_dict.get('stat_files'), f'{fn}__success')

        if not os.path.exists(success_path):
            qry: str = f'''{sel_line}
                                            provider_id,
                                            {','.join(raw_fields)}
                                        FROM
                                            {engine_bundle.data_schema}.{table} {table_abbrev_dict.get(table)}'''
            if save_query:
                save_data(qry, os.path.join(dir_dict.get('audit_source'), f'{fn}.sql'))
            save_data(check_load_df(qry,
                                    engine=engine_bundle.engine,
                                    chunksize=1000),
                      out_path=os.path.join(dir_dict.get('audit_source'), f'{fn}.csv'))
            open(success_path, mode='a').close()

    else:
        raw_fields: List[str] = [x for x in t.cdm_field_name.drop_duplicates().tolist() if 'date' not in x]
        concept_id_fields: List[str] = [x for x in raw_fields if 'concept_id' in x]
        lookup_joins: str = '\n'.join([f'LEFT JOIN {engine_bundle.lookup_schema}.{engine_bundle.lookup_table} L_{x} on L_{x}.concept_id = {table_abbrev_dict.get(table)}.{x}' for x in concept_id_fields])

        fn: str = f'{table}'
        success_path: str = os.path.join(dir_dict.get('stat_files'), f'{fn}__success')

        if not os.path.exists(success_path):
            qry: str = f'''{sel_line}
                                            {'person_id,' if table not in ['location', 'care_site', 'cost'] else ''}
                                            {'visit_occurrence_id,' if table not in ['payer_plan_period', 'location', 'care_site', 'cost', 'death', 'person'] else ''}
                                            {','.join(raw_fields)}{',' if len(concept_id_fields) > 0 else ''}
                                            {','.join([('L_' + x + '.concept_id [' + x + '_variable_name]') for x in concept_id_fields])}
                                        FROM
                                            {engine_bundle.data_schema}.{table} {table_abbrev_dict.get(table)}
                                        {lookup_joins}'''

            if save_query:
                save_data(qry, os.path.join(dir_dict.get('audit_source'), f'{fn}.sql'))

            save_data(check_load_df(qry,
                                    engine=engine_bundle.engine,
                                    chunksize=1000),
                      out_path=os.path.join(dir_dict.get('audit_source'), f'{fn}.csv'))
            open(success_path, mode='a').close()


def analyze_audit_data(dir_dict: dict, raw_df: pd.DataFrame, engine_bundle: omop_engine_bundle):

    report_success_fp: str = os.path.join(dir_dict.get('stat_files'), 'variable_audit_success__')

    if not os.path.exists(report_success_fp):

        raw_df['audit_var'] = raw_df.apply(lambda row: coalesce(row.result_field_name, row.variable_name, row.cdm_field_name), axis=1)
        # audit values
        for f in find_files(directory=dir_dict.get('audit_source'),
                            patterns=[r'.*\.csv'],
                            exclusion_patterns=[r'_person_visit_counts\.csv'],  # filter out the person counts from this phase of the audit
                            regex=True, agg_results=True, recursive=False):

            success_fp: str = f.replace('.csv', '_audit_complete_')

            if not os.path.exists(success_fp):
                fn: str = os.path.basename(f)

                fnc: List[str] = fn.replace('.csv', '').split('__')

                if len(fnc) == 3:
                    table: str = fnc[0]
                    cdm_field: str = fnc[1]
                    variable: str = fnc[2]
                    filter_str: str = f'(cdm_table == "{table}") & (cdm_field_name == "{cdm_field}") & (audit_var == "{variable}")'
                else:
                    table: str = fnc[0]
                    cdm_field: str = None
                    variable: str = None

                print(f'Auditing: {fn}')

                # read file
                tdf: pd.DataFrame = pd.read_csv(f)

                # extract counts
                counts: dict = {'person_count': [tdf.person_id.nunique()]} if 'person_id' in tdf.columns else {}
                if 'visit_occurrence_id' in tdf.columns:
                    counts['visit_count'] = [tdf.visit_occurrence_id.nunique()]

                if len(counts) > 0:
                    save_data(pd.DataFrame(counts),
                              out_path=f.replace('.csv', '_person_visit_counts.csv'))

                tdf.drop(columns=['person_id', 'visit_occurrence_id'], errors='ignore', inplace=True)

                # first lookup by table name
                t_spec_df: pd.DataFrame = raw_df.query(filter_str)

                assert t_spec_df.shape[0] > 0, f'Unable to find {fn} in the audit specification. Please revise the audit specification and try again.'

                # for source_col in t_spec_df.source_column.unique():

                process_df_v2(df=tdf,
                              instruction_fp=f.replace('.csv', '_audit_results.xlsx'),
                              training_run=True,
                              master_config_dict={x: {'output_dtype': 'cat_embedding'} for x in ([y for y in tdf.columns if 'concept_id' in y] + ([variable] if 'concept_id' in str(cdm_field) else []))},
                              default_lower_limit_percentile=None,
                              default_scale_values=False,
                              default_min_num_cat_levels=1,
                              encoder_dir=dir_dict.get('audit_source'),
                              default_upper_limit_percentile=None,
                              # stacked_meas_name='variable_name',
                              # stacked_meas_value=source_col,
                              default_ensure_col=True,
                              default_na_values=None,
                              default_dtype=None)

                open(success_fp, 'a').close()

        # cleanup categorical encoder files
        for f in find_files(directory=dir_dict.get('audit_source'),
                            patterns=[r'.*\.bin'],
                            exclusion_patterns=[],
                            regex=True, agg_results=True, recursive=False):
            os.remove(f)

        report = check_load_df('', directory=dir_dict.get('audit_source'), patterns=['*.xlsx'], regex=False, tag_source=True)\
            .query('output_dtype != "parameter"')

        report['cdm_table'] = report.source_file.apply(lambda x: os.path.basename(x).replace('_audit_results.xlsx', '').split('__')[0])

        report.rename(columns={'column_name': 'audit_var', 'output_dtype': 'data_type',
                               'fill_lower_upper_bound_value': '15th_percentile',
                               'fill_upper_lower_bound_value': '85th_percentile'}, inplace=True)

        report_pt1 = report.merge(raw_df[['audit_var', 'cdm_table', 'cdm_field_name', 'variable_name']].drop_duplicates(),
                                  on=['audit_var', 'cdm_table'],
                                  how='left')

        # cleanup manually added units that were not in the query table, only for audit purposes
        unit_idx: pd.Series = (report_pt1.audit_var == 'unit_concept_id') & report_pt1.cdm_field_name.isnull()

        if unit_idx.any():
            report_pt1.loc[unit_idx, 'cdm_field_name'] = report_pt1.loc[unit_idx, 'audit_var'].values
            report_pt1.loc[unit_idx, 'variable_name'] = report_pt1.loc[unit_idx, 'source_file'].apply(lambda x: os.path.basename(x).replace('_audit_results.xlsx', '').split('__')[-1])
            report_pt1.loc[unit_idx, 'audit_var'] = (report_pt1.loc[unit_idx, 'variable_name'] + '_unit_concept_id').values

        # cleanup manually added variable names for concept ids
        var_name_idx: pd.Series = report_pt1.audit_var.str.contains(r'_variable_name$', regex=True) & report_pt1.cdm_field_name.isnull()

        if var_name_idx.any():
            report_pt1.loc[var_name_idx, 'cdm_field_name'] = report_pt1.loc[var_name_idx, 'audit_var'].str.replace(r'_variable_name$', '', regex=True).values
            report_pt1.loc[var_name_idx, 'variable_name'] = report_pt1.loc[var_name_idx, 'cdm_field_name'].values

        # cleanup remaining fields without special variable names
        cdm_field_missing_idx: pd.Series = report_pt1.cdm_field_name.isnull()

        if cdm_field_missing_idx.any():
            report_pt1.loc[cdm_field_missing_idx, 'cdm_field_name'] = report_pt1.loc[cdm_field_missing_idx, 'audit_var'].values

        report_pt1 = report_pt1[['cdm_table', 'cdm_field_name', 'variable_name', 'data_type', 'overall_missingness', 'mean', 'median',
                                 'std', 'mad', 'max', 'min', 'mode', 'value_counts', '15th_percentile', '85th_percentile', 'source_file']].drop_duplicates()

        # load results of person audit
        people = check_load_df('', directory=dir_dict.get('audit_source'), patterns=['*_person_visit_counts.csv'], regex=False, tag_source=True)

        # format for merge
        people['source_file'] = people.source_file.str.replace('_person_visit_counts.csv', '_audit_results.xlsx', regex=False)

        # build final report template
        final_report = report_pt1\
            .merge(people, on=['source_file'], how='left')

        final_report.person_count.fillna(0, inplace=True)
        final_report.visit_count.fillna(0, inplace=True)

        final_report.insert(loc=5, column='number_of_observations', value=final_report.overall_missingness.str.extract(r'\/([0-9]+)\s').fillna(0).astype(int).iloc[:, 0].apply(lambda x: f'{x:,}').values)
        final_report.drop(columns=['overall_missingness'], inplace=True)

        total_persons = pd.read_sql(f'SELECT COUNT(person_id) FROM {engine_bundle.data_schema}.PERSON', con=engine_bundle.engine).iloc[0, 0]

        total_visits = pd.read_sql(f'SELECT COUNT(visit_occurrence_id) FROM {engine_bundle.data_schema}.VISIT_OCCURRENCE', con=engine_bundle.engine).iloc[0, 0]

        final_report.insert(loc=5, column='percent_persons_observed', value=(final_report.person_count.astype(int) / total_persons).apply(lambda x: "{:.2}".format(x)))
        final_report.insert(loc=5, column='percent_visits_observed', value=(final_report.visit_count.astype(int) / total_visits).apply(lambda x: "{:.2}".format(x)))

        final_report['person_count'] = final_report['person_count'].apply(lambda x: f'{int(x):,}')
        final_report['visit_count'] = final_report['visit_count'].apply(lambda x: f'{int(x):,}')

        save_data(final_report.drop_duplicates(), out_path=os.path.join(dir_dict.get('Audit'), 'final_report.xlsx'))

        open(report_success_fp, 'a').close()


def build_data_queries(df: pd.DataFrame, engine_bundle: omop_engine_bundle, query_save_folder: str = None) -> pd.DataFrame:

    template: pd.DataFrame = df[['field_name',
                                 'variable_name',
                                 'convert',
                                 'index_type',
                                 'start_reference_point',
                                 'start_reference_point_time_delta',
                                 'end_reference_point',
                                 'end_reference_point_time_delta',
                                 'aggregation_function',
                                 'exclude_0_concepts',
                                 'lookup_join_type',
                                 # 'stack_variables',
                                 # 'manual_join_filter',
                                 # 'manual_where_filter',
                                 # 'first_before',
                                 # 'first_after',
                                 'partition_seq',
                                 'partition_visit_detail_filter',
                                 # 'primary_merge_key',
                                 'concept_class_id',
                                 'cdm_table',
                                 'subject_id',
                                 'subject_id_source_table',
                                 'subject_id_source_table_abbrev',
                                 'cdm_table_abbrev',
                                 'use_subset',
                                 'drug_lookup_col',
                                 'additional_join_filter',
                                 'additional_where_filter',
                                 'drug_route_filter',
                                 'row_id']].copy(deep=True)

    # flag lab tests so they are not mixed with other measurements
    template['lab_test'] = template.concept_class_id.str.contains('lab test', regex=False, na=False, case=False) & ~template.variable_name.str.contains(r'spo2|rbc_transfusion', regex=True, na=False, case=False)

    grpby_cols: List[str] = ['cdm_table', 'index_type',
                             'aggregation_function',
                             'start_reference_point',
                             'start_reference_point_time_delta',
                             'end_reference_point',
                             'end_reference_point_time_delta',
                             'partition_visit_detail_filter',
                             'lookup_join_type',
                             'drug_route_filter',
                             'lab_test',
                             'drug_lookup_col',
                             'additional_join_filter',
                             'additional_where_filter',
                             'partition_seq', 'exclude_0_concepts']

    # fill nulls with -999 to prevent groupby errors
    template[grpby_cols] = template[grpby_cols].fillna('-999')

    # conense fields and variables in common tables
    template = template.groupby(grpby_cols).agg({**{'variable_name': _join_vars,
                                                 'field_name': _join_fields,
                                                    'row_id': _join_row_ids},
                                                 **{x: 'first' for x in ['concept_class_id', 'subject_id', 'subject_id_source_table', 'subject_id_source_table_abbrev',
                                                                         'cdm_table_abbrev', 'use_subset']}})\
        .reset_index(drop=False)\
        .replace({'-999': None})\
        .rename(columns={'field_name': 'fields', 'variable_name': 'variables', 'row_id': 'row_ids'})

    # solve the where filters for each group
    template[['filters', 'index_bound', 'start_col', 'end_col']] = template.apply(_solve_filter, engine_bundle=engine_bundle, axis=1)

    grpby2_cols: List[str] = ['cdm_table', 'partition_seq', 'lab_test', 'aggregation_function', 'index_bound', 'index_type',
                              'start_col', 'end_col', 'start_reference_point_time_delta', 'end_reference_point_time_delta',
                              'partition_visit_detail_filter', 'lookup_join_type', 'drug_lookup_col', 'additional_join_filter',]

    # fill nulls with -999 to prevent groupby errors
    template[grpby2_cols] = template[grpby2_cols].fillna('-999')

    # build the queries
    queries = template.groupby(grpby2_cols, as_index=False).apply(build_query_group, engine_bundle=engine_bundle).replace({'-999': None}).rename(columns={None: 'query'})

    # determine file name based on settings
    queries['file_name'] = queries.apply(_solve_file_name, axis=1)

    queries['row_ids'] = queries.row_ids.apply(lambda x: x.split('||'))

    # # stack queries based
    # test = queries.groupby('file_name').agg({'query':_join_query}).reset_index(drop=False)

    if isinstance(query_save_folder, str):
        assert os.path.exists(query_save_folder), f'Unable to locate query_save_folder: {query_save_folder}'

        for _, row in queries.iterrows():
            _test_writer(row.query, out_file_path=os.path.join(query_save_folder, f'{row.file_name}.sql'))

    return df.merge(queries.explode('row_ids')[['row_ids', 'file_name']].rename(columns={'row_ids': 'row_id'}), how='left', on='row_id')


def _join_row_ids(s: pd.Series) -> str:
    rows: List[str] = []

    for r in s:
        rows += str(r).split('||')

    return '||'.join(rows)


def _solve_file_name(row: pd.Series) -> str:
    if row.lab_test:
        fn: str = 'labs'
    else:
        fn: str = row.cdm_table

    if row.index_type != 'person':
        if row.index_bound:
            fn: str = f'{fn}_from_{row.index_type}'
        else:
            lower_bound: str = f'before_{row.index_type}' if row.start_col == 'unbounded' else f'{row.start_col}' if pd.isnull(row.start_reference_point_time_delta) else f'{row.start_reference_point_time_delta.lower().replace(" ", "_")}_{"before" if "-" in row.start_reference_point_time_delta else "after"}_{row.start_col}'
            upper_bound: str = f'after_{row.index_type}' if row.end_col == 'unbounded' else f'{row.end_col}' if pd.isnull(row.end_reference_point_time_delta) else f'{row.end_reference_point_time_delta.lower().replace(" ", "_")}_{"before" if "-" in row.end_reference_point_time_delta else "after"}_{row.end_col}'

            fn: str = f'{fn}_from_{lower_bound}_to_{upper_bound}'

    if pd.notnull(row.partition_seq):
        fn: str = f'{int(abs(row.partition_seq))}_{"most_recent_" if row.partition_seq < 0 else ""}{fn}'
    elif isinstance(row.aggregation_function, str):
        fn: str = f'{row.aggregation_function}_{fn}'

    if isinstance(row.drug_lookup_col, str):
        fn: str = f'{row.drug_lookup_col}_drug_search_{fn}'

    if isinstance(row.additional_join_filter, str):
        fn: str = f'{fn}_additional_join_filters_{_sanatize_file_name(row.additional_join_filter)}'

    return f'{fn}_{row.lookup_join_type}_lookup_join'


def _sanatize_file_name(s: str) -> str:
    return s.replace(' ', '_').replace('*', 'asterisk').replace('<', 'less_than').replace('=', 'equal_to').replace('>', 'greater_than').replace('/', '').replace('\\', '').replace('?', 'question_mark').replace('"', 'quotes')


def _test_writer(content, out_file_path: str):
    with open(out_file_path, 'w') as f:
        f.write(content)


def _join_vars(row: pd.Series) -> str:
    return deduplicate_and_join(input_s=row,
                                sep="', '",
                                leading="'",
                                trailing="'")


def _join_fields(row: pd.Series) -> str:
    return deduplicate_and_join(input_s=row,
                                sep='||',
                                leading="",
                                trailing="")


def _join_query(s: pd.Series) -> str:
    return '\nXXXSPLITXXX\n'.join(s)


def build_query_group(dfg: pd.DataFrame, engine_bundle: omop_engine_bundle) -> str:

    dfg.replace({'-999': None}, inplace=True)

    if dfg.shape[0] == 1:
        try:
            return build_query(dfg.iloc[0, :], engine_bundle=engine_bundle)
        except Exception as e:
            dfg.to_pickle('dfg.pkl')
            raise Exception('stop here')
            dfg = pd.read_pickle('dfg.pkl')
    elif (dfg.cdm_table == 'location').all():
        return dfg.apply(build_query, engine_bundle=engine_bundle, axis=1)
    else:

        dfg = dfg.groupby(['cdm_table']).agg({**{'index_type': _index_agg,
                                             'variables': _var_agg,
                                                 'row_ids': _join_row_ids,
                                                 'filters': _filter_agg,
                                                 'fields': _fields_agg},
                                              **{x: 'first' for x in ['start_col', 'partition_visit_detail_filter',
                                                                      'aggregation_function', 'partition_seq', 'index_type', 'lookup_join_type',
                                                                      'start_reference_point_time_delta', 'end_reference_point_time_delta',
                                                                      'concept_class_id', 'subject_id', 'subject_id_source_table',
                                                                      'subject_id_source_table_abbrev', 'cdm_table_abbrev', 'use_subset',
                                                                      'end_reference_point', 'start_reference_point',
                                                                      'index_bound', 'lab_test', 'end_col', 'drug_lookup_col',
                                                                      'additional_join_filter']}}).reset_index(drop=False)

        if dfg.shape[0] == 1:
            return build_query(dfg.iloc[0, :], engine_bundle=engine_bundle)
        else:
            dfg.to_pickle('dfg.pkl')
            raise Exception('stop here')

            dfg = pd.read_pickle('dfg.pkl')


def _index_agg(s: pd.Series) -> str:
    if 'person' in s:
        return 'person'
    elif 'visit_occurrence' in s:
        return 'visit_occurrence'
    else:
        return 'visit_detail'


def _filter_agg(s: pd.Series) -> str:
    # print('got here')
    non_null_filters: List[str] = [x.replace('\n', '\n\t\t') for x in s if isinstance(x, str)]

    # if len(non_null_filters) == 1:
    #     print(non_null_filters[0])
    #     non_null_filter: str = re.sub(r'AND\n\tvariable_name IN \([A-z_\', ]+\)\n|AND\n\t\tvariable_name IN \([A-z_\', ]+\)\n', '', non_null_filters[0])

    #     if not bool(re.search(r'AND|OR', non_null_filter)):
    #         return '\n\t' + non_null_filter[1:-1].strip()

    return None if len(non_null_filters) == 0 else ('(\n\t\t' + '\n\t\tOR\n\t\t'.join(f'{x} --XXX{i}XXX' for i, x in enumerate(non_null_filters)) + '\n\t)')


def _fields_agg(s: pd.Series) -> str:
    fields: List[str] = []

    for f in s:
        fields += f.split('||')  # re.sub(r'\s', '', f).split('||')

    return deduplicate_and_join(input_s=pd.Series(fields),
                                sep="||",
                                leading="",
                                trailing="")


def _var_agg(s: pd.Series) -> str:
    variables: List[str] = []

    for v in s:
        if isinstance(v, str):
            variables += re.sub(r'\s', '', v).replace("'", '').split(',')

    return deduplicate_and_join(input_s=pd.Series(variables),
                                sep="', '",
                                leading="'",
                                trailing="'")

# def _add_casts(row: pd.Series) -> str:
#     return '||'.join(list(set([f'COVERT({row.cast}, {x}) [x]' for x in re.sub(r'\s', '', row.fields).split('||')])))


def _format_selection(row: pd.Series) -> str:

    try:

        tbl_abbrev: str = row.cdm_table_abbrev + ('2' if ((row.index_type != row.subject_id_source_table) and (row.cdm_table == row.subject_id_source_table)) else '')

        # determine field
        if isinstance(row.custom_field_function, (str, int, float)) and pd.notnull(row.custom_field_function):
            field: str = str(row.custom_field_function)
        else:
            field: str = f'{tbl_abbrev}.{row.cdm_field_name}'

        # determine base selection
        if isinstance(row.convert, str):
            base_selection: str = f'CONVERT({row.convert}, {field})'
        elif isinstance(row.aggregation_function, str):
            base_selection: str = f'{row.aggregation_function}({field})'
        elif (row.cdm_table == 'location') and (row.index_type == 'person') and row.location_history_available:
            base_selection: str = f'COALESCE(l2.{row.cdm_field_name}, {field})'
        else:
            base_selection: str = field

        return f'{base_selection} [{coalesce(row.result_field_name, (row.variable_name if not isinstance(row.concept_class_id, str) else row.cdm_field_name), row.cdm_field_name)}]'
    except Exception as e:
        print(row)
        raise Exception(e)


def _solve_field_join(row: pd.Series) -> str:
    join_id: str = f'{row.index_type}_id' if row.index_bound else 'person_id'

    if row.cdm_table == 'cost':
        out: str = f'''{row.cdm_table_abbrev}.cost_event_id = {row.subject_id_source_table_abbrev}.{join_id} AND cost_domain_id = '{row.index_type.replace('_', ' ')}' '''
    elif row.cdm_table == 'provider':
        out: str = f'{row.cdm_table_abbrev}.provider_id = {table_abbrev_dict.get(row.index_type)}.provider_id'
    else:
        out: str = f'{row.cdm_table_abbrev}.{join_id} = {row.subject_id_source_table_abbrev}.{join_id}'

    if isinstance(row.additional_join_filter, str):
        out: str = f'({out}) AND (row.additional_join_filter)'

    return out


def build_query(row: pd.Series, engine_bundle: omop_engine_bundle) -> str:

    try:
        # handle case where subject_id table had to be joined twice
        if (row.subject_id_source_table == row.cdm_table) and (row.index_type != row.cdm_table):
            row['cdm_table_abbrev'] = row['cdm_table_abbrev'] + '2'
        ### Comments ###
        comments: List[str] = [f'{_solve_file_name(row)} Query prepared at {dt.now()} by {os.path.basename(__file__)}', f'retrieves variables from the following row_ids: {row.row_ids}']

        ### Configure Fields ###
        # Add ID fields
        id_fields: List[str] = ['C.subject_id'] + [f'{table_abbrev_dict.get(row.index_type) if row.cdm_table == "payer_plan_period" else row.subject_id_source_table_abbrev if row.cdm_table in ["cost", "location_history", "location"] else row.cdm_table_abbrev}.{x}' for x in (['person_id'] if row.cdm_table in ['person', 'death'] else
                                                                                                                                                                                                                                                                                   ['person_id', 'visit_occurrence_id'] if row.cdm_table in ['visit_occurrence'] else
                                                                                                                                                                                                                                                                                   ['person_id', 'visit_occurrence_id', 'visit_detail_id'] if row.cdm_table in ['visit_detail',
                                                                                                                                                                                                                                                                                                                                                                'condition_occurrence',
                                                                                                                                                                                                                                                                                                                                                                'drug_exposure',
                                                                                                                                                                                                                                                                                                                                                                'procedure_occurrence',
                                                                                                                                                                                                                                                                                                                                                                'device_exposure',
                                                                                                                                                                                                                                                                                                                                                                'measurement',
                                                                                                                                                                                                                                                                                                                                                                'observation',
                                                                                                                                                                                                                                                                                                                                                                'note'] else
                                                                                                                                                                                                                                                                                   [f'{row.index_type}_id'] if row.cdm_table in ['location', 'care_site', 'provider',
                                                                                                                                                                                                                                                                                                                                 'payer_plan_period', 'cost'] else []) if x != row.subject_id]
        fields: List[str] = []

        # add unit concept id
        if ('unit_concept_id' not in row.fields) and (row.cdm_table in ['device_exposure', 'observation', 'measurement']) and (('value_as_number' in row.fields) or ('quantity' in row.fields)):
            id_fields += ['unit_concept_id']
        elif ('quantity' in row.fields) and (row.cdm_table in ['drug_exposure']):
            id_fields += ['conversion_factor', 'numerator_unit_concept_id', 'numerator_unit', 'denominator_unit_concept_id', 'denominator_unit']
        elif ('poa' not in str(row.variables)) and (row.cdm_table in ['condition_occurrence']):
            fields += ['CASE WHEN co.condition_status_concept_id IN (32890, 32901, 32907) THEN 1 ELSE NULL END [poa]']

        if ('route_concept_id' not in row.fields) and (row.cdm_table in ['drug_exposure']):
            id_fields += [f'{row.cdm_table_abbrev}.route_concept_id']

        # remove visit_occurrence_id and/or visit detail id if non-index bound aggregation
        if (not row.index_bound) and isinstance(row.aggregation_function, str):
            id_fields: List[str] = [x for x in id_fields if x not in [f'{row.cdm_table_abbrev}.visit_detail_id', f'{row.cdm_table_abbrev}.visit_occurrence_id']]

        # add Time index
        if ((row.cdm_table not in ['person', 'location', 'provider', 'cost']) and (not isinstance(row.aggregation_function, str))):
            if table_time_index_dict.get(row.cdm_table) is None:
                fields += [f'{row.cdm_table_abbrev}.{table_start_time_index_dict.get(row.cdm_table)} [{table_start_time_index_dict.get(row.cdm_table)}]',
                           f'{row.cdm_table_abbrev}.{table_end_time_index_dict.get(row.cdm_table)} [{table_end_time_index_dict.get(row.cdm_table)}]']
            else:
                fields += [f'{row.cdm_table_abbrev}.{table_time_index_dict.get(row.cdm_table)} [{table_time_index_dict.get(row.cdm_table)}]']

        # add Variable Name
        if isinstance(row.concept_class_id, str):
            if isinstance(row.drug_lookup_col, str):
                id_fields += [f'{row.drug_lookup_col} [variable_name]']
            else:
                id_fields += ['variable_name']

        # add additional fields required for lab cleaning functions
        if row.lab_test:
            fields += [f'{row.cdm_table_abbrev}.{x} [{x}]' for x in ['value_as_concept_id', 'value_as_number', 'value_source_value', 'operator_concept_id', 'unit_concept_id', 'unit_source_concept_id', 'unit_source_value']]

        # add specified fields if not otherwise included already
        fields += [f'{row.cdm_table_abbrev}.{x} [{x}]' if ((row.cdm_table_abbrev not in x)
                                                           and ('case when' not in x.lower())
                                                           and (not bool(re.search(r'max\(|avg\(|count\(|min\(|stdev\(|sum\(|var\(', x.lower())))) else
                   x for x in row.fields.split('||') if (x not in fields) and (f'{row.cdm_table_abbrev}.{x} [x]' not in fields)]

        ### Configure Table Joins ###
        # start with cohort table
        joins: List[str] = [f'{engine_bundle.results_schema}.COHORT c']

        # add source table
        joins += [f'INNER JOIN {engine_bundle.data_schema}.{row.subject_id_source_table} {row.subject_id_source_table_abbrev} on {row.subject_id_source_table_abbrev}.{row.subject_id} = C.subject_id']

        start_idx_table: List[str] = [k for k, v in table_start_time_index_dict.items() if v == row.start_reference_point] + [k for k, v in table_start_time_index_dict2.items() if v == row.start_reference_point] + [k for k, v in table_end_time_index_dict.items() if v == row.start_reference_point] + [k for k, v in table_end_time_index_dict2.items() if v == row.start_reference_point]
        assert (len(start_idx_table) == 1) or (row.start_reference_point == 'unbounded'), f'Unable to reconcile start_reference_point: {row.start_reference_point} with OMOP CDM Tables'
        start_idx_table: str = start_idx_table[0] if (len(start_idx_table) == 1) else []

        # add index table if different then subject_id table and time before/after index is populated (if just the person/visit_occurrence id is needed, there is no need to join an additional table at this point)
        if (row.index_type != row.subject_id_source_table) and (row[['start_reference_point_time_delta', 'end_reference_point_time_delta']].notnull().any() or
                                                                (row.cdm_table == row.index_type) or
                                                                (((row.end_reference_point in [table_start_time_index_dict.get(row.index_type), table_end_time_index_dict.get(row.index_type),
                                                                                               table_start_time_index_dict2.get(row.index_type), table_end_time_index_dict2.get(row.index_type)]) or
                                                                  (row.start_reference_point in [table_start_time_index_dict.get(row.index_type), table_end_time_index_dict.get(row.index_type),
                                                                                                 table_start_time_index_dict2.get(row.index_type), table_end_time_index_dict2.get(row.index_type)])) and (not row.index_bound))):

            joins += [f'INNER JOIN {engine_bundle.data_schema}.{row.index_type} {table_abbrev_dict.get(row.index_type)} on {table_abbrev_dict.get(row.index_type)}.{row.index_type}_id = {row.subject_id_source_table_abbrev}.{row.index_type}_id']

        # join visit_detail or visit_occurrence table if needed in order to handle temporal filter
        if ((row.start_reference_point not in [table_start_time_index_dict.get(row.subject_id_source_table), table_end_time_index_dict.get(row.subject_id_source_table),
                                               table_start_time_index_dict2.get(row.subject_id_source_table), table_end_time_index_dict2.get(row.subject_id_source_table)])
            and (row.start_reference_point not in [table_start_time_index_dict.get(row.index_type), table_end_time_index_dict.get(row.index_type),
                                                   table_start_time_index_dict2.get(row.index_type), table_end_time_index_dict2.get(row.index_type)])
                and (start_idx_table in ['visit_detail', 'visit_occurrence'])):

            joins += [f'INNER JOIN {engine_bundle.data_schema}.{start_idx_table} {table_abbrev_dict.get(start_idx_table)} on {table_abbrev_dict.get(start_idx_table)}.{start_idx_table}_id = {row.subject_id_source_table_abbrev}.{start_idx_table}_id']

        end_idx_table: List[str] = [k for k, v in table_start_time_index_dict.items() if v == row.end_reference_point] + [k for k, v in table_start_time_index_dict2.items() if v == row.end_reference_point] + [k for k, v in table_end_time_index_dict.items() if v == row.end_reference_point] + [k for k, v in table_end_time_index_dict2.items() if v == row.end_reference_point]
        assert (len(end_idx_table) == 1) or (row.end_reference_point == 'unbounded'), f'Unable to reconcile start_reference_point: {row.start_reference_point} with OMOP CDM Tables'
        end_idx_table: str = end_idx_table[0] if (len(end_idx_table) == 1) else []

        # join visit_detail or visit_occurrence table if needed in order to handle temporal filter
        if ((row.end_reference_point not in [table_start_time_index_dict.get(row.subject_id_source_table), table_end_time_index_dict.get(row.subject_id_source_table),
                                             table_start_time_index_dict2.get(row.subject_id_source_table), table_end_time_index_dict2.get(row.subject_id_source_table)])
            and (row.end_reference_point not in [table_start_time_index_dict.get(row.index_type), table_end_time_index_dict.get(row.index_type),
                                                 table_start_time_index_dict2.get(row.index_type), table_end_time_index_dict2.get(row.index_type)])
            and (end_idx_table in ['visit_detail', 'visit_occurrence'])
                and (start_idx_table != end_idx_table)):

            joins += [f'INNER JOIN {engine_bundle.data_schema}.{end_idx_table} {table_abbrev_dict.get(end_idx_table)} on {table_abbrev_dict.get(end_idx_table)}.{end_idx_table}_id = {row.subject_id_source_table_abbrev}.{end_idx_table}_id']

        # join source table if not joined already
        # join the field table
        if row.cdm_table in ['location', 'payer_plan_period']:
            if row.index_type != row.subject_id_source_table:
                joins += [f'INNER JOIN {engine_bundle.data_schema}.{row.index_type} {table_abbrev_dict.get(row.index_type)} on {table_abbrev_dict.get(row.index_type)}.{row.index_type}_id = {row.subject_id_source_table_abbrev}.{row.index_type}_id']

            if row.cdm_table == 'location':
                if row.index_type == 'person':
                    if pd.read_sql(f'''SELECT COLUMN_NAME FROM {engine_bundle.database}.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'LOCATION_HISTORY' AND TABLE_SCHEMA = '{engine_bundle.data_schema.split(".")[-1]}';''', con=engine_bundle.engine).shape[0] > 0:
                        joins += [f'''LEFT JOIN {engine_bundle.data_schema}.LOCATION_HISTORY {table_abbrev_dict.get("location_history")} on ({table_abbrev_dict.get("location_history")}.entity_id = {row.subject_id_source_table_abbrev}.person_id AND {table_abbrev_dict.get("location_history")}.domain_id = 'person' AND CONVERT(DATE, {table_start_time_index_dict.get(row.subject_id_source_table)}) BETWEEN  {table_abbrev_dict.get("location_history")}.start_date AND {table_abbrev_dict.get("location_history")}.end_date)''',
                                  f'LEFT JOIN {engine_bundle.data_schema}.LOCATION l2 on l2.location_id = {table_abbrev_dict.get("location_history")}.location_id']

                        comments += [f'The patients Home Location from Location History at {table_start_time_index_dict.get(row.subject_id_source_table)} was used if available, otherwise the patients location from the person table was used']

                    joins += [f'LEFT JOIN {engine_bundle.data_schema}.LOCATION {table_abbrev_dict.get("location")} on {table_abbrev_dict.get("location")}.location_id = {table_abbrev_dict.get(row.index_type)}.location_id']
                elif row.index_type in ['visit_occurrence', 'visit_detail']:
                    joins += [f'INNER JOIN {engine_bundle.data_schema}.CARE_SITE {table_abbrev_dict.get("care_site")} on {table_abbrev_dict.get("care_site")}.care_site_id = {table_abbrev_dict.get(row.index_type)}.care_site_id',
                              f'INNER JOIN {engine_bundle.data_schema}.LOCATION {table_abbrev_dict.get("location")} on {table_abbrev_dict.get("location")}.location_id = {table_abbrev_dict.get("care_site")}.location_id']

            elif row.cdm_table == 'payer_plan_period':
                joins += [f'INNER JOIN {engine_bundle.data_schema}.{row.cdm_table} {row.cdm_table_abbrev} on (ppp.person_id = {table_abbrev_dict.get(row.index_type)}.person_id AND CONVERT(DATE, {table_abbrev_dict.get(row.index_type)}.{table_start_time_index_dict.get(row.index_type)}) BETWEEN ppp.[payer_plan_period_start_date] AND ppp.[payer_plan_period_end_date])']

        elif isinstance(row.partition_visit_detail_filter, str):
            joins += [f'INNER JOIN {engine_bundle.data_schema}.VISIT_DETAIL vdp on vdp.{row.index_type}_id = {row.subject_id_source_table_abbrev}.visit_occurrence_id',
                      f"INNER JOIN {engine_bundle.lookup_schema}.{engine_bundle.lookup_table} L on (L.concept_id = vdp.visit_detail_concept_id AND L.variable_name = '{row.partition_visit_detail_filter}')"]
            partioned_joins: List[str] = [f'INNER JOIN {engine_bundle.data_schema}.{row.cdm_table} {row.cdm_table_abbrev} on {row.cdm_table_abbrev}.visit_detail_id = partitioned.visit_detail_id']

        elif (row.cdm_table not in [row.subject_id_source_table, row.index_type]) or ('2' in row.cdm_table_abbrev):
            joins += [f'INNER JOIN {engine_bundle.data_schema}.{row.cdm_table} {row.cdm_table_abbrev} on {_solve_field_join(row)}']

        # join the lookup table
        if isinstance(row.concept_class_id, str):
            lookup_join_col: str = f'{row.cdm_table_abbrev}.{row.cdm_table.replace("_occurrence", "").replace("_exposure", "")}_concept_id'

            lt_col: str = 'drug_concept_id' if isinstance(row.drug_lookup_col, str) else 'concept_id'
            lt_n: str = row.drug_lookup_col if isinstance(row.drug_lookup_col, str) else 'variable_name'
            lt: str = engine_bundle.drug_lookup_table if isinstance(row.drug_lookup_col, str) else engine_bundle.lookup_table

            if ('primary_procedure' in row.variables):
                fj: str = f"{row.lookup_join_type} JOIN {engine_bundle.lookup_schema}.{lt} L on ((L.{lt_col} = {row.cdm_table_abbrev}.modifier_concept_id AND L.{lt_n} = 'primary_procedure') OR (L.concept_id = {lookup_join_col} AND L.{lt_n} IN ({row.variables})))"
            else:
                fj: str = f'{row.lookup_join_type} JOIN {engine_bundle.lookup_schema}.{lt} L on (L.{lt_col} = {lookup_join_col} AND L.{lt_n} IN ({row.variables}))'

            if isinstance(row.partition_visit_detail_filter, str):
                partioned_joins += [fj]
            else:
                joins += [fj]

        ### Establish the Where Criteria ###
        if isinstance(row.filters, str):
            if ('--XXX0XXX' not in row.filters) and (f"{coalesce(row.drug_lookup_col, 'variable_name')} IN" in row.filters) and bool(re.search(r'AND|OR', row.filters)):
                non_null_filter: str = re.sub(r'AND\s+{}\s+IN\s+\([A-z_0-9\',\s]+\)\s+|\s+{}\s+IN\s+\([A-z_0-9\',\s]+\)\s+AND'.format(coalesce(row.drug_lookup_col, 'variable_name'),
                                                                                                                                      coalesce(row.drug_lookup_col, 'variable_name')),
                                              '', row.filters)

                row['filters'] = non_null_filter[1:-1].strip() if ((not bool(re.search(r'AND|OR', non_null_filter))) or (('BETWEEN' in non_null_filter) and (len(re.findall('AND', non_null_filter)) == 1))) else non_null_filter

        criteria: List[str] = ([row.filters] if (isinstance(row.filters, str) and (not bool(re.search(r'^{} IN \([A-z_\', ]+\)$'.format(coalesce(row.drug_lookup_col, 'variable_name')), str(row.filters))))) else [])
        cohort_criteria: List[str] = ['C.cohort_definition_id = XXXXXX']

        # add subset filter
        if row.use_subset:
            cohort_criteria += ['C.subset_id = YYYY']

        # add count to aggregation functions (besides count, min, and max) in order to allow reconcilliation of numbers with different units
        if isinstance(row.aggregation_function, str) and (str(row.aggregation_function).lower() not in ['min', 'max', 'count']):
            fields += [x.replace(f'{row.aggregation_function}(', 'COUNT(')[:-1] + '_count]' for x in fields if row.aggregation_function in x]

        ### compile Query ###
        field_statement: str = '\n\t' + ',\n\t'.join(id_fields + fields) + '\n'
        join_statement: str = '\n\t' + '\n\t'.join(joins) + '\n'
        where_statement: str = '\n\t' + '\n\tAND\n\t'.join(criteria if isinstance(row.partition_visit_detail_filter, str) else (criteria + cohort_criteria))

        # handle partion by index and order by to get first, last, 2nd , etc.
        if isinstance(row.partition_seq, (int, float)):
            if isinstance(row.partition_visit_detail_filter, str):
                seq_str: str = ',\n\t' + f'ROW_NUMBER() OVER(PARTITION BY {row.subject_id_source_table_abbrev}.{row.index_type}_id ORDER BY {row.subject_id_source_table_abbrev}.visit_detail_start_datetime {"ASC" if row.partition_seq > 0 else "DESC"}) AS seq'

                partition_join_statement: str = '\n\t' + '\n\t'.join(partioned_joins) + '\n'
                partition_where_statement: str = '\n\t' + '\n\tAND\n\t'.join(cohort_criteria)
                where_statement: str = '\n\t' + '\n\tAND\n\t'.join(criteria + [f'seq = {int(abs(row.partition_seq))}'])

                query: str = 'with partitioned as (\n' + 'SELECT\n\tsubject_id,\n\tvdp.visit_detail_id{}\nFROM{}\nWHERE{})\nSELECT\n\n{}\nFROM\n\tpartitioned{}\nWHERE{};'.format(seq_str, join_statement, partition_where_statement, field_statement.replace('C.subject_id', 'subject_id'), partition_join_statement, where_statement)
            else:
                seq_str: str = ',\n\t' + f'ROW_NUMBER() OVER(PARTITION BY {row.subject_id_source_table_abbrev}.{row.index_type}_id {", {}".format(coalesce(row.drug_lookup_col, "variable_name")) if isinstance(row.concept_class_id, str) else ""} ORDER BY {row.cdm_table_abbrev}.{table_time_index_dict.get(row.cdm_table, table_start_time_index_dict.get(row.cdm_table))} {"ASC" if row.partition_seq > 0 else "DESC"}) AS seq'

                partition_field_statement: str = '\n\t' + ',\n\t'.join([(re.search(r'\[[A-z_0-9]+\]', x).group(0) if ']' in x else x) for x in sorted(list(set([re.sub(r'\.|'.join(list(table_abbrev_dict.values()) + [row.cdm_table_abbrev]) + r'\.', '', x) for x in (id_fields + fields)])))]) + '\n'

                query: str = 'with partitioned as (\n' + 'SELECT\n\t' + field_statement.strip() + seq_str + '\nFROM' + join_statement + '\nWHERE' + where_statement + '\n)\n' + f'SELECT {partition_field_statement} FROM partitioned WHERE seq = {int(abs(row.partition_seq))};'''
        else:
            query: str = f"""SELECT {field_statement} FROM {join_statement} WHERE {where_statement};"""

            if isinstance(row.aggregation_function, str):
                query: str = '{}\nGROUP BY{}'.format(query[:-1], '\n\t' + ',\n\t'.join([re.sub(r'\[[A-z_]+\]', '', x) for x in id_fields]) + '\n')

        # add commments to the query
        query: str = '/******\n{}\n******/\n{}'.format('\n\n'.join(comments), query)

        return pd.Series({'query': query, 'row_ids': row.row_ids})
    except Exception as e:
        row.to_pickle('row.pkl')
        raise Exception(e)
        row = pd.read_pickle('row.pkl')
