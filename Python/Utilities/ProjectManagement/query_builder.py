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
from sql_metadata import Parser
from ..PreProcessing.data_format_and_manipulation import deduplicate_and_join, coalesce
from ..FileHandling.io import save_data, check_load_df, find_files
from ..PreProcessing.standardization_functions import process_df_v2
from ..Database.connect_to_database import omop_engine_bundle, execute_query_in_transaction
from ..General.func_utils import debug_inputs


def validate_and_run_query_builder(spec_fp: str,
                                   engine_bundle: omop_engine_bundle,
                                   cohort_definition_id: int,
                                   project_name: str,
                                   dir_dict: Dict[str, str],
                                   limit_to_cohorts: Union[str, None] = None,
                                   mode: str = 'audit',
                                   cdm_version: str = '5.4',
                                   additional_variables_to_generate: Union[List[str], None] = None,
                                   quick_audit_n: Union[int, None] = None,
                                   save_audit_queries: bool = False,
                                   time_index_mode: bool = True,
                                   time_index_sub_visit_precision: str = 'datetime',
                                   time_index_visit_precision: str = 'datetime',
                                   cache_meta_for_debug: bool = False,
                                   use_visit_occurrence_parent_information: bool = True) -> list:
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
        Whether data should be audited (audit), build Queries (build) or SQL Queries built (data_retrieval) or both (both). The default is 'audit'.
    quick_audit_n : Union[int, None], optional
        Audit the first n samples in each table matching the criterion. The default is None which will audit the entire table.
    save_audit_queries : bool, optional
        Whether the audit queries should be saved to the audit_source directory or not. The default is False, which will save the results of the audit queries, but not the queries themselves.
    additional_variables_to_generate: Union[List[str], None], optional
        List of ancillary projects to generate data for
    time_index_mode: bool, optional
        Use the start/end date/datetimes for the referenced visit_occurrence/visit_detail instead of the visit_occurrence_id/visit_detail_id. This should be used if concurrent encounter information has not been harmonized under one parent visit occurrence id.
    time_index_sub_visit_precision: str, optional
        The precision of the subvisit index (drug_exposure, visit_detail, procedure_occurrence, etc) used in the cohort definition. The options are 'datetime' (use exact datetime) and 'date' (use only date part). The default is datetime.
    time_index_visit_precision: str, optional
        The precision of the visit index (e.g. visit occurrence) used in the cohort definition. The options are 'datetime' (use exact datetime) and 'date' (use only date part). The default is datetime.
    cache_meta_for_debug: bool, optional
        Argument used to debug the python script of query builder without having to wait for the database query which can take some time. Default is false. This should not be true in production.
    use_visit_occurrence_parent_information: bool, optional
        Whether to use the source visit occurrence ids and values or to use the curated parents. The default is True, which will use the curated information. Note: There is a check to ensure the parent columns exist, if they do not it will fallback to the source information.
    
    Returns
    -------
    List[str]
        List of SQL query file paths.

    """
    assert mode in ['audit', 'data_retrieval', 'both', 'build'], f"Unsupported mode: {mode}, please choose from ['audit', 'data_retrieval', 'both']"
    assert cdm_version in ['5.4', '5.3'], f"Unsupoorted CDM version: {cdm_version}, please choose from ['5.4', '5.3']"
    assert time_index_sub_visit_precision in ['date', 'datetime'], f'Unsupoprted time_index_visit_precision: {time_index_sub_visit_precision}. Please choose from the following ["datetime", "date"]'
    assert time_index_visit_precision in ['date', 'datetime'], f'Unsupoprted time_index_visit_precision: {time_index_visit_precision}. Please choose from the following ["datetime", "date"]'

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
            assert gen_variable_df.project.isin([v]).any(), f'Unable to find the requested project: {v}. Please choose from the following: {gen_variable_df.project.unique().tolist()}'

        variable_df = pd.concat([variable_df, gen_variable_df[gen_variable_df.project.isin(additional_variables_to_generate)]], axis=0, ignore_index=True)

    variable_df.dropna(subset=['variable_name', 'cdm_field_name'], how='all', inplace=True)

    assert variable_df.project.notnull().all(), f'Variable Specification document has {variable_df.project.isnull()} rows missing project names'

    # TODO: add additional validation assertions

    return _build_queries(df=variable_df,
                          engine_bundle=engine_bundle,
                          cohort_definition_id=cohort_definition_id,
                          dir_dict=dir_dict,
                          cdm_version=cdm_version,
                          query_complete_fp=query_complete_fp,
                          audit_complete_fp=audit_complete_fp,
                          mode=mode,
                          time_index_visit_precision=time_index_visit_precision,
                          time_index_sub_visit_precision=time_index_sub_visit_precision,
                          limit_to_cohorts=limit_to_cohorts,
                          project_name=project_name,
                          cache_meta=cache_meta_for_debug,
                          time_index_mode=time_index_mode,
                          quick_audit_n=quick_audit_n,
                          save_audit_queries=save_audit_queries,
                          use_visit_occurrence_parent_information=(use_visit_occurrence_parent_information
                                                                   and
                                                                   (pd.read_sql(f'''SELECT TOP 1 *
                                                                                   FROM
                                                                                       {engine_bundle.data_schema.split(".")[0] if "." in engine_bundle.data_schema else engine_bundle.database}.INFORMATION_SCHEMA.COLUMNS
                                                                                   WHERE
                                                                                       TABLE_SCHEMA = '{engine_bundle.data_schema.split(".")[-1]}'
                                                                                       AND
                                                                                       TABLE_NAME = 'VISIT_OCCURRENCE'
                                                                                       AND
                                                                                       COLUMN_NAME = 'parent_visit_occurrence_id';''', con=engine_bundle.engine).shape[0] == 1)))


def _build_queries(df: pd.DataFrame,
                   engine_bundle: omop_engine_bundle,
                   cohort_definition_id: int,
                   cdm_version: str,
                   dir_dict: Dict[str, str],
                   query_complete_fp: str,
                   audit_complete_fp: str,
                   time_index_mode: bool,
                   project_name: str,
                   time_index_sub_visit_precision: str,
                   time_index_visit_precision: str,
                   mode: str = 'audit',
                   quick_audit_n: Union[int, None] = None,
                   save_audit_queries: bool = False,
                   use_visit_occurrence_parent_information: bool = False,
                   limit_to_cohorts: Union[str, None] = None,
                   custom_query_folder: Union[str, None] = None,
                   cache_meta: bool = False,
                   **logging_kwargs) -> List[str]:
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
    use_visit_occurrence_parent_information: bool, Optional
        Whether to use the parent visit occurrence ids instead of source ones for queries involving visit occurrence information.

    Returns
    -------
    List[str]
        List of SQL query file paths.

    """
    assert mode in ['audit', 'data_retrieval', 'both', 'build'], f"Unsupported mode: {mode}, please choose from ['audit', 'data_retrieval', 'both']"

    assert cdm_version in ['5.4', '5.3'], f"Unsupoorted CDM version: {cdm_version}, please choose from ['5.4', '5.3']"
    
    if cache_meta:
        computed_df_fp: str = os.path.join(dir_dict.get('intermediate_data'), 'computed_df_cache.pkl')
        raw_df_fp: str = os.path.join(dir_dict.get('intermediate_data'), 'raw_df_cache.pkl')
        
        if os.path.exists(computed_df_fp) and computed_df_fp:
            raw_df = pd.read_pickle(raw_df_fp)
            computed_df = pd.read_pickle(computed_df_fp)
        else:
            raw_df, computed_df = _format_metadata(df=check_load_df(df), engine_bundle=engine_bundle, cohort_definition_id=cohort_definition_id, cdm_version=cdm_version)
            
            raw_df.to_pickle(raw_df_fp)
            computed_df.to_pickle(computed_df_fp)
    else:
        raw_df, computed_df = _format_metadata(df=check_load_df(df), engine_bundle=engine_bundle, cohort_definition_id=cohort_definition_id, cdm_version=cdm_version)

    if mode in ['audit', 'both']:
        if not os.path.exists(audit_complete_fp):
        
            raw_df.groupby('cdm_table', as_index=False, group_keys=False).apply(retrieve_audit_data, engine_bundle=engine_bundle,
                                                                                dir_dict=dir_dict, quick_audit_n=quick_audit_n,
                                                                                save_query=save_audit_queries,
                                                                                limit_to_cohorts=limit_to_cohorts,
                                                                                project_name=project_name,
                                                                                use_visit_occurrence_parent_information=use_visit_occurrence_parent_information)

            analyze_audit_data(dir_dict=dir_dict, raw_df=raw_df.copy(deep=True), engine_bundle=engine_bundle, limit_to_cohorts=limit_to_cohorts)

            open(audit_complete_fp, 'a').close()

    if mode in ['data_retrieval', 'both', 'build']:

        if not os.path.exists(query_complete_fp):
            save_data(build_data_queries(df=raw_df, engine_bundle=engine_bundle, query_save_folder=dir_dict.get('SQL'), cdm_version=cdm_version,
                                         use_visit_occurrence_parent_information=use_visit_occurrence_parent_information,
                                         time_index_visit_precision=time_index_visit_precision,
                                         time_index_sub_visit_precision=time_index_sub_visit_precision,
                                         time_index_mode=time_index_mode),
                      out_path=os.path.join(dir_dict.get('intermediate_data'), 'variable_file_linkage.xlsx'))

            # 1 use the find_files function to find any sql files in the custom query directory (if exists)
            # 2 check against the list of built queries to ensure that there are no collisions (e.g. custom query.sql file can't use a name created by the query builder)
            # 3 load each custom sql file and replace the Data,schema,database place holders with the values from the engine_bundle and save to the output sql file from the query builder
            # 4 use a sql_parser to read the variable names in the custom queries to append the information to the variable_file_lookup.xlsx
            # example in def _parse_columns(sql_query: str, table_map: dict) -> dict from the FileHandling.io module, which uses this module from sql_metadata import Parser
            if isinstance(dir_dict.get('custom_sql'), str):
                if os.path.exists(dir_dict.get('custom_sql')):
                    custom_sql_files: List[str] =  find_files(directory=dir_dict.get('custom_sql'),
                                                              patterns=[r'.*\.sql'],
                                                              exclusion_patterns=[r'eligibility_criteria\.sql'],
                                                              regex=True, agg_results=True, recursive=False)
                    if len(custom_sql_files) > 0:
                        meta_df: pd.DataFrame = pd.read_excel(os.path.join(dir_dict.get('intermediate_data'), 'variable_file_linkage.xlsx'))
                        for query_file in custom_sql_files:
                            query_name: str = os.path.basename(query_file).replace('.sql', '')
                            
                            assert not meta_df.file_name.str.lower().isin([query_name.lower()]).any(), f"The custom query file {query_name} Collides with a file generated by Query builder. Please rename your custom query located: {dir_dict.get('custom_sql')} to a different name. You can check the list of used names here: {os.path.join(dir_dict.get('intermediate_data'), 'variable_file_linkage.xlsx')}"

                            query: str = check_load_df(query_file, replacements={'ReSuLtS_ScHeMa': engine_bundle.results_schema,
                                                                                 'DaTa_ScHeMa': engine_bundle.data_schema,
                                                                                 'VoCaB_ScHeMa': engine_bundle.vocab_schema,
                                                                                 'LoOkUp_ScHeMa': engine_bundle.lookup_schema,
                                                                                 'LoOkUp_TaBlE': engine_bundle.lookup_table,
                                                                                 'DrUg_LoOkUp_TaBlE': engine_bundle.drug_lookup_table},
                                                       return_raw_query=True, **logging_kwargs)
                            
                            if not use_visit_occurrence_parent_information:
                                for x in ['visit_occurrence_id', 'admitted_from_concept_id', 'admitted_from_source_value', 'discharged_to_concept_id',
                                          'discharged_to_source_value', 'visit_start_datetime', 'visit_end_datetime']:
                                    query: str = query.replace(f'parent_{x}', x)
                            
                            save_data(query, out_path=os.path.join(dir_dict.get('SQL'), os.path.basename(query_file)), **logging_kwargs)
                            
                           # print(query)
                            query_meta = Parser(query)
                            # print(query_meta.columns_aliases.items())
                            
                            new_meta_df: pd.DataFrame = pd.DataFrame([[x[:x.rfind('.')].lower() if '.' in x else None,x[x.rfind('.')+1:].lower(), x.lower() ] for x in query_meta.columns_dict.get('select')],
                                                                     columns=['cdm_table_alias', 'cdm_field_name', 'full_specification'])\
                                .merge(pd.DataFrame([[key.lower(), value.lower()] for key, value in query_meta.tables_aliases.items()], columns=['cdm_table_alias', 'cdm_table']),
                                       how='left', on='cdm_table_alias')\
                                .query('cdm_field_name != "variable_name"')\
                                .merge(
                                    pd.DataFrame([[key.lower().replace('[', '').replace(']', ''), (value if isinstance(value, str) else value[0]).lower()] for key, value in query_meta.columns_aliases.items()], columns=['result_field_name', 'full_specification']),
                                    on='full_specification', how='left')
                            new_meta_df.loc[new_meta_df.cdm_table.isnull(), 'cdm_table'] = new_meta_df.loc[new_meta_df.cdm_table.isnull(), 'cdm_table_alias']
                            
                            new_meta_df.loc[new_meta_df.cdm_table.notnull(), 'cdm_table'] = new_meta_df.loc[new_meta_df.cdm_table.notnull(), 'cdm_table'].apply(lambda x: x.split('.')[-1]).values
                            
                            new_meta_df.drop(columns=['cdm_table_alias', 'full_specification'], inplace=True)
                            new_meta_df['file_name'] = query_name
                            new_meta_df['project'] = project_name
                            
                            meta_df: pd.DataFrame = pd.concat([meta_df, new_meta_df], axis=0, sort=False, ignore_index=True)
                    save_data(meta_df, out_path=os.path.join(dir_dict.get('intermediate_data'), 'variable_file_linkage.xlsx'), **logging_kwargs)
                            
                            
                            

            open(query_complete_fp, 'a').close()
        if mode in ['data_retrieval', 'both']:
            return find_files(directory=dir_dict.get('SQL'),
                              patterns=[r'.*\.sql'],
                              exclusion_patterns=[r'eligibility_criteria\.sql'],
                              regex=True, agg_results=True, recursive=False)

    return []


def get_subject_id(cohort_definition_id: int, engine_bundle: omop_engine_bundle) -> str:

    if  isinstance(cohort_definition_id, str):
        assert cohort_definition_id in ['person_id', 'visit_occurrence_id', 'visit_detail_id', 'procedure_occurrence_id',
                                        'condition_occurrence_id', 'drug_exposure_id', 'device_exposure_id']
        return cohort_definition_id
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
    lookup_table: str = f"{str(dt.now().timestamp()).replace('.', '_')}_tmp_table_concept_class_lookup"
    # save to database
    df.to_sql(name=lookup_table, schema=engine_bundle.operational_schema, if_exists='replace', con=engine_bundle.engine, index=True)

    dl_joins: str = '\n'.join([f'LEFT JOIN {engine_bundle.lookup_schema}.{engine_bundle.drug_lookup_table} DL{i} on S.variable_name = DL{i}.{col}' for i, col in enumerate(df.drug_lookup_col.dropna().unique().tolist())])
    dl_classes: str = ','.join([f'DL{i}.drug_concept_class' for i in range(df.drug_lookup_col.dropna().nunique())])
    
    dl_cw: str = f'OR (COALESCE({dl_classes} , NULL) IS NOT NULL)' if len(dl_classes) > 0 else ''
    
    cdm_schema: str = engine_bundle.data_schema.split('.')[-1]

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
                                                                      WHEN L.domain_id IN ('Drug', 'Device') {dl_cw} THEN CONCAT (COALESCE(L.domain_id, 'drug'), '_exposure') ELSE L.Domain_id END,
                                                                         IFC.TABLE_NAME) [cdm_table]
                                                    FROM
                                                        {engine_bundle.operational_schema}.[{lookup_table}] S
                                                        LEFT JOIN {engine_bundle.lookup_schema}.{engine_bundle.lookup_table} L on S.variable_name = L.variable_name
                                                        {dl_joins}
                                                        INNER JOIN {engine_bundle.database}.INFORMATION_SCHEMA.COLUMNS IFC on (IFC.COLUMN_NAME = S.cdm_field_name
                                                                                                                              AND IFC.TABLE_SCHEMA = '{cdm_schema}'
                                                                                                                              AND (COALESCE(CONVERT(VARCHAR, S.[cdm_table]), CASE WHEN L.domain_id IN ('Procedure', 'Condition') THEN CONCAT(L.domain_id, '_Occurrence')
                                                                      WHEN L.domain_id IN ('Drug', 'Device') THEN CONCAT (L.domain_id, '_exposure') ELSE L.Domain_id END) = IFC.TABLE_NAME OR L.domain_id IS NULL))
                                                        WHERE
                                                            IFC.TABLE_NAME NOT LIKE '%_STAGE'
                                                        ) F
                                                    GROUP BY
                                                        [index], [cdm_table];''',
                                       con=engine_bundle.engine)

    assert result.cdm_table.notnull().all(), 'There is one or more field/variable pairs without a table. Please check the field list against the CDM specification'

    execute_query_in_transaction(engine=engine_bundle.engine, raise_exceptions=True, query=f'DROP TABLE {engine_bundle.operational_schema}.[{lookup_table}]')

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

invalid_cdm_v53_fields: List[str] = ['devE.unit_concept_id', 'devE.unit_source_value',
                                     'devE.unit_source_concept_id',
                                     'm.unit_source_concept_id', 'o.value_source_value']

def _format_metadata(df: pd.DataFrame, engine_bundle: omop_engine_bundle, cohort_definition_id: Union[int, str], cdm_version: str) -> pd.DataFrame:
    
    # debug_inputs(function=_format_metadata, kwargs=locals(), dump_fp='test.pkl')
    # raise Exception('stop here')
    # import pickle
    # locals().update(pickle.load(open('test.pkl', 'rb')))
    subject_id: str = get_subject_id(cohort_definition_id=cohort_definition_id, engine_bundle=engine_bundle)
    
    sub_visit_type: List[str] = ['condition_occurrence_id', 'device_exposure_id', 'drug_exposure_id',
                                 'episode_id', 'procedure_occurrence_id', 'visit_detail_id']
    # replace subVisit index placeholders
    if df.index_type.astype(str).str.contains('XXXSubVisitXXX', regex=False, na=False).any():
        assert subject_id in sub_visit_type, f'The subject_id {subject_id} is not compatible with this query specification which requires the subject id type to be more specific than a vist. Options include: {sub_visit_type}'
        
        df.index_type = df.index_type.replace({'XXXSubVisitXXX': subject_id[:-3], 'XXXSubVisitXXX_force': f'{subject_id[:-3]}_force'})
    
    # replace subject index placeholders
    if df.index_type.astype(str).str.contains('XXXSubjectXXX', regex=False, na=False).any():
        df.index_type = df.index_type.replace({'XXXSubjectXXX': subject_id[:-3], 'XXXSubjectXXX_force': f'{subject_id[:-3]}_force'})
        
    # replace subVisit index placeholders
    for col in ['start_reference_point', 'end_reference_point']:
        if df[col].astype(str).str.contains('XXXSubVisit_start_datetimeXXX|XXXSubVisit_start_dateXXX|XXXSubVisit_end_datetimeXXX|XXXSubVisit_end_dateXXX', regex=True, na=False).any():
            assert subject_id in sub_visit_type, f'The subject_id {subject_id} is not compatible with this query specification which requires the subject id type to be more specific than a vist. Options include: {sub_visit_type}'
             
            df[col] = df[col].replace({'XXXSubVisit_start_datetimeXXX': table_start_time_index_dict.get(subject_id[:-3], '-999XXX'),
                                       'XXXSubVisit_start_dateXXX': 'condition_start_date' if subject_id == 'condition_occurrence_id' else table_start_time_index_dict2.get(subject_id[:-3], '-999XXX'),
                                       'XXXSubVisit_end_datetimeXXX': table_end_time_index_dict.get(subject_id[:-3], '-999XXX'),
                                       'XXXSubVisit_end_dateXXX': 'condition_end_date' if subject_id == 'condition_occurrence_id' else table_end_time_index_dict2.get(subject_id[:-3], '-999XXX')}
                                      )
         
        # replace subject index placeholders
        if df[col].astype(str).str.contains('XXXSubject_start_datetimeXXX|XXXSubject_start_dateXXX|XXXSubject_end_datetimeXXX|XXXSubject_end_dateXXX', regex=True, na=False).any():
           df[col] = df[col].replace({'XXXSubject_start_datetimeXXX': table_start_time_index_dict.get(subject_id[:-3], '-999XXX'),
                                      'XXXSubject_start_dateXXX': 'condition_start_date' if subject_id == 'condition_occurrence_id' else table_start_time_index_dict2.get(subject_id[:-3], '-999XXX'),
                                      'XXXSubject_end_datetimeXXX': table_end_time_index_dict.get(subject_id[:-3], '-999XXX'),
                                      'XXXSubject_end_dateXXX': 'condition_end_date' if subject_id == 'condition_occurrence_id' else table_end_time_index_dict2.get(subject_id[:-3], '-999XXX')}
                                     )
    
        
    if cdm_version == '5.3':
        field_name_changes: dict = {'admitted_from_concept_id': 'admitting_source_concept_id',
                                    'admitted_from_source_value': 'admitting_source_value',
                                    'discharged_to_concept_id': 'discharge_to_concept_id',
                                    'discharged_to_source_value': 'discharge_to_source_value',
                                    'parent_visit_detail_id': 'visit_detail_parent_id'}
        
        # fields from v5.4 not in v5.3
        non_existant_fields: List[str] = ['procedure_end_date', 'procedure_end_datetime', 'production_id',
                                           'measurement_event_id', 'meas_event_field_concept_id',
                                           'observation_event_id', 'obs_event_field_concept_id',
                                           'note_event_id', 'note_event_field_concept_id',
                                           'country_concept_id', 'country_source_value',
                                           'latitude', 'lngitude']
        
        df.field_name.replace(field_name_changes, inplace=True)

        df: pd.DataFrame = df[~df.field_name.isin(non_existant_fields)].copy(deep=True).reset_index(drop=True)

        invalid_table_field_pair_idx: pd.Series = df[['field_name', 'cdm_table']].apply(lambda row: f'{table_abbrev_dict.get(str(row.cdm_table).lower())}.{str(row.field_name).lower()}', axis=1).isin(invalid_cdm_v53_fields)

        df: pd.DataFrame = df[~invalid_table_field_pair_idx].copy(deep=True).reset_index(drop=True)

        table_end_time_index_dict.pop('procedure_occurrence', None)
        table_end_time_index_dict2.pop('procedure_occurrence', None)

    df = get_table_and_concept_class(df=df, engine_bundle=engine_bundle)

    df['row_id'] = pd.Series(range(df.shape[0])).astype(str).values

    generated_var_idx: pd.Series = df.computation_source.notnull()

    compuatation_fields: pd.DataFrame = df[generated_var_idx].copy(deep=True)

    df = df[~generated_var_idx].copy(deep=True)

    df['subject_id'] = subject_id
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
        df.loc[missing_start_idx, 'start_reference_point'] = df.loc[missing_start_idx, 'index_type'].apply(lambda x: table_start_time_index_dict.get(x.replace('_force', ''), 'unbounded'))

    missing_end_idx: pd.Series = df.end_reference_point.isnull()
    if missing_end_idx.any():
        df.loc[missing_end_idx, 'end_reference_point'] = df.loc[missing_end_idx, 'index_type'].apply(lambda x: table_end_time_index_dict.get(x.replace('_force', ''), 'unbounded'))
        
    # ensure forced indexes don't have any time indexes
    forced_idx: pd.Series = df.index_type.astype(str).str.contains(r'_force$', regex=True)
    if forced_idx.any():
        for col in ['start_col', 'end_col', 'start_reference_point_time_delta', 'end_reference_point_time_delta']:
            df.loc[forced_idx, col] = None

    return df, compuatation_fields


def _solve_filter(row: pd.Series, time_index_mode: bool, engine_bundle: omop_engine_bundle, time_index_sub_visit_precision: str,
                  time_index_visit_precision: str) -> pd.Series:
    
    # if (row.cdm_table == 'measurement') and ('rbc_transfusion' in str(row.variables)):
    #     debug_inputs(function=_solve_filter, kwargs=locals(), dump_fp='_solve_filter_deubg.pkl')
    #     raise Exception('stop here')
    #     import pickle
    #     locals().update(pickle.load(open('_solve_filter_deubg.pkl', 'rb')))
    try:
        forced: bool = '_force' in row.index_type
        if forced:
            row.index_type = row.index_type.replace('_force', '')
            time_index_mode: bool = False
        # determine time index for joined table
        tbl_time_index: str = table_time_index_dict.get(row.cdm_table,
                                                        (table_start_time_index_dict2 if row.cdm_table in ['procedure_occurrence'] else
                                                         table_start_time_index_dict).get(row.cdm_table))
        
        # rename table abbrev if it is partitioned to avoid downstream problems
        if (row.subject_id_source_table == row.cdm_table) and pd.notnull(row.partition_seq) and ('2' not in  row.cdm_table_abbrev):
            row.cdm_table_abbrev = f'{row.cdm_table_abbrev}2'
        # if not isinstance(tbl_time_index, str):
        #     print(row)
    
        # determine start column
        start_col: str = row.start_reference_point if isinstance(row.start_reference_point, str) else table_start_time_index_dict.get(row.index_type.replace('_force', ''), 'unbounded')
    
        # switch datetime field to date field if cdm_table_time_index is a date, or the precision of the index type is a date
        if (
                (bool(re.search(r'datetime$', start_col)) and (bool(re.search(r'date$', str(tbl_time_index))) or ('CONVERT(DATE, ' in str(tbl_time_index))))
                or
                ((bool(re.search(r'date$', str(start_col))) or ('CONVERT(DATE, ' in str(start_col))) and bool(re.search(r'datetime$', str(tbl_time_index))))
                or
                (bool(re.search(r'datetime$', start_col)) and (str(row.index_type).lower() in ['visit_occurrence']) and (time_index_visit_precision == 'date'))
                or
                (bool(re.search(r'datetime$', start_col)) and (str(row.index_type).lower() not in ['visit_occurrence']) and (time_index_sub_visit_precision == 'date'))
            ):
            start_col_str: str = start_col.replace('datetime', 'date')
            if isinstance(tbl_time_index, str):
                if (row.cdm_table in ['person']):
                    tbl_time_index: str = f'CONVERT(DATE, {tbl_time_index})'
                else:
                    tbl_time_index: str = tbl_time_index.replace('datetime', 'date')

        else:
            start_col_str: str = start_col
            
        if '.' not in start_col_str:
            
            st_table_abbrev: str = None
            
            for d in [table_time_index_dict, table_start_time_index_dict, table_start_time_index_dict2]:
                if pd.isnull(st_table_abbrev):
                    for k, v in d.items():
                        if start_col == v:
                            st_table_abbrev = table_abbrev_dict.get(k)
                            break
            if isinstance(st_table_abbrev, str):
                start_col_str: str = f'{st_table_abbrev}.{start_col_str}'
    
        # cdm field for start key
        if (row.index_type != 'person') and isinstance(tbl_time_index, str):
    
            # extract hours/days and set filter statement
            if (start_col != 'unbounded') and isinstance(row.start_reference_point_time_delta, str):
                try:
                    start_time_delta: str = re.search(r'([0-9]+)\s(day|hour)', row.start_reference_point_time_delta, re.IGNORECASE).groups(0)
                except AttributeError:
                    raise Exception(f'Unable to parse timedelta: {row.start_reference_point_time_delta} from the field {start_col} for the row corresponding to the field/variable pair {row.fields}: {row.variables}')
    
                lower_filter: str = f"DATEADD({start_time_delta[1]}, {'-' if '-' in row.start_reference_point_time_delta else ''}{start_time_delta[0]}, {start_col_str})"
                if bool(re.search(r'datetime$', start_col_str)) and bool(re.search(r'date$', tbl_time_index)):
                    lower_filter: str = f'CONVERT(DATE, {lower_filter})'
            elif pd.isnull(row.start_reference_point_time_delta) and (start_col != 'unbounded'):
                lower_filter: str = start_col_str
                if bool(re.search(r'datetime$', start_col_str)) and bool(re.search(r'date$', tbl_time_index)):
                    lower_filter: str = f'CONVERT(DATE, {lower_filter})'
                start_time_delta: str = None
            else:
                start_time_delta: str = start_col
        else:
            start_time_delta: str = start_col
    
        end_col = row.end_reference_point if isinstance(row.end_reference_point, str) else table_end_time_index_dict.get(row.index_type.replace('_force', ''), 'unbounded')
    
        # switch datetime field to date field if cdm_table_time_index is a date, or the precision of the index type is a date
        if (
                (bool(re.search(r'datetime$', str(end_col))) and (bool(re.search(r'date$', str(tbl_time_index))) or ('CONVERT(DATE, ' in str(tbl_time_index))))
                or
                ((bool(re.search(r'date$', str(end_col))) or ('CONVERT(DATE, ' in str(end_col))) and bool(re.search(r'datetime$', str(tbl_time_index))))
                or
                (bool(re.search(r'datetime$', end_col)) and (str(row.index_type).lower() in ['visit_occurrence']) and (time_index_visit_precision == 'date'))
                or
                (bool(re.search(r'datetime$', end_col)) and (str(row.index_type).lower() not in ['visit_occurrence']) and (time_index_sub_visit_precision == 'date'))
            ):
            end_col_str: str = end_col.replace('datetime', 'date')
            if isinstance(tbl_time_index, str):
                if (row.cdm_table in ['person']):
                    if ('CONVERT(DATE, ' not in tbl_time_index):
                        tbl_time_index: str = f'CONVERT(DATE, {tbl_time_index})'
                else:
                    tbl_time_index: str = tbl_time_index.replace('datetime', 'date')
        else:
            end_col_str: str = end_col
            
        if '.' not in end_col_str:
            
            end_table_abbrev: str = None
            
            for d in [table_time_index_dict, table_end_time_index_dict, table_end_time_index_dict2]:
                if pd.isnull(end_table_abbrev):
                    for k, v in d.items():
                        if end_col == v:
                            end_table_abbrev = table_abbrev_dict.get(k)
                            break
            if isinstance(end_table_abbrev, str):
                end_col_str: str = f'{end_table_abbrev}.{end_col_str}'

        # cdm field for start key
        if (row.index_type != 'person') and isinstance(tbl_time_index, str):
    
            # extract hours/days and set filter statement
            if (end_col != 'unbounded') and isinstance(row.end_reference_point_time_delta, str):
                try:
                    end_time_delta: str = re.search(r'([0-9]+)\s(day|hour)', row.end_reference_point_time_delta, re.IGNORECASE).groups(0)
                except AttributeError:
                    raise Exception(f'Unable to parse timedelta: {row.end_reference_point_time_delta} from the field {end_col} for the row corresponding to the field/variable pair {row.fields}: {row.variables}')
    
                upper_filter: str = f"DATEADD({end_time_delta[1]}, {'-' if '-' in row.end_reference_point_time_delta else ''}{end_time_delta[0]}, {end_col_str})"
                if bool(re.search(r'datetime$', end_col_str)) and bool(re.search(r'date$', tbl_time_index)):
                    upper_filter: str = f'CONVERT(DATE, {upper_filter})'
            elif pd.isnull(row.end_reference_point_time_delta) and (end_col != 'unbounded'):
                upper_filter: str = end_col_str
                if bool(re.search(r'datetime$', end_col_str)) and bool(re.search(r'date$', tbl_time_index)):
                    upper_filter: str = f'CONVERT(DATE, {upper_filter})'
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
        if (
                (
                    (
                        pd.isnull(start_time_delta)
                        and
                        pd.isnull(end_time_delta)
                    )
                    and
                    (row.index_type != 'person')
                    and
                    (row.cdm_table not in ['condition_occurrence'])
                    and
                    (
                        (end_col in [table_end_time_index_dict.get(row.index_type), table_end_time_index_dict2.get(row.index_type)])
                        and
                        (start_col in [table_start_time_index_dict.get(row.index_type), table_start_time_index_dict2.get(row.index_type)])
                    )
                )
                or
                forced
            ):
            # if (row.table != row.index_type) and (row.table not in ['payer_plan_period', 'cost']):
            # filters.append(f'{row.table_abbrev}.{row.index_type}_id = {row.subject_id_source_table_abbrev}.{row.index_type}_id')
            index_bounded: bool = True
        else:
            # filters.append(f'{row.table_abbrev}.person_id = {row.subject_id_source_table_abbrev}.person_id')
            index_bounded: bool = False
    
        # add temporal component
        if (
                (
                    (start_col == 'unbounded')
                    and
                    (end_col == 'unbounded')
                )
                or
                (row.cdm_table == 'person')
                or
                (
                    (
                        (
                            (end_col in [table_end_time_index_dict.get(row.index_type), table_end_time_index_dict2.get(row.index_type)])
                            and
                            ((end_time_delta == end_col) or pd.isnull(end_time_delta))
                         )
                        and
                        (
                            (start_col in [table_start_time_index_dict.get(row.index_type), table_start_time_index_dict2.get(row.index_type)])
                            and
                            ((start_time_delta == start_col) or (pd.isnull(start_time_delta)))
                        )
                    )
                    and
                    (not time_index_mode)
                )
                or
                forced
            ):
            pass   # No additional filters needed, already filtered to index
    
        elif (start_col in ['unbounded']):
            filters.append(f'{row.cdm_table_abbrev}.{tbl_time_index} <= {upper_filter}')
    
        elif (end_col in ['unbounded']):
            filters.append(f'{row.cdm_table_abbrev}.{tbl_time_index} >= {lower_filter}')
    
        elif ((row.cdm_table.lower() not in ['location', 'cost', 'provider'])
              and
              (not ((row.subject_id == f'{row.index_type}_id') and (row.cdm_table.lower() == 'visit_occurrence')))):
            filters.append(f'{row.cdm_table_abbrev}.{tbl_time_index} BETWEEN {lower_filter} AND {upper_filter}')
    
        # add variable filter
        if pd.notnull(row.concept_class_id):
            if isinstance(row.drug_lookup_col, str):
                filters.append(f'{row.drug_lookup_col} IN ({row.variables})')
            else:
                filters.append(f'variable_name IN ({row.variables})')
        elif isinstance(row.variables, str) and (row.lookup_join_type == 'INNER'):
            filters.append(f'variable_name IN ({row.variables})')
    
        if isinstance(row.additional_where_filter, str):
            filters.append(f'({row.additional_where_filter})')
    
        if isinstance(row.drug_route_filter, str):
            criteria: str = "'" + "', '".join([x.strip() for x in row.drug_route_filter.split(',')]) + "'"
    
            filters.append(f'{table_abbrev_dict.get(row.cdm_table)}.route_concept_id IN (SELECT DISTINCT concept_id FROM {engine_bundle.lookup_schema}.{engine_bundle.lookup_table} WHERE variable_name IN ({criteria}))')
    
        return pd.Series({'filters': (filters[0]) if len(filters) == 1 else ('(\n\t' + '\n\tAND\n\t'.join(filters) + '\n\t)') if len(filters) > 1 else None,
                          'index_bound': True if forced else index_bounded,
                          'start_col': None if forced else start_col,
                         'end_col': None if forced else end_col})
    except Exception as e:
        row.to_pickle('problem_row_solve_filter.pkl')
        raise Exception(e)
        row = pd.read_pickle('problem_row_solve_filter.pkl')


def retrieve_audit_data(dfg: pd.DataFrame, engine_bundle: omop_engine_bundle, dir_dict: dict, project_name: str,
                        quick_audit_n: Union[int, None] = None,
                        save_query: bool = False,
                        use_visit_occurrence_parent_information: bool = True,
                        limit_to_cohorts: Union[str, None] = None):

    table: str = dfg.cdm_table.iloc[0]

    sel_line: str = f'SELECT TOP {quick_audit_n}' if isinstance(quick_audit_n, int) else 'SELECT'
    
    # use the cohort definition table to determine what the subject id type is (this should be done already in another function, you can replicate it)
    # Select all of the unique patients from the relevant table using the person id
    # create a temporary table in the database in the operational schema something like "project_name_audit_person_list" with just person ids in it
    # inner join all of the audit queries on person id to that table (except for provider)
    # delete the temporary table when done
    if isinstance(limit_to_cohorts, str):
        subject_id = get_subject_id(cohort_definition_id=int(limit_to_cohorts.split(',')[0].strip()), engine_bundle=engine_bundle)
        person_ids: pd.DataFrame = pd.read_sql(f'''SELECT 
                                                           DISTINCT person_id
                                                     FROM  
                                                          {engine_bundle.data_schema}.{subject_id[:-3]} s
                                                     INNER JOIN  
                                                          {engine_bundle.results_schema}.COHORT  C  on C.subject_id=s.{subject_id}
                                                     WHERE
                                                         C.cohort_definition_id IN ({limit_to_cohorts})''', con=engine_bundle.engine)
    
        person_ids.to_sql(name=f'{limit_to_cohorts.replace(",", "_")}_audit_person_list', schema=engine_bundle.operational_schema, if_exists='replace', con=engine_bundle.engine, index=False)
        filter_persons: bool = True
    else:
        filter_persons: bool = False

    t = dfg.drop_duplicates(subset=['cdm_field_name', 'variable_name']).query(f'~cdm_field_name.isin({(list(table_time_index_dict.values()) + list(table_start_time_index_dict.values()) + list(table_end_time_index_dict.values()))})', engine='python')
    if dfg.cdm_table.isin(['procedure_occurrence', 'drug_exposure', 'device_exposure', 'measurement', 'observation', 'condition_era', 'drug_era', 'dose_era', 'condition_occurrence']).all():
        for _, row in dfg.query('~cdm_field_name.isin(["unit_concept_id"])', engine='python').iterrows():
            variable: str = coalesce(row.result_field_name, row.variable_name, row.cdm_field_name)
            fn: str = f'{row.cdm_table}__{row.cdm_field_name}__{variable}'
            success_path: str = os.path.join(dir_dict.get('stat_files'), f'{fn}__success')
            lookup_left_jn: str = f"LEFT JOIN {engine_bundle.lookup_schema}.{engine_bundle.lookup_table} Lc on Lc.concept_id = {row.cdm_table_abbrev}.value_as_concept_id" if row.field_name == 'value_as_concept_id' else ''

            lookup_join_col: str = 'modifier_concept_id' if row.variable_name == 'primary_procedure' else f'{row.cdm_table_abbrev}.{row.cdm_table.replace("_occurrence", "").replace("_exposure", "")}_concept_id'
            
            if pd.read_sql(f"SELECT TOP 1 * FROM {engine_bundle.lookup_schema}.{engine_bundle.lookup_table} where variable_name = '{row.variable_name}'", con=engine_bundle.engine).shape[0] == 1:
                lookup_table_join: str = f'''INNER JOIN {engine_bundle.lookup_schema}.{engine_bundle.lookup_table} L on (L.concept_id = {lookup_join_col} AND L.variable_name = '{row.variable_name}')'''
            elif pd.read_sql(f"SELECT TOP 1 * FROM {engine_bundle.lookup_schema}.{engine_bundle.drug_lookup_table} where ingredient_category = '{row.variable_name}'", con=engine_bundle.engine).shape[0] == 1:
                lookup_table_join: str = f'''INNER JOIN (SELECT DISTINCT ingredient_category [variable_name], drug_concept_id FROM {engine_bundle.lookup_schema}.{engine_bundle.ingredient_category} L on (L.drug_concept_id = {lookup_join_col} AND L.ingredient_category = '{row.variable_name}')) L'''
            else:
                lookup_table_join: str = ''
                
            vo_col: str = 'vo.parent_visit_occurrence_id [visit_occurrence_id]' if use_visit_occurrence_parent_information else 'visit_occurrence_id'
            vo_join: str = f'INNER JOIN {engine_bundle.data_schema}.VISIT_OCCURRENCE vo on {row.cdm_table_abbrev}.visit_occurrence_id = vo.visit_occurrence_id' if use_visit_occurrence_parent_information else ''

            if not os.path.exists(success_path):
                person_query: str = f'''WHERE {row.cdm_table_abbrev}.person_id IN (SELECT person_id FROM {engine_bundle.operational_schema}.[{limit_to_cohorts.replace(",", "_")}_audit_person_list])''' if filter_persons else ''
                qry: str = f'''{sel_line}
                                                {row.cdm_table_abbrev}.person_id,
                                                {vo_col},
                                                {row.cdm_field_name} [{variable}]
                                                {',unit_concept_id' if row.cdm_field_name == 'value_as_number' else ',lc.concept_id [{variable}_variable_name]' if lookup_left_jn != '' else ''}
                                            FROM
                                                {engine_bundle.data_schema}.{row.cdm_table} {row.cdm_table_abbrev}
                                                {vo_join}
                                                {lookup_table_join}
                                                {lookup_left_jn}
                                            {person_query};'''
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
            person_query: str = f'''WHERE {table_abbrev_dict.get(table)}.person_id IN (SELECT person_id FROM {engine_bundle.operational_schema}.[{limit_to_cohorts.replace(",", "_")}_audit_person_list])''' if (filter_persons
                                                                                                                                                                                   and
                                                                                                                                                                                   table not in ['location',
                                                                                                                                                                                                 'care_site',
                                                                                                                                                                                                 'cost']) else ''
            qry: str = f'''{sel_line}
                                            {'person_id,' if table not in ['location', 'care_site', 'cost'] else ''}
                                            {'visit_occurrence_id,' if table not in ['payer_plan_period', 'location', 'care_site', 'cost', 'death', 'person'] else ''}
                                            {','.join(raw_fields)}{',' if len(concept_id_fields) > 0 else ''}
                                            {','.join([('L_' + x + '.concept_id [' + x + '_variable_name]') for x in concept_id_fields])}
                                        FROM
                                            {engine_bundle.data_schema}.{table} {table_abbrev_dict.get(table)}
                                            {lookup_joins}
                                        {person_query}'''

            if save_query:
                save_data(qry, os.path.join(dir_dict.get('audit_source'), f'{fn}.sql'))

            save_data(check_load_df(qry,
                                    engine=engine_bundle.engine,
                                    chunksize=1000),
                      out_path=os.path.join(dir_dict.get('audit_source'), f'{fn}.csv'))
            open(success_path, mode='a').close()

    if filter_persons:
        execute_query_in_transaction(query=f'DROP TABLE {engine_bundle.operational_schema}.[{limit_to_cohorts.replace(",", "_")}_audit_person_list]', engine=engine_bundle.engine)

def analyze_audit_data(dir_dict: dict, raw_df: pd.DataFrame, limit_to_cohorts: Union[str, None], engine_bundle: omop_engine_bundle):

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
        
        if isinstance(limit_to_cohorts, str):
            subject_id = get_subject_id(cohort_definition_id=int(limit_to_cohorts.split(',')[0].strip()), engine_bundle=engine_bundle)
            person_ids: pd.DataFrame = pd.read_sql(f'''SELECT 
                                                               DISTINCT person_id
                                                         FROM  
                                                              {engine_bundle.data_schema}.{subject_id[:-3]} s
                                                         INNER JOIN  
                                                              {engine_bundle.results_schema}.COHORT  C  on C.subject_id=s.{subject_id}
                                                         WHERE
                                                             C.cohort_definition_id IN ({limit_to_cohorts})''', con=engine_bundle.engine)
        
            person_ids.to_sql(name=f'{limit_to_cohorts.replace(",", "_")}_audit_person_list', schema=engine_bundle.operational_schema, if_exists='replace', con=engine_bundle.engine, index=False)
            
            total_persons = pd.read_sql(f'SELECT COUNT(person_id) FROM {engine_bundle.operational_schema}.[{limit_to_cohorts.replace(",", "_")}_audit_person_list]', con=engine_bundle.engine).iloc[0, 0]

            total_visits = pd.read_sql(f'''SELECT 
                                               COUNT(visit_occurrence_id)
                                           FROM
                                               {engine_bundle.data_schema}.VISIT_OCCURRENCE
                                           WHERE
                                               person_id IN (SELECT
                                                                 person_id
                                                             FROM
                                                                 {engine_bundle.operational_schema}.[{limit_to_cohorts.replace(",", "_")}_audit_person_list]);''', con=engine_bundle.engine).iloc[0, 0]
            execute_query_in_transaction(query=f'DROP TABLE {engine_bundle.operational_schema}.[{limit_to_cohorts.replace(",", "_")}_audit_person_list]', engine=engine_bundle.engine)
        else:

            total_persons = pd.read_sql(f'SELECT COUNT(person_id) FROM {engine_bundle.data_schema}.PERSON', con=engine_bundle.engine).iloc[0, 0]
    
            total_visits = pd.read_sql(f'SELECT COUNT(visit_occurrence_id) FROM {engine_bundle.data_schema}.VISIT_OCCURRENCE', con=engine_bundle.engine).iloc[0, 0]
       
        final_report.insert(loc=5, column='percent_visits_observed', value=(final_report.visit_count.astype(int) / total_visits).apply(lambda x: float(f"{x:.2g}")))
        final_report.insert(loc=6, column='percent_persons_observed', value=(final_report.person_count.astype(int) / total_persons).apply(lambda x: float(f"{x:.2g}")))
        
        apari_model_master_variable = check_load_df(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), 'Resource_Files', 'APARI_model_master_variable.xlsx'))
        apari_model_master_variable['pass_threshold'] = pd.to_numeric(apari_model_master_variable['pass_threshold'], errors='coerce')
        apari_model_master_variable['fail_threshold'] = pd.to_numeric(apari_model_master_variable['fail_threshold'], errors='coerce')

        final_report = final_report.merge(
           apari_model_master_variable,
            on=['cdm_table', 'cdm_field_name', 'variable_name'],
            how='inner'
        ).sort_values(by=['cdm_table',  'variable_name',]).reset_index(drop=True)
        

        final_report.insert(loc=7,column='adjudication',value=final_report.apply(
                lambda row: 'PASS' if row['percent_persons_observed'] >= row['pass_threshold']
                else 'FAIL' if row['percent_persons_observed'] < row['fail_threshold']
                else 'WARN',
                axis=1
            )
        )

        final_report['person_count'] = final_report['person_count'].apply(lambda x: f'{int(x):,}')
        final_report['visit_count'] = final_report['visit_count'].apply(lambda x: f'{int(x):,}')
        #filter the final report  includes only variables that are part of APARI model features or patient outcome 
       

        save_data(final_report.drop_duplicates(), out_path=os.path.join(dir_dict.get('Audit'), 'final_report.xlsx'))

        open(report_success_fp, 'a').close()


def build_data_queries(df: pd.DataFrame, engine_bundle: omop_engine_bundle, cdm_version: str,
                       time_index_sub_visit_precision: str, time_index_visit_precision: str,
                       time_index_mode: bool, query_save_folder: str = None,
                       use_visit_occurrence_parent_information: bool = False) -> pd.DataFrame:

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
    template = template.groupby(grpby_cols, group_keys=False).agg({**{'variable_name': _join_vars,
                                                                      'field_name': _join_fields,
                                                                      'row_id': _join_row_ids,
                                                                      'concept_class_id' : _join_concept_class_ids},
                                                 **{x: 'first' for x in ['subject_id', 'subject_id_source_table', 'subject_id_source_table_abbrev',
                                                                         'cdm_table_abbrev', 'use_subset']}})\
        .reset_index(drop=False)\
        .replace({'-999': None})\
        .rename(columns={'field_name': 'fields', 'variable_name': 'variables', 'row_id': 'row_ids'})

    # solve the where filters for each group
    template[['filters', 'index_bound', 'start_col', 'end_col']] = template.apply(_solve_filter, time_index_mode=time_index_mode, engine_bundle=engine_bundle,
                                                                                  time_index_sub_visit_precision=time_index_sub_visit_precision, time_index_visit_precision=time_index_visit_precision, axis=1)

    grpby2_cols: List[str] = ['cdm_table', 'partition_seq', 'lab_test', 'aggregation_function', 'index_bound', 'index_type',
                              'start_col', 'end_col', 'start_reference_point_time_delta', 'end_reference_point_time_delta',
                              'partition_visit_detail_filter', 'lookup_join_type', 'drug_lookup_col', 'additional_join_filter',]

    # fill nulls with -999 to prevent groupby errors
    template[grpby2_cols] = template[grpby2_cols].fillna('-999')
    
    # build the queries
    queries = template.copy(deep=True).groupby(grpby2_cols, as_index=False, group_keys=False).apply(build_query_group, engine_bundle=engine_bundle,
                                                                                    cdm_version=cdm_version, time_index_mode=time_index_mode,
                                                                                    use_visit_occurrence_parent_information=use_visit_occurrence_parent_information,
                                                                                    time_index_sub_visit_precision=time_index_sub_visit_precision, time_index_visit_precision=time_index_visit_precision)\
        .replace({'-999': None}).rename(columns={None: 'query'})

    # determine file name based on settings
    queries['file_name'] = queries.apply(_solve_file_name, axis=1)

    queries['row_ids'] = queries.row_ids.apply(lambda x: x.split('||'))
    
    
    try:
        assert queries.file_name.nunique() == queries.shape[0], f'There are repeating file names detected. Please resolve issue before proceeding. affected names: {queries.file_name[queries.file_name.duplicated()]}'
    except Exception as e:
        queries.to_pickle(os.path.join(query_save_folder, 'query_name_problems.pkl'))
        raise Exception(e)
        

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
        if row.cdm_table == 'measurement':
            fn: str = 'labs'
        else:
            fn: str = f'{row.cdm_table}_labs'
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


def _join_concept_class_ids(s: pd.Series) -> str:
    if s.notnull().any():
        return ','.join(sorted(list(set(deduplicate_and_join(input_s=s,
                                    sep=',',
                                    leading="",
                                    trailing="").split(',')))))


def build_query_group(dfg: pd.DataFrame, engine_bundle: omop_engine_bundle, cdm_version: str, use_visit_occurrence_parent_information: bool, time_index_mode: bool,
                      time_index_sub_visit_precision: str, time_index_visit_precision: str,) -> str:
    # dfg = template.loc[[20], :].copy(deep=True)
    dfg.replace({'-999': None}, inplace=True)

    if dfg.shape[0] == 1:
        try:
            return build_query(dfg.iloc[0, :].copy(deep=True), engine_bundle=engine_bundle, cdm_version=cdm_version,
                               use_visit_occurrence_parent_information=use_visit_occurrence_parent_information,
                               time_index_mode=time_index_mode, time_index_sub_visit_precision=time_index_sub_visit_precision, time_index_visit_precision=time_index_visit_precision)
        except Exception as e:
            dfg.to_pickle('dfg.pkl')
            print(e)
            raise Exception('stop here')
            dfg = pd.read_pickle('dfg.pkl')
            row = dfg.iloc[0, :].copy(deep=True)
    elif (dfg.cdm_table == 'location').all():
        return dfg.apply(build_query, engine_bundle=engine_bundle, cdm_version=cdm_version, 
                         use_visit_occurrence_parent_information=use_visit_occurrence_parent_information,
                         time_index_mode=time_index_mode, time_index_sub_visit_precision=time_index_sub_visit_precision,
                         time_index_visit_precision=time_index_visit_precision, axis=1)
    else:

        dfg = dfg.groupby(['cdm_table'], group_keys=False).agg({**{'index_type': _index_agg,
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
            return build_query(dfg.iloc[0, :].copy(deep=True), engine_bundle=engine_bundle, cdm_version=cdm_version,
                               use_visit_occurrence_parent_information=use_visit_occurrence_parent_information,
                               time_index_mode=time_index_mode, time_index_sub_visit_precision=time_index_sub_visit_precision,
                               time_index_visit_precision=time_index_visit_precision)
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


def _solve_field_join(row: pd.Series, use_visit_occurrence_parent_information: bool,  time_index_mode: bool, include_time_filters: bool = True, return_time_filter: bool = False) -> str:
    join_id: str = f'{row.index_type}_id' if (row.index_bound and not time_index_mode) else 'person_id'
    
    if use_visit_occurrence_parent_information and (join_id.lower() == 'visit_occurrence_id'):
        left_table_abbrev: str = table_abbrev_dict.get('visit_occurrence')
    else:
        left_table_abbrev: str = row.subject_id_source_table_abbrev

    if row.cdm_table == 'cost':
        out: str = f'''{row.cdm_table_abbrev}.cost_event_id = {left_table_abbrev}.{join_id} AND cost_domain_id = '{row.index_type.replace('_', ' ')}' '''
    elif row.cdm_table == 'provider':
        out: str = f'{row.cdm_table_abbrev}.provider_id = {table_abbrev_dict.get(row.index_type)}.provider_id'
    else:
        out: str = f'{row.cdm_table_abbrev}.{join_id} = {left_table_abbrev}.{join_id}'
        
    if include_time_filters:
        time_filters: List[str] = list(set([x.group() for x in re.finditer(r'(DATEADD\([A-Z]+,\s[0-9\-\+]+,\s[A-z_0-9.]+\)|[A-z_0-9.]+)\s(BETWEEN\s(DATEADD\([A-Z]+,\s[0-9\-\+]+,\s[A-z_0-9.]+\)|[A-z_0-9.]+)\sAND|<=|>=|<|>)\s(DATEADD\([A-Z]+,\s[0-9\-\+]+,\s[A-z_0-9.]+\)|[A-z_0-9.]+)', str(row.filters))]))
        
        if len(time_filters) == 1:
            out: str = f'({out}) AND ({time_filters[0]})'
        elif len(time_filters) > 1:
            print(row)
            print(time_filters)
            raise Exception('stop here')

    # if pd.notnull(row.filters):
        
    #     for res in re.finditer(r'[\s\S]+\s--XXX', row.filters):
    #         t = row.filters.split('--XXX')
    #         if len(t) > 1:
    #             t = t[:-1]
    #         if all([re.search(r'{}\.[A-z_]+date'.format(row.cdm_table_abbrev), x) for x in t]):
    #             out: str = f'({out}) AND (row.filters)'
           

    if isinstance(row.additional_join_filter, str):
        out: str = f'({out}) AND ({row.additional_join_filter})'

    if include_time_filters and return_time_filter:
        return f'({out})', time_filters
    return f'({out})'


def build_query(row: pd.Series, engine_bundle: omop_engine_bundle, cdm_version: str, use_visit_occurrence_parent_information: bool, time_index_mode: bool,
                time_index_sub_visit_precision: str, time_index_visit_precision: str) -> str:
    # if (row.cdm_table == 'observation') and (row.lookup_join_type == 'INNER') and ('procedure_urgency' in str(row.variables)) and pd.isnull(row.partition_visit_detail_filter):
    #     debug_inputs(function=build_query, kwargs=locals(), dump_fp='build_query_deubg.pkl')
    #     raise Exception('stop here')
    #     import pickle
    #     locals().update(pickle.load(open('build_query_deubg.pkl', 'rb')))
    try:
        force_mode: bool = '_force' in row.index_type
        if force_mode:
            time_index_mode: bool = False
        row.index_type = row.index_type.replace('_force', '')
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
        
        if use_visit_occurrence_parent_information and (row.subject_id == 'visit_occurrence_id') and (row.cdm_table == 'visit_occurrence'):
            id_fields.append(f'{table_abbrev_dict.get("visit_occurrence")}.parent_visit_occurrence_id [visit_occurrence_id]')

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
        if use_visit_occurrence_parent_information and (row.subject_id.lower() == 'visit_occurrence_id') and row.index_bound and (not time_index_mode):
            joins += [f'INNER JOIN {engine_bundle.data_schema}.{row.subject_id_source_table} {row.subject_id_source_table_abbrev} on {row.subject_id_source_table_abbrev}.parent_{row.subject_id} = C.subject_id']
        else:
            joins += [f'INNER JOIN {engine_bundle.data_schema}.{row.subject_id_source_table} {row.subject_id_source_table_abbrev} on {row.subject_id_source_table_abbrev}.{row.subject_id} = C.subject_id']

        start_idx_table: List[str] = [k for k, v in table_start_time_index_dict.items() if v == row.start_reference_point] + [k for k, v in table_start_time_index_dict2.items() if v == row.start_reference_point] + [k for k, v in table_end_time_index_dict.items() if v == row.start_reference_point] + [k for k, v in table_end_time_index_dict2.items() if v == row.start_reference_point]
        assert (len(start_idx_table) == 1) or (row.start_reference_point == 'unbounded'), f'Unable to reconcile start_reference_point: {row.start_reference_point} with OMOP CDM Tables'
        start_idx_table: str = start_idx_table[0] if (len(start_idx_table) == 1) else []

        # add index table if different then subject_id table and time before/after index is populated (if just the person/visit_occurrence id is needed, there is no need to join an additional table at this point, unless the parent_visit_detail_id is needed)
        if (row.index_type != row.subject_id_source_table) and (row[['start_reference_point_time_delta', 'end_reference_point_time_delta']].notnull().any() or
                                                                (row.cdm_table == row.index_type) or
                                                                (((row.end_reference_point in [table_start_time_index_dict.get(row.index_type), table_end_time_index_dict.get(row.index_type),
                                                                                               table_start_time_index_dict2.get(row.index_type), table_end_time_index_dict2.get(row.index_type)]) or
                                                                  (row.start_reference_point in [table_start_time_index_dict.get(row.index_type), table_end_time_index_dict.get(row.index_type),
                                                                                                 table_start_time_index_dict2.get(row.index_type), table_end_time_index_dict2.get(row.index_type)])) and (not row.index_bound))
                                                                or (use_visit_occurrence_parent_information and (row.subject_id.lower() != 'visit_occurrence_id') and (row.index_type.lower() == 'visit_occurrence'))):

            if use_visit_occurrence_parent_information and (row.index_type.lower() == 'visit_occurrence') :
                joins += [f'INNER JOIN {engine_bundle.data_schema}.{row.index_type} {table_abbrev_dict.get(row.index_type)}p on {table_abbrev_dict.get(row.index_type)}p.{row.index_type}_id = {row.subject_id_source_table_abbrev}.{row.index_type}_id']
                if ((row.index_bound and (not time_index_mode)) or (row.cdm_table == 'cost')): # use the children for the lookup by visit occurrence id
                    joins += [f'INNER JOIN {engine_bundle.data_schema}.{row.index_type} {table_abbrev_dict.get(row.index_type)} on {table_abbrev_dict.get(row.index_type)}.parent_{row.index_type}_id = {table_abbrev_dict.get(row.index_type)}p.parent_{row.index_type}_id']
                else: # get the start/end reference times from the parent
                    joins += [f'INNER JOIN {engine_bundle.data_schema}.{row.index_type} {table_abbrev_dict.get(row.index_type)} on {table_abbrev_dict.get(row.index_type)}.{row.index_type}_id = {table_abbrev_dict.get(row.index_type)}p.parent_{row.index_type}_id']
            else:
                joins += [f'INNER JOIN {engine_bundle.data_schema}.{row.index_type} {table_abbrev_dict.get(row.index_type)} on {table_abbrev_dict.get(row.index_type)}.{row.index_type}_id = {row.subject_id_source_table_abbrev}.{row.index_type}_id']

        # join visit_detail or visit_occurrence table if needed in order to handle temporal filter
        if ((row.start_reference_point not in [table_start_time_index_dict.get(row.subject_id_source_table), table_end_time_index_dict.get(row.subject_id_source_table),
                                               table_start_time_index_dict2.get(row.subject_id_source_table), table_end_time_index_dict2.get(row.subject_id_source_table)])
            and (row.start_reference_point not in [table_start_time_index_dict.get(row.index_type), table_end_time_index_dict.get(row.index_type),
                                                   table_start_time_index_dict2.get(row.index_type), table_end_time_index_dict2.get(row.index_type)])
                and (start_idx_table in ['visit_detail', 'visit_occurrence'])):
            
            # check that the table wasn't already joined for the parent lookup
            if not any([re.search(r'INNER\sJOIN\s{}\.{}\s{}\son'.format(engine_bundle.data_schema, start_idx_table, table_abbrev_dict.get(start_idx_table)), x) for x in joins]):
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
            
            # check that the table wasn't already joined for the parent lookup
            if not any([re.search(r'INNER\sJOIN\s{}\.{}\s{}\son'.format(engine_bundle.data_schema, end_idx_table, table_abbrev_dict.get(end_idx_table)), x) for x in joins]):
                joins += [f'INNER JOIN {engine_bundle.data_schema}.{end_idx_table} {table_abbrev_dict.get(end_idx_table)} on {table_abbrev_dict.get(end_idx_table)}.{end_idx_table}_id = {row.subject_id_source_table_abbrev}.{end_idx_table}_id']

        # join source table if not joined already
        # join the field table
        time_filter: List[str] = []
        if row.cdm_table in ['location', 'payer_plan_period']:
            if row.index_type != row.subject_id_source_table:
                # check that the table wasn't already joined for the parent lookup
                if not any([re.search(r'INNER\sJOIN\s{}\.{}\s{}\son'.format(engine_bundle.data_schema, row.index_type, table_abbrev_dict.get(row.index_type)), x) for x in joins]):
                    joins += [f'INNER JOIN {engine_bundle.data_schema}.{row.index_type} {table_abbrev_dict.get(row.index_type)} on {table_abbrev_dict.get(row.index_type)}.{row.index_type}_id = {row.subject_id_source_table_abbrev}.{row.index_type}_id']

            if row.cdm_table == 'location':
                if row.index_type == 'person':
                    if pd.read_sql(f'''SELECT COLUMN_NAME FROM {engine_bundle.database}.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'LOCATION_HISTORY' AND TABLE_SCHEMA = '{engine_bundle.data_schema.split(".")[-1]}';''', con=engine_bundle.engine).shape[0] > 0:
                        joins += [f'''LEFT JOIN {engine_bundle.data_schema}.LOCATION_HISTORY {table_abbrev_dict.get("location_history")} on ({table_abbrev_dict.get("location_history")}.entity_id = {row.subject_id_source_table_abbrev}.person_id AND {table_abbrev_dict.get("location_history")}.entity_domain = 'person' AND CONVERT(DATE, {table_start_time_index_dict.get(row.subject_id_source_table)}) BETWEEN  {table_abbrev_dict.get("location_history")}.start_date AND {table_abbrev_dict.get("location_history")}.end_date)''',
                                  f'LEFT JOIN {engine_bundle.data_schema}.LOCATION l2 on l2.location_id = {table_abbrev_dict.get("location_history")}.location_id']

                        comments += [f'The patients Home Location from Location History at {table_start_time_index_dict.get(row.subject_id_source_table)} was used if available, otherwise the patients location from the person table was used']

                    joins += [f'LEFT JOIN {engine_bundle.data_schema}.LOCATION {table_abbrev_dict.get("location")} on {table_abbrev_dict.get("location")}.location_id = {table_abbrev_dict.get(row.index_type)}.location_id']
                elif row.index_type in ['visit_occurrence', 'visit_detail']:
                    joins += [f'INNER JOIN {engine_bundle.data_schema}.CARE_SITE {table_abbrev_dict.get("care_site")} on {table_abbrev_dict.get("care_site")}.care_site_id = {table_abbrev_dict.get(row.index_type)}.care_site_id',
                              f'INNER JOIN {engine_bundle.data_schema}.LOCATION {table_abbrev_dict.get("location")} on {table_abbrev_dict.get("location")}.location_id = {table_abbrev_dict.get("care_site")}.location_id']

            elif row.cdm_table == 'payer_plan_period':
                joins += [f'INNER JOIN {engine_bundle.data_schema}.{row.cdm_table} {row.cdm_table_abbrev} on (ppp.person_id = {table_abbrev_dict.get(row.index_type)}.person_id AND CONVERT(DATE, {table_abbrev_dict.get(row.index_type)}.{table_start_time_index_dict.get(row.index_type)}) BETWEEN ppp.[payer_plan_period_start_date] AND ppp.[payer_plan_period_end_date])']

        elif isinstance(row.partition_visit_detail_filter, str):
            fd_join, time_filter = _solve_field_join(row, use_visit_occurrence_parent_information, time_index_mode, return_time_filter=True)
            joins += [f'INNER JOIN {engine_bundle.data_schema}.VISIT_DETAIL vdp on {fd_join.replace("vd2", "vdp")}',
                      f"INNER JOIN {engine_bundle.lookup_schema}.{engine_bundle.lookup_table} L on (L.concept_id = vdp.visit_detail_concept_id AND L.variable_name = '{row.partition_visit_detail_filter}')"]
            # joins += [f'INNER JOIN {engine_bundle.data_schema}.VISIT_DETAIL vdp on vdp.{row.index_type}_id = {table_abbrev_dict.get("visit_occurrence")}.visit_occurrence_id',
            #           f"INNER JOIN {engine_bundle.lookup_schema}.{engine_bundle.lookup_table} L on (L.concept_id = vdp.visit_detail_concept_id AND L.variable_name = '{row.partition_visit_detail_filter}')"]
            partioned_joins: List[str] = [f'INNER JOIN {engine_bundle.data_schema}.{row.cdm_table} {row.cdm_table_abbrev} on {row.cdm_table_abbrev}.visit_detail_id = partitioned.visit_detail_id']

        elif (row.cdm_table not in [row.subject_id_source_table, row.index_type]) or ('2' in row.cdm_table_abbrev):
            fd_join, time_filter = _solve_field_join(row, use_visit_occurrence_parent_information, time_index_mode, return_time_filter=True)
            joins += [f'INNER JOIN {engine_bundle.data_schema}.{row.cdm_table} {row.cdm_table_abbrev} on {_solve_field_join(row, use_visit_occurrence_parent_information, time_index_mode)}']

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
                
            if len(time_filter) > 0:
                for tf in time_filter:
                    row['filters'] = re.sub(r'\({}\)|AND\s+{}|{}\s+AND|{}'.format(tf.replace('(', r'\(').replace(')', r'\)'), tf.replace('(', r'\(').replace(')', r'\)'),
                                                                                tf.replace('(', r'\(').replace(')', r'\)'), tf.replace('(', r'\(').replace(')', r'\)')), '', row['filters'])
                    
            # check for empty filters
            row['filters'] = re.sub(r'\s+\(\s+\)\s--XXX[0-9]+XXX\s+(OR|AND)|(OR|AND)\s+\(\s+\)\s--XXX[0-9]+XXX', '', row.filters)
                    
            if len(row.filters) == 0:
                row['filters'] = None

        criteria: List[str] = ([row.filters] if (isinstance(row.filters, str) and (not bool(re.search(r'^{} IN \([A-z_\', ]+\)$'.format(coalesce(row.drug_lookup_col, 'variable_name')), str(row.filters))))) else [])
        cohort_criteria: List[str] = ['C.cohort_definition_id = XXXXXX']

        # add subset filter
        if row.use_subset:
            cohort_criteria += ['C.subset_id = YYYY']

        # add count to aggregation functions (besides count, min, and max) in order to allow reconcilliation of numbers with different units
        if isinstance(row.aggregation_function, str) and (str(row.aggregation_function).lower() not in ['min', 'max', 'count']):
            fields += [x.replace(f'{row.aggregation_function}(', 'COUNT(')[:-1] + '_count]' for x in fields if row.aggregation_function in x]

        def _parent_visit_occurrence_check(input_L: List[str]) -> List[str]:
            cols_with_parent: str = r'(visit_occurrence_id|admitted_from_concept_id|admitted_from_source_value|discharged_to_concept_id|discharged_to_source_value|visit_start_datetime|visit_end_datetime)'
            # cols_wth_parent_specific: str = [r'[A-z_0-9]+\.{}'.format(x) for x in cols_with_parent]
            if use_visit_occurrence_parent_information:
                out: List[str] = []
                for x in input_L:
                    sob: Union[None, re.Match] = re.search(r'[A-z_0-9]+\.{}'.format(cols_with_parent), x)
                    if isinstance(sob, re.Match):
                        if (sob.groups(0)[0] not in ['visit_occurrence_id']) and (table_abbrev_dict.get('visit_detail') in x.split('.')[0]):
                            out.append(x) # these columns also exist in the visit detail table and there source values should be maintained
                        else:
                            out.append(re.sub(r'[A-z_0-9]+\.{}'.format(cols_with_parent) , f'vo.parent_{sob.groups(0)[0]}' + (f' [{sob.groups(0)[0]}]' if ('[' not in x) else ''), x))
                    else:
                        out.append(x)
                return out
            return input_L
        
        # handle instance where parent information is desired
        if use_visit_occurrence_parent_information:
            for table in set([re.search(r'([A-z_0-9]+)\.visit_occurrence_id', x).groups(0)[0] for x in (id_fields + fields) if re.search(r'([A-z_0-9]+)\.visit_occurrence_id', x)]):
                if isinstance(row.partition_visit_detail_filter, str):
                    partioned_joins.append(f'INNER JOIN {engine_bundle.data_schema}.VISIT_OCCURRENCE {table_abbrev_dict.get("visit_occurrence")}p on {table}.visit_occurrence_id = {table_abbrev_dict.get("visit_occurrence")}p.visit_occurrence_id')
                    partioned_joins.append(f'INNER JOIN {engine_bundle.data_schema}.VISIT_OCCURRENCE {table_abbrev_dict.get("visit_occurrence")} on {table_abbrev_dict.get("visit_occurrence")}.visit_occurrence_id = {table_abbrev_dict.get("visit_occurrence")}p.parent_visit_occurrence_id')
                    
                  
                if not any([re.search(r'\.VISIT_OCCURRENCE {}'.format(table_abbrev_dict.get("visit_occurrence")), x, re.IGNORECASE) for x in joins]):
                    joins.append(f'INNER JOIN {engine_bundle.data_schema}.VISIT_OCCURRENCE {table_abbrev_dict.get("visit_occurrence")} on {table}.visit_occurrence_id = {table_abbrev_dict.get("visit_occurrence")}.visit_occurrence_id')
                
            id_fields: List[str] = _parent_visit_occurrence_check(id_fields)
            fields: List[str] = _parent_visit_occurrence_check(fields)

        ### compile Query ###
        field_statement: str = '\n\t' + ',\n\t'.join(id_fields + fields) + '\n'
        join_statement: str = '\n\t' + '\n\t'.join(joins) + '\n'
        where_statement: str = '\n\t' + '\n\tAND\n\t'.join(criteria if isinstance(row.partition_visit_detail_filter, str) else (criteria + cohort_criteria))

        # handle partion by index and order by to get first, last, 2nd , etc.
        if isinstance(row.partition_seq, (int, float)):
            part_index: str = 'vo.parent_visit_occurrence_id' if use_visit_occurrence_parent_information and (row.index_type.lower() == 'visit_occurrence') else f'{row.subject_id_source_table_abbrev}.{row.index_type}_id'
            if isinstance(row.partition_visit_detail_filter, str):
                seq_str: str = ',\n\t' + f'ROW_NUMBER() OVER(PARTITION BY {part_index} ORDER BY {row.subject_id_source_table_abbrev}.visit_detail_start_datetime {"ASC" if row.partition_seq > 0 else "DESC"}) AS seq'

                partition_join_statement: str = '\n\t' + '\n\t'.join(partioned_joins) + '\n'
                partition_where_statement: str = '\n\t' + '\n\tAND\n\t'.join(cohort_criteria)
                where_statement: str = '\n\t' + '\n\tAND\n\t'.join(criteria + [f'seq = {int(abs(row.partition_seq))}'])

                query: str = 'with partitioned as (\n' + 'SELECT\n\tsubject_id,\n\tvdp.visit_detail_id{}\nFROM{}\nWHERE{})\nSELECT\n\n{}\nFROM\n\tpartitioned{}\nWHERE{};'.format(seq_str, join_statement, partition_where_statement, field_statement.replace('C.subject_id', 'subject_id'), partition_join_statement, where_statement)
            else:
                seq_str: str = ',\n\t' + f'ROW_NUMBER() OVER(PARTITION BY {part_index} {", {}".format(coalesce(row.drug_lookup_col, "variable_name")) if isinstance(row.concept_class_id, str) else ""} ORDER BY {row.cdm_table_abbrev}.{table_time_index_dict.get(row.cdm_table, table_start_time_index_dict.get(row.cdm_table))} {"ASC" if row.partition_seq > 0 else "DESC"}) AS seq'

                partition_field_statement: str = '\n\t' + ',\n\t'.join([(re.search(r'\[[A-z_0-9]+\]', x).group(0) if ']' in x else x) for x in sorted(list(set([re.sub(r'\.|'.join(list(table_abbrev_dict.values()) + [row.cdm_table_abbrev]) + r'\.', '', x) for x in (id_fields + fields)])))]) + '\n'

                query: str = 'with partitioned as (\n' + 'SELECT\n\t' + field_statement.strip() + seq_str + '\nFROM' + join_statement + '\nWHERE' + where_statement + '\n)\n' + f'SELECT {partition_field_statement} FROM partitioned WHERE seq = {int(abs(row.partition_seq))};'''
        else:
            query: str = f"""SELECT {field_statement} FROM {join_statement} WHERE {where_statement};"""

            if isinstance(row.aggregation_function, str):
                query: str = '{}\nGROUP BY{}'.format(query[:-1], '\n\t' + ',\n\t'.join([re.sub(r'\[[A-z_]+\]', '', x) for x in id_fields]) + '\n')

        # add commments to the query
        query: str = '/******\n{}\n******/\n{}'.format('\n\n'.join(comments), query)
        
        if use_visit_occurrence_parent_information:
            if bool(re.search(r'\.person_id\s=\svo\.person_id', query)):
                query = re.sub(r'visit_occurrence\svo\son\svo\.parent_visit_occurrence_id\s=\sC\.subject_id', 'visit_occurrence vo on vo.visit_occurrence_id = C.subject_id', query)
            for ptrn in [r'(\s|vo\.)visit_{}_datetime'.format(x) for x in ['start', 'end']]:
                query = re.sub(ptrn, ptrn.replace(r'(\s|vo\.)', ' vo.parent_'), query)
                
            for ptrn in [r'(\s|vo\.)visit_{}_date'.format(x) for x in ['start', 'end']]:
                query = re.sub(ptrn, ptrn.replace(r'(\s|vo\.)', ' CONVERT(DATE, vo.parent_') + 'time)', query)
                
            if 'visit_occurrence_from' in _solve_file_name(row):
                query = re.sub(r'vo\.parent_visit_occurrence_id\s\[visit_occurrence_id\],', 'vo.visit_occurrence_id [visit_occurrence_id],', query)
            

        if cdm_version == '5.3':
            for invalid_combo_str in invalid_cdm_v53_fields:
                invalid_so: Union[re.Match, None] = re.search(r'({})\s\[[A-z0-9_\-]+\],'.format(invalid_combo_str), query)

                if isinstance(invalid_so, re.Match):
                    if (invalid_combo_str == 'devE.unit_concept_id') and ('rbc_transfusion' in query):
                        query: str = query.replace(invalid_so.groups()[0], "CASE WHEN L.variable_name == 'rbc_transfusion' AND devE.quantity < 100 THEN 8510 WHEN L.variable_name == 'rbc_transfusion' AND devE.quantity >= 100 THEN  ELSE NULL")
                    else:
                        query: str = query.replace(invalid_so.group(), '')

        return pd.Series({'query': query, 'row_ids': row.row_ids})
    except Exception as e:
        row.to_pickle('row.pkl')
        raise Exception(e)
        row = pd.read_pickle('row.pkl')

if __name__ == '__main__':
    pass
    # import sys
    # sys.path.append(r'P:\GitHub')
    # import pandas as pd
    # import re
    # import os
    # from typing import List, Dict, Union
    # from datetime import datetime as dt
    # from Utilities.PreProcessing.data_format_and_manipulation import deduplicate_and_join, coalesce
    # from Utilities.FileHandling.io import save_data, check_load_df, find_files
    # from Utilities.PreProcessing.standardization_functions import process_df_v2
    # from Utilities.Database.connect_to_database import omop_engine_bundle
    # from Utilities.General.func_utils import debug_inputs
    # from Utilities.ProjectManagement.query_builder import validate_and_run_query_builder, _format_metadata, get_table_and_concept_class, _solve_file_name,\
    # table_start_time_index_dict, table_end_time_index_dict, table_start_time_index_dict2, table_end_time_index_dict2, table_abbrev_dict, _solve_field_join,\
    #     build_query, build_data_queries, table_time_index_dict, build_query_group, _join_vars, _join_fields,  _join_row_ids, _test_writer, _solve_filter, get_subject_id
    
    
    # from Utilities.Database.connect_to_database import omop_engine_bundle, get_SQL_database_connection_v2
    
    # __file__ = r"Z:\GitHub\Utilities\ProjectManagement\query_builder.py"
    

    # database: str = 'ic3_inpatient_pipeline_2024'
    # engine_bundle = omop_engine_bundle(engine=get_SQL_database_connection_v2(database=database),
    #                                     database=database,
    #                                     vocab_schema=f'{database}.VOCAB',
    #                                     data_schema=f'{database}.CDM',
    #                                     operational_schema=f'{database}.IC3_Internal',
    #                                     lookup_schema=f'{database}.IC3',
    #                                     results_schema=f'{database}.RESULTS',
    #                                     database_update_table='DATABASE_UPDATES',
    #                                     lookup_table='IC3_Variable_Lookup_Table',
    #                                     drug_lookup_table='IC3_DRUG_LOOKUP_TABLE')
    
    # validate_and_run_query_builder(spec_fp=r"Z:\GitHub\APARI_Federated_Learning\Resource_Files\APARI_Variable_Specification.xlsx",
    #                                     engine_bundle=engine_bundle,
    #                                     cohort_definition_id='procedure_occurrence_id',
    #                                     dir_dict={'status_files': r"Z:\Project_Data\new_query_builder",
    #                                               'intermediate_data': r"Z:\Project_Data\new_query_builder",
    #                                               'SQL': r"Z:\Project_Data\new_query_builder"},
    #                                     mode='data_retrieval',
    #                                     project_name='APARI',
    #                                     cdm_version='5.4',
    #                                     additional_variables_to_generate=['Variable_Generation', 'AKI_Phenotype', 'Outcome_Generation', 'SOFA'],
    #                                     quick_audit_n=None,
    #                                     save_audit_queries=False,
    #                                     use_visit_occurrence_parent_information=False)