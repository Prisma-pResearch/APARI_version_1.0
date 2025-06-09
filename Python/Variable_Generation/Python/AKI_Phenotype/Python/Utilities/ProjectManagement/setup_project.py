# -*- coding: utf-8 -*-
"""
Module to Setup a Project and download the necessary data to conduct the project from a SQL database.

Created on Fri Apr 30 11:53:38 2021.

@author: ruppert20
"""
import os
from flatten_dict import flatten
from ..Logging.log_messages import log_print_email_message as logm
from ..Logging.log_messages import start_logging_to_file
from .Populate_OMOP_Cohort import populate_omop_cohort
import shutil
import numpy as np
from sqlalchemy.engine.base import Engine
from ..FileHandling.io import check_load_df, save_data
from .cohort_splitting import split_development_validation
from ..FileHandling.io import query_folder_with_sql
import pandas as pd
from typing import Union, List
from ..Database.connect_to_database import omop_engine_bundle
from .query_builder import validate_and_run_query_builder
from ..General.dict_helper import dict_union
from math import ceil


# TODO: update docstring, and enable a manual query mode
def setup_OMOP_project_and_download_data(eligibility_query: str,
                                         engine_bundle: omop_engine_bundle,
                                         root_dir: str,
                                         configuration_instructions_fp: str,
                                         project_name: str,
                                         definition_desc: str,
                                         subject_id_field: str,
                                         start_date_field: str,
                                         end_date_field: str,
                                         dev_percent: float = 0.7,
                                         val_percent: float = 0.1,
                                         test_percent: float = 0.2,
                                         split_type: str = 'longitudinal',
                                         stratification_columns: list = None,
                                         time_index_col: str = None,
                                         unique_index_col: str = 'row_id',
                                         random_state: int = 20,
                                         num_batches: Union[int, None] = None,
                                         required_folders={'Results': ['models', 'figures', 'predictions', 'performance_tables'],
                                                           'Data': {'intermediate_data': {'transformed_data': ['preop', 'preop+intraop'],
                                                                                          'generated_data': ['preop', 'preop+intraop'],
                                                                                          'lookup_tables': ['preop', 'preop+intraop']},
                                                                    'source_data': None,
                                                                    'AUDIT': None,
                                                                    'reference_folder': None},
                                                           'Logs': 'status_files',
                                                           'Code': None,
                                                           'SQL': None,
                                                           'Documenation': None},
                                         additional_variables_to_generate: Union[List[str], None] = None,
                                         non_unique_base_folders: list = [],
                                         copy_to_project: dict = None,
                                         to_add_to_dir_dict: dict = None,
                                         logging_fp: str = None,
                                         cohort_preprocess_func: callable = None,
                                         query_mode: str = 'both',
                                         quick_audit_n: Union[int, None] = None,
                                         save_audit_queries: bool = False,
                                         min_train_size: Union[int, None] = None,
                                         n_samples: Union[int, None] = None,
                                         samples_per_batch: Union[int, None] = 20000,
                                         cdm_version: str = '5.4',
                                         use_visit_occurrence_parent_information: bool = True,
                                         time_index_mode: bool = True,
                                         **cohort_preprocess_func_kwargs):
    """
    Create Project Directories and Download data.

    Actions
    -------
        1. Setup folder directory based on required_folders dictionary
        2. Copy any files specified in copy_to_project
        3. Run cohort definition query.
        4. Download data using sql files in sql_file_list

    Parameters
    ----------
        eligibility_query: str
            String of file_path to sql query used to define the cohort.
        engine_bundle: omop_engine_bundle,
            An OMOP engine bundle that contains an SQL alchemy database connection and database metadata.
        root_dir: str
            The folder where the project should be setup inside of.
        configuration_instructions_fp: str
            file path to the variable specification .xlsx workbook where the list of variables needed for a project can be found.
        project_name: str
            Name of the Project. This is used in the cohort definition and the folder organizaiton.
        definition_desc: str, *required in OMOP Mode.
            Description of the cohort to be inserted in the cohort_definition table.
        subject_id_field: str, *required in OMOP Mode.
            Subject ID identifier in the eligiblity query for the project. This should be the most specific identifier possible. Options include 'visit_detail_id', 'visit_occurrence_id', 'person_id'
        start_date_field: str, *required in OMOP Mode.
            The column name representing the start of a patient's eligibility in the eligilibity query.
        end_date_field: str, *required in OMOP Mode.
            The column name representing the end of a patient's eligibility in the eligibility query'
        dev_percent: float, optional
            Percentage of eligible ids from eligilbity criteria used for developemnt. Default is 0.7.
        val_percent: float, optional
            Percentage of eligible ids from eligilbity criteria used for validation. Default is 0.1.
        test_percent: float, optional
            Percentage of eligible ids from eligilbity criteria used for testing. Default is 0.2.
        split_type: str, optional
            Whether the cohort should be split randomly or longitudinally. The default is 'longitudinal'. Options are 'longitudinal' and 'random'
        stratification_columns: list, optional
            List of columns in the eligilibty query that should be used to stratify the division between development, test, and validation.
        time_index_col: str, *required if logitudinal split is used
            The column name in the eligilibity query that is used to split the cohorts in time.
        unique_index_col: str, optional
            The column in the eligilbity query that must be unique. The default is 'row_id', but it is recommended to use 'visit_detail_id', 'visit_occurrence_id', or 'person_id' if those are unique.
        random_state: int, optional
            The random state used to split the cohort randomly. The default is 20.
        num_batches: int, optional
            The number of batches to subdivide the cohort into. The default is 32.
        required_folders: dict
            folder structure for your project. Required keys will be autopoulated.
        non_unique_base_folders: list, optional
            list of folders to be ommitted from the folder dict created from the required folders dict. The default is [].
        copy_to_project: dict, optional
            Set of of files/folders to copy to the project. Format {'destination key': source_folder/file}
        to_add_to_dir_dict: dict, optional
            Set of keys/values that should be added to the resultant dir_dict. Format {'key': value}
        logging_fp: str, optional
            A specific logging file path to use. eg. and existing log. By default a new log path will be generated for you.
        cohort_preprocess_func: callable, optional
            A function used to pre-process the cohort file downloaded from the server before uploading to the cohort table.
        query_mode: str, optional
            Whether data should be audited (aduit), downloaded (data_retrieval) or both (both). The default is 'both'.
        quick_audit_n: Union[int, None], Optional
            Whether the data_aduit should be limited to the first n rows. The default is None which will not limit it. This can be done to do a quick audit to ensure variables exist in a database.
        save_audit_queries: bool, Optional
            Whether the audit_queries should be saved for later review/use. The deafult is False, which will not save them.
        min_train_size: Union[int, None], Optional
            Minimum number of eligibility query results required for model training. If the results are less than that threshold, the train cohort is not constructed. The default is None, where it is not checked.
        n_samples: Union[int, None], Optional
            Select only the TOP n_samples from the elibility criteria query. Note this requires the characters "XXXXTOP_NXXXX" AFTER SELECT before the first field. The default is None, where there is no such restriction.
        samples_per_batch: Union[int, None], Optional
            The number of ids (based on the unique_index_col) that are in each sub batch. The default is 20000. Note larger batch sizes are generally faster to query since result streaming is enabled.
        cdm_version: str, Optional
            The OMOP CDM version being queried. The default is '5.4'; however, 5.3 is also supported.
        use_visit_occurrence_parent_information: bool, optional
            Whether to use the source visit occurrence ids and values or to use the curated parents. The default is True, which will use the curated information. Note: There is a check in the query builder to ensure the parent columns exist, if they do not it will fallback to the source information.
        time_index_mode: bool, optional
            Use the start/end date/datetimes for the referenced visit_occurrence/visit_detail instead of the visit_occurrence_id/visit_detail_id. This should be used if concurrent encounter information has not been harmonized under one parent visit occurrence id.
        **cohort_preprocess_func_kwargs: kwargs, optional
            List of kwargs to be directed to the cohort_preprocess_func.

    """
    assert isinstance(engine_bundle, omop_engine_bundle), f'The data_source must be of type omop_engine_bundle, however, one of type: {type(engine_bundle)} was found'

    assert isinstance(required_folders, dict)

    assert cdm_version in ['5.4', '5.3'], f'Unsupported CDM Version; {cdm_version}, please choose from 5.4 or 5.3'

    if query_mode in ['both', 'audit']:
        required_folders: dict = dict_union(required_folders, {'Data': {'Audit': ['stat_files', 'audit_source']}, 'Logs': 'status_files'})

    if query_mode in ['both', 'data_retrieval']:
        required_folders: dict = dict_union(required_folders, {'Data': ['source_data', 'intermediate_data'], 'Logs': 'status_files', 'SQL': None})

    if isinstance(additional_variables_to_generate, list):
        # all of these projects will run AKI_Phenotyping. AKI_Phenotyping if done in parallel without the directory structure is known to crash. This ensures that this does not occur.
        if len(set(additional_variables_to_generate).intersection(['AKI_Phenotype', 'Outcome_Generation', 'Variable_Generation'])) > 0:
            required_folders: dict = dict_union(required_folders,
                                                {'Data': {'generated_data': None,
                                                          'intermediate_data': {'Phenotyping': {'visit_occurrence_id': {'without_race_correction_v2': ['filtered_encounters',
                                                                                                                                                       'filtered_labs',
                                                                                                                                                       'filtered_diagnosis',
                                                                                                                                                       'filtered_procedure',
                                                                                                                                                       'filtered_dialysis',
                                                                                                                                                       'encounter_admin_flags',
                                                                                                                                                       'encounter_egfr_flags',
                                                                                                                                                       'ckd_row_egfr',
                                                                                                                                                       'encounter_creatinine_parameters',
                                                                                                                                                       'encounter_ckd',
                                                                                                                                                       'dialysis_time',
                                                                                                                                                       'encounter_aki',
                                                                                                                                                       'aki_trajectory']}}}}})

    # define folder structure
    dir_dict = setup_project(root_dir=root_dir,
                             required_folders=required_folders,
                             project_name=project_name,
                             non_unique_base_folders=non_unique_base_folders,
                             copy_to_project=copy_to_project,
                             to_add_to_dir_dict=to_add_to_dir_dict,
                             logging_fp=logging_fp)

    # define cohort
    cohort_success_fp: str = os.path.join(dir_dict.get('status_files'), 'cohort_defintion_success')

    if not os.path.exists(cohort_success_fp):
        raw_cache_success: str = cohort_success_fp.replace('_success', '_raw_success')
        raw_result_fp: str = os.path.join(dir_dict.get('source_data'), f'{project_name}_raw_cohort_result.csv')
        if not os.path.exists(raw_cache_success):
            save_data(df=check_load_df(input_v=eligibility_query,
                                       parse_dates=None if time_index_col is None else [time_index_col],
                                       engine=engine_bundle,
                                       replacements={'XXXXTOP_NXXXX': f'TOP {n_samples}' if isinstance(n_samples, int) else ''},
                                       chunksize=1000),
                      out_path=raw_result_fp)
            open(raw_cache_success, 'a')
        if cohort_preprocess_func is not None:

            qry_result: pd.DataFrame = cohort_preprocess_func(check_load_df(raw_result_fp,
                                                                            dtype=None,
                                                                            parse_dates=None if time_index_col is None else [time_index_col]),
                                                              **cohort_preprocess_func_kwargs)
        else:
            qry_result: pd.DataFrame = check_load_df(raw_result_fp,
                                                     dtype=None,
                                                     parse_dates=None if time_index_col is None else [time_index_col])

        df = split_development_validation(df=qry_result,
                                          project_name=project_name,
                                          dev_percent=dev_percent if (((qry_result.shape[0] * dev_percent) >= min_train_size) if isinstance(min_train_size, int) else True) else 0,
                                          val_percent=val_percent if (((qry_result.shape[0] * dev_percent) >= min_train_size) if isinstance(min_train_size, int) else True) else 0,
                                          test_percent=test_percent if (((qry_result.shape[0] * dev_percent) >= min_train_size) if isinstance(min_train_size, int) else True) else 1,
                                          split_type=split_type,
                                          stratification_columns=stratification_columns,
                                          unique_index_col=unique_index_col,
                                          time_index_col=time_index_col,
                                          random_state=random_state)\
            .reset_index(drop=False)\
            .rename(columns={'index': 'row_id'})

        num_batches: int = ceil(df.shape[0] / samples_per_batch) if isinstance(samples_per_batch, int) else num_batches if isinstance(num_batches, int) else 1

        for cohort in df.cohort.unique():

            for i, subset in enumerate(np.array_split(df[df.cohort == cohort], num_batches)):

                df.loc[df.row_id.isin(subset.row_id), 'subset'] = str(i)

        df.drop(columns=['row_id'], inplace=True)

        save_data(df=df, out_path=os.path.join(dir_dict.get('source_data'), f'{project_name}_master_cohort_definition.csv'))

        for cohort in df.cohort.unique():
            save_data(df=df[df.cohort == cohort], out_path=os.path.join(dir_dict.get('source_data'), f'{cohort}_cohort_definition.csv'))

        cohort_ids: List[int] = []
        for cohort in df.cohort.unique():

            cohort_ids.append(populate_omop_cohort(sql_file=eligibility_query,
                                                   results=df[df.cohort == cohort].copy(deep=True).rename(columns={'subset': 'subset_id'}),
                                                   results_schema=engine_bundle.results_schema,
                                                   engine=engine_bundle.engine,
                                                   cohort_name=cohort,
                                                   definition_desc=definition_desc,
                                                   subject_id_field=subject_id_field,
                                                   start_date_field=start_date_field,
                                                   end_date_field=end_date_field))

            open(cohort_success_fp, 'a').close()
            save_data(df=','.join([str(x) for x in cohort_ids]), out_path=os.path.join(dir_dict.get('status_files'), 'cohort_ids.txt'))

    # download data from server
    download_for_cohort(data_source=engine_bundle.engine,
                        dir_dict=dir_dict,
                        data_schema=engine_bundle.data_schema,
                        lookup_schema=engine_bundle.lookup_schema,
                        vocab_schema=engine_bundle.vocab_schema,
                        results_schema=engine_bundle.results_schema,
                        project_name=project_name,
                        sql_file_list=validate_and_run_query_builder(spec_fp=configuration_instructions_fp,
                                                                     engine_bundle=engine_bundle,
                                                                     cohort_definition_id=check_load_df(os.path.join(dir_dict.get('status_files'), 'cohort_ids.txt'), header=None).astype(int).iloc[0, 0],
                                                                     dir_dict=dir_dict,
                                                                     cdm_version=cdm_version,
                                                                     quick_audit_n=quick_audit_n,
                                                                     save_audit_queries=save_audit_queries,
                                                                     additional_variables_to_generate=additional_variables_to_generate,
                                                                     mode=query_mode,
                                                                     time_index_mode=time_index_mode,
                                                                     use_visit_occurrence_parent_information=use_visit_occurrence_parent_information),
                        cohort_table='COHORT',
                        patterns=None,
                        omop=True)

    dir_dict['variable_file_link'] = os.path.join(dir_dict.get('intermediate_data'), 'variable_file_linkage.xlsx')

    return dir_dict


def setup_project_and_download_data(eligibility_query: str,
                                    data_source: Union[Engine, str],
                                    root_dir: str,
                                    sql_file_list: list,
                                    project_name: str,
                                    data_schema: str,
                                    omop: bool,
                                    vocab_schema: str = None,
                                    results_schema: str = None,
                                    lookup_schema: str = None,
                                    definition_desc: str = None,
                                    subject_id_field: str = None,
                                    start_date_field: str = None,
                                    end_date_field: str = None,
                                    dev_percent: float = 0.7,
                                    val_percent: float = 0.1,
                                    test_percent: float = 0.2,
                                    split_type: str = 'longitudinal',
                                    stratification_columns: list = None,
                                    time_index_col: str = None,
                                    unique_index_col: str = 'row_id',
                                    random_state: int = 20,
                                    num_batches: int = 32,
                                    required_folders={'Results': ['models', 'figures', 'predictions', 'performance_tables'],
                                                      'Data': {'intermediate_data': {'transformed_data': ['preop', 'preop+intraop'],
                                                                                     'generated_data': ['preop', 'preop+intraop'],
                                                                                     'lookup_tables': ['preop', 'preop+intraop']},
                                                               'source_data': None,
                                                               'reference_folder': None},
                                                      'Logs': 'status_files',
                                                      'Code': None,
                                                      'SQL_folder': None,
                                                      'Documenation': None},
                                    non_unique_base_folders: list = [],
                                    copy_to_project: dict = None,
                                    to_add_to_dir_dict: dict = None,
                                    patterns: list = [r'_clean_[0-9_]+_optimized_ids\.csv', r'_clean_[0-9_]+\.csv',
                                                      r'_[0-9_]+_optimized_ids\.csv', r'_[0-9]+_[0-9]+\.csv', r'_[0-9]+\.csv', r'\.csv'],
                                    logging_fp: str = None,
                                    cohort_preprocess_func: callable = None,
                                    cohort_table: str = 'cohort_def',
                                    **cohort_preprocess_func_kwargs):
    """
    Create Project Directories and Download data.

    Actions
    -------
        1. Setup folder directory based on required_folders dictionary
        2. Copy any files specified in copy_to_project
        3. Run cohort definition query.
        4. Download data using sql files in sql_file_list

    Parameters
    ----------
        eligibility_query: str
            String of file_path to sql query used to define the cohort.
        data_source: Union[Engine, str],
            A folder path to where the data is located or an SQLAlchemy Engine connection to the database of interest.
        root_dir: str
            The folder where the project should be setup inside of.
        sql_file_list: list
            List of sql queries to run to download the required data.
        project_name: str
            Name of the Project. This is used in the cohort definition and the folder organizaiton.
        data_schema: str
            Location in the database where patient data is stored.
        omop: bool
            Whether it is in OMOP database source or not.
        vocab_schema: str, *required if in OMOP Mode.
            Location in the database where vocabulary files are stored.
        results_schema: str, *required if in OMOP Mode.
            Location in the database where the cohort and cohort_definition tables are stored.
        lookup_schema: str, *required if in OMOP Mode
            Location in the database where look up tables are stored.
        definition_desc: str, *required in OMOP Mode.
            Description of the cohort to be inserted in the cohort_definition table.
        subject_id_field: str, *required in OMOP Mode.
            Subject ID identifier in the eligiblity query for the project. This should be the most specific identifier possible. Options include 'visit_detail_id', 'visit_occurrence_id', 'person_id'
        start_date_field: str, *required in OMOP Mode.
            The column name representing the start of a patient's eligibility in the eligilibity query.
        end_date_field: str, *required in OMOP Mode.
            The column name representing the end of a patient's eligibility in the eligibility query'
        dev_percent: float, optional
            Percentage of eligible ids from eligilbity criteria used for developemnt. Default is 0.7.
        val_percent: float, optional
            Percentage of eligible ids from eligilbity criteria used for validation. Default is 0.1.
        test_percent: float, optional
            Percentage of eligible ids from eligilbity criteria used for testing. Default is 0.2.
        split_type: str, optional
            Whether the cohort should be split randomly or longitudinally. The default is 'longitudinal'. Options are 'longitudinal' and 'random'
        stratification_columns: list, optional
            List of columns in the eligilibty query that should be used to stratify the division between development, test, and validation.
        time_index_col: str, *required if logitudinal split is used
            The column name in the eligilibity query that is used to split the cohorts in time.
        unique_index_col: str, optional
            The column in the eligilbity query that must be unique. The default is 'row_id', but it is recommended to use 'visit_detail_id', 'visit_occurrence_id', or 'person_id' if those are unique.
        random_state: int, optional
            The random state used to split the cohort randomly. The default is 20.
        num_batches: int, optional
            The number of batches to subdivide the cohort into. The default is 32.
        required_folders: dict
            required keys:
                * 'source_data'
                * 'status_files'
        non_unique_base_folders: list, optional
            list of folders to be ommitted from the folder dict created from the required folders dict. The default is [].
        copy_to_project: dict, optional
            Set of of files/folders to copy to the project. Format {'destination key': source_folder/file}
        to_add_to_dir_dict: dict, optional
            Set of keys/values that should be added to the resultant dir_dict. Format {'key': value}
        patterns: list, optional
            File patterns used to load data from a fodler. The default is ['_clean_[0-9_]+_optimized_ids.csv', '_clean_[0-9_]+.csv',
                          '_[0-9_]+_optimized_ids.csv', '_[0-9]+_[0-9]+.csv', '_[0-9]+.csv', '.csv'],
        logging_fp: str, optional
            A specific logging file path to use. eg. and existing log. By default a new log path will be generated for you.
        cohort_preprocess_func: callable, optional
            A function used to pre-process the cohort file downloaded from the server before uploading to the cohort table.
        cohort_table: str, optional
            The name of the cohort table. The default is 'cohort_def'; however, this is automatically overwritten to COHORT if omop is set to True.
        **cohort_preprocess_func_kwargs: kwargs, optional
            List of kwargs to be directed to the cohort_preprocess_func.

    """
    assert isinstance(data_source, (Engine, str)), f'The data_source must be of type string or SQL alchemy engine, however, one of type: {type(data_source)} was found'
    if omop:
        assert isinstance(data_source, Engine), f'In OMOP mode the data_source must be of type SQL alchemy engine, however, one of type: {type(data_source)} was found'
        assert isinstance(results_schema, str), 'results_schema is required in OMOP Mode'
        assert isinstance(vocab_schema, str), 'vocab_schema is required in OMOP Mode'
        assert isinstance(lookup_schema, str), 'lookup_schema is required in OMOP Mode'
        assert isinstance(definition_desc, str), 'definition_description is required in OMOP mode'
        assert isinstance(subject_id_field, str), 'subject_id_fieldn is required in OMOP mode'
        assert isinstance(start_date_field, str), 'start_date_field is required in OMOP mode'
        assert isinstance(end_date_field, str), 'end_date_field is required in OMOP mode'
        cohort_table: str = 'COHORT'

    elif isinstance(data_source, str):
        assert os.path.exists(data_source), f'Unable to locate data_source: {data_source}'

    # define folder structure
    dir_dict = setup_project(root_dir=root_dir,
                             required_folders=required_folders,
                             project_name=project_name,
                             non_unique_base_folders=non_unique_base_folders,
                             copy_to_project=copy_to_project,
                             to_add_to_dir_dict=to_add_to_dir_dict,
                             logging_fp=logging_fp)

    # define cohort
    cohort_success_fp: str = os.path.join(dir_dict.get('status_files'), 'cohort_defintion_success')
    if isinstance(data_source, Engine):
        if data_source.name == 'mssql':
            cohort_table = cohort_table.upper()
        engine: Engine = data_source
        query_folder: str = None
    else:
        engine: Engine = None
        query_folder: str = data_source
        data_schema: str = ''

    if not os.path.exists(cohort_success_fp):
        raw_cache_success: str = cohort_success_fp.replace('_success', '_raw_success')
        raw_result_fp: str = os.path.join(dir_dict.get('source_data'), f'{project_name}_raw_cohort_result.csv')
        if not os.path.exists(raw_cache_success):
            save_data(df=check_load_df(input_v=eligibility_query,
                                       parse_dates=None if time_index_col is None else [time_index_col],
                                       query_folder=query_folder,
                                       engine=engine,
                                       chunksize=1000,
                                       replacements={'ReSuLtS_ScHeMa': results_schema,
                                                     'DaTa_ScHeMa': data_schema,
                                                     'VoCaB_ScHeMa': vocab_schema,
                                                     'LoOkUp_ScHeMa': lookup_schema},
                                       patterns=patterns),
                      out_path=raw_result_fp)
            open(raw_cache_success, 'a')
        if cohort_preprocess_func is not None:

            qry_result: pd.DataFrame = cohort_preprocess_func(check_load_df(raw_result_fp,
                                                                            dtype=None,
                                                                            parse_dates=None if time_index_col is None else [time_index_col]),
                                                              **cohort_preprocess_func_kwargs)
        else:
            qry_result: pd.DataFrame = check_load_df(raw_result_fp,
                                                     dtype=None,
                                                     parse_dates=None if time_index_col is None else [time_index_col])

        df = split_development_validation(df=qry_result,
                                          project_name=project_name,
                                          dev_percent=dev_percent,
                                          val_percent=val_percent,
                                          test_percent=test_percent,
                                          split_type=split_type,
                                          stratification_columns=stratification_columns,
                                          unique_index_col=unique_index_col,
                                          time_index_col=time_index_col,
                                          random_state=random_state)\
            .reset_index(drop=False)\
            .rename(columns={'index': 'row_id'})

        for cohort in df.cohort.unique():

            for i, subset in enumerate(np.array_split(df[df.cohort == cohort], num_batches)):

                df.loc[df.row_id.isin(subset.row_id), 'subset'] = str(i)

        df.drop(columns=['row_id'], inplace=True)

        save_data(df=df, out_path=os.path.join(dir_dict.get('source_data'), f'{project_name}_master_cohort_definition.csv'))

        for cohort in df.cohort.unique():
            save_data(df=df[df.cohort == cohort], out_path=os.path.join(dir_dict.get('source_data'), f'{cohort}_cohort_definition.csv'))

        if isinstance(engine, Engine):
            cohort_ids: List[int] = []
            if omop:
                for cohort in df.cohort.unique():

                    cohort_ids.append(populate_omop_cohort(sql_file=eligibility_query,
                                                           results=df[df.cohort == cohort].copy(deep=True).rename(columns={'subset': 'subset_id'}),
                                                           results_schema=results_schema,
                                                           engine=engine,
                                                           cohort_name=cohort,
                                                           definition_desc=definition_desc,
                                                           subject_id_field=subject_id_field,
                                                           start_date_field=start_date_field,
                                                           end_date_field=end_date_field))

                open(cohort_success_fp, 'a').close()
                save_data(df=','.join([str(x) for x in cohort_ids]), out_path=os.path.join(dir_dict.get('status_files'), 'cohort_ids.txt'))
            else:
                save_data(df=df[df.columns.intersection(['patient_deiden_id', 'merged_enc_id', 'or_case_num', 'person_id', 'visit_occurrence_id', 'visit_detail_id', 'cohort', 'subset'])],
                          engine=engine, dtypes=None, dest_table=cohort_table, dest_schema=data_schema,
                          success_fp=cohort_success_fp, failure_fp=cohort_success_fp.replace('_success', '_upload_problems.pkl'), debug=True)
        else:
            cd_fp: str = os.path.join(query_folder, f'{cohort_table}.csv')
            if os.path.exists(cd_fp):
                old: pd.DataFrame = check_load_df(cd_fp, ds_type='pandas')
                df = pd.concat([df, old[~old.cohort.str.contains(r'^{}_'.format(project_name), regex=True, case=False)]], axis=0, sort=False, ignore_index=True)
            save_data(df=df[df.columns.intersection(['patient_deiden_id', 'merged_enc_id', 'or_case_num', 'person_id', 'visit_occurrence_id', 'visit_detail_id', 'cohort', 'subset'])],
                      out_path=cd_fp)
            open(cohort_success_fp, 'a').close()

    # download data from server
    download_for_cohort(data_source=data_source, dir_dict=dir_dict, data_schema=data_schema, lookup_schema=lookup_schema,
                        vocab_schema=vocab_schema, results_schema=results_schema, project_name=project_name,
                        sql_file_list=sql_file_list, cohort_table=cohort_table, patterns=patterns, omop=omop)

    return dir_dict


def setup_project(root_dir: str,
                  required_folders: dict = {'Results': ['models', 'figures', 'predictions', 'performance_tables'],
                                            'Data': {'intermediate_data': {'transformed_data': ['preop', 'preop+intraop'],
                                                                           'generated_data': ['preop', 'preop+intraop'],
                                                                           'lookup_tables': ['preop', 'preop+intraop']},
                                                     'source_data': None},
                                            'Logs': 'status_files',
                                            'Code': None,
                                            'SQL': None},
                  project_name: str = None,
                  non_unique_base_folders: list = ['preop', 'preop+intraop'],
                  copy_to_project: dict = None,
                  to_add_to_dir_dict: dict = None,
                  logging_fp: str = None):
    """
    Configure Project Directories, create folder dict, and copy necessary files/folders.

    Parameters
    ----------
    root_dir : str
        DESCRIPTION.
    required_folders : dict, optional
        DESCRIPTION. The default is {'Results': ['models', 'figures', 'predictions', 'performance_tables'],
                                     'Data': {'intermediate_data': {'transformed_data': ['preop', 'preop+intraop'],
                                                                    'generated_data': ['preop', 'preop+intraop'],
                                                                    'lookup_tables': ['preop', 'preop+intraop']},
                                              'source_data': None},
                                     'Logs': 'status_files',
                                     'Code': None,
                                     'SQL': None}.
    project_name : str, optional
        DESCRIPTION. The default is None.
    non_unique_base_folders : list, optional
        DESCRIPTION. The default is ['preop', 'preop+intraop'].
    copy_to_project : dict, optional
        DESCRIPTION. The default is None.

    Returns
    -------
    dir_dict : TYPE
        DESCRIPTION.

    """
    assert isinstance(root_dir, str), f'{root_dir} is not a valid folder path'
    assert isinstance(required_folders, dict), f'The required_folders parameter must be of type dict, but found type {type(required_folders)}'
    assert 'Logs' in required_folders, 'Logs must be a key in the first layer of the required_folders dictionary'

    # setup folder structure
    dir_dict: dict = _ensure_folder_dict(folder_dict=required_folders, root_dir=root_dir, non_unique_base_folders=non_unique_base_folders)

    # setup logging
    dir_dict['logging_fp'] = logging_fp if isinstance(logging_fp, str) else start_logging_to_file(directory=dir_dict.get('Logs'),
                                                                                                  file_name=project_name if isinstance(project_name, str) else os.path.basename(root_dir))

    # copy code to project folder
    if isinstance(copy_to_project, dict):
        for k, f in copy_to_project.items():
            if ((f.find(root_dir) == -1) and (k in dir_dict)):
                logm(message=f'Copying {os.path.basename(f)} to {k}')
                _copy_files(f, dir_dict.get(k))
            elif not (k in dir_dict):
                logm(message=f'Skipping copying {k} to {os.path.basename(f)} for {k} not in dir_dict', warning=True)
            else:
                logm(message=f'Skipping copying {k} to {os.path.basename(f)} for {k} file(s) are already in the project folder', warning=True)

    if isinstance(to_add_to_dir_dict, dict):
        dir_dict.update(to_add_to_dir_dict)

    return dir_dict


def _ensure_folder_dict(folder_dict: dict, root_dir: str, non_unique_base_folders: list = []) -> dict:
    flat_dict: dict = flatten(folder_dict, reducer='path')

    out: dict = {}

    terminal_keys: list = []
    for tk in pd.Series(flat_dict.values()).dropna():
        if isinstance(tk, str):
            terminal_keys.append(tk)
        elif isinstance(tk, list):
            for x in tk:
                terminal_keys.append(x)

    terminal_keys: list = pd.Series(terminal_keys).value_counts().reset_index(drop=False)\
        .rename(columns={0: 'count'}).query('count == 1')['index'].tolist()

    for k, v in flat_dict.items():
        # establish base folder
        tfp: str = os.path.join(root_dir, k)
        os.makedirs(name=tfp, exist_ok=True)

        # determine key hierarchy
        ks: list = k.split('\\' if os.name == 'nt' else '/')

        # add hierarchy to dict
        xfp: str = tfp
        for x in reversed(ks):
            out[x] = xfp
            xfp: str = os.path.dirname(xfp)

        # determine value hierarchy and add to dict
        if (v is None):
            pass
        elif isinstance(v, str) or isinstance(v, list):
            for x in (v if isinstance(v, list) else [v]):
                xfp: str = os.path.join(tfp, x)
                os.makedirs(name=xfp, exist_ok=True)
                if x in terminal_keys:
                    out[x] = xfp
        else:
            raise Exception(f'Unexpected Value type: {type(v)}. Expecting a str or list')

    return out


def _ensure_path_exists(fp: str, reflect_fp: bool = False):
    os.makedirs(name=fp, exist_ok=True)

    if reflect_fp:
        return fp


def _copy_files(src: str, dst: str):
    # determine if it is a file or a folder and format appropriately
    if os.path.isdir(src) and os.path.isdir(dst):
        shutil.copytree(src=src, dst=dst, symlinks=False, ignore=None,
                        copy_function=shutil.copy,
                        ignore_dangling_symlinks=False,
                        dirs_exist_ok=True)
    elif os.path.isfile(src):
        shutil.copy(src=src, dst=dst if os.path.isfile(dst) else os.path.join(dst, os.path.basename(src)))
    else:
        raise Exception('Cannot Copy a folder to a file')


def download_for_cohort(data_source: Union[Engine, str],
                        dir_dict: dict,
                        data_schema: str,
                        results_schema: str,
                        vocab_schema: str,
                        project_name: str,
                        lookup_schema: str,
                        cohort_table: str,
                        sql_file_list: list,
                        patterns: list = [r'_clean_[0-9_]+_optimized_ids\.csv', r'_clean_[0-9_]+\.csv', r'_[0-9_]+_optimized_ids\.csv',
                                          r'_[0-9]+_[0-9]+\.csv', r'_[0-9]+\.csv', r'\.csv'],
                        self_contained: bool = False,
                        omop: bool = True,
                        has_subsets: bool = True):
    """
    Download data from sql server for specified project.

    Parameters
    ----------
    engine : Engine
        SQL Alchemy database engine.
    dir_dict : dict
        dictionary of folder information that must contain the following keys:
            *'status_files'
            *'source_data'
    data_schema : str
        Schema where the data is located.
    results_schema: str
        Where the cohort is located in OMOP mode.
    vocab_schema: str
        Where the vocabulary schema is located in OMOP mode.
    lookup_schema: str
        Location where the lookup table is stored.
    project_name : str
        project name.
    sql_file_list : list
        list of sql filepaths that each must contain the following fields:
            *'XXXXXX' which is where the cohort name will be filled in
            *YYYY which is where the subset will be filled in

    Returns
    -------
    None.

    """
    if isinstance(data_source, Engine):
        engine: Engine = data_source
        # pull cohort and subset lists from server
        if omop:
            cohort_df = check_load_df(f'''SELECT DISTINCT
                                                c.cohort_definition_id [cohort],
                                                cd.cohort_definition_name,
                                                c.subset_id [subset]
                                            FROM
                                                {results_schema}.COHORT c
                                                INNER JOIN {results_schema}.COHORT_DEFINITION cd on c.cohort_definition_id = cd.cohort_definition_id
                                            WHERE
                                                c.cohort_definition_id IN ({check_load_df(os.path.join(dir_dict.get('status_files'), 'cohort_ids.txt'), raw_txt=True)}) -- cd.cohort_definition_name LIKE '{project_name}%%'
                                            ORDER BY
                                                c.cohort_definition_id, c.subset_id ASC;''', engine=engine)
        else:
            cohort_df = check_load_df(f'''SELECT DISTINCT
                                          cohort, subset
                                      FROM
                                          {data_schema}.{cohort_table}
                                      WHERE
                                          cohort LIKE '{project_name}%%';''', engine=engine) if not self_contained else None
    else:
        cohort_df = query_folder_with_sql(sql_query=f'''SELECT DISTINCT
                                      cohort, subset
                                  FROM
                                      {cohort_table}
                                  WHERE
                                      cohort LIKE '{project_name}%%';''',
                                          query_folder=data_source,
                                          db_fp=None, patterns=patterns,
                                          return_db_connection=False, load_all_cols=False,
                                          overwrite_existing_tables=False, load_only=False)

    cohorts: list = [f'{x}' for x in cohort_df.cohort.dropna().unique()] if not self_contained else ['']

    # iterate through sql file list
    for sql_query in sql_file_list:

        cohorts_to_process: pd.Series = pd.Series({cohort: os.path.join(dir_dict.get('status_files'),
                                                                        os.path.basename(sql_query)
                                                                        .replace('.sql', f'_{cohort}_success')) for cohort in cohorts},
                                                  name='success_path')

        cohorts_to_process: pd.DataFrame = pd.concat([cohorts_to_process, cohorts_to_process.apply(os.path.exists).rename('completed')], axis=1)

        if not cohorts_to_process.completed.all():
            # load sqlite connection as engine
            if not isinstance(data_source, Engine):
                engine = query_folder_with_sql(sql_query=sql_query,
                                               query_folder=data_source,
                                               patterns=patterns,
                                               db_fp=None,
                                               replacements={'YYYY': '0'},
                                               return_db_connection=False, load_all_cols=False,
                                               overwrite_existing_tables=False, load_only=True)

            for cohort, row in cohorts_to_process[~cohorts_to_process.completed].iterrows():
                # iterate through list of subsets
                for subset in [f'{x}' for x in cohort_df[cohort_df.cohort.astype(str) == cohort].subset.dropna().unique()] if has_subsets else ['']:

                    # create s_name
                    s_name: str = f'_chunk_{subset}' if has_subsets else ''

                    # define success and out file paths for each cohort/subset combination
                    success_fp: str = os.path.join(dir_dict.get('status_files'), os.path.basename(sql_query).replace('.sql', f'_{cohort}{s_name}_success'))

                    out_path: str = os.path.join(dir_dict.get('source_data'), os.path.basename(sql_query).replace('.sql', f'_{cohort}{s_name}.csv'))

                    # if the success path does not exists pull the data from the server using the cohort definition
                    if not os.path.exists(success_fp):

                        save_data(check_load_df(sql_query,
                                                engine=engine,
                                                chunksize=2000,
                                                preserve_case=True,
                                                replacements={'XXXXXX': cohort, 'YYYY': str(subset),
                                                              'ReSuLtS_ScHeMa': results_schema,
                                                              'DaTa_ScHeMa': data_schema,
                                                              'VoCaB_ScHeMa': vocab_schema,
                                                              'LoOkUp_ScHeMa': lookup_schema}),
                                  out_path=out_path)

                        open(success_fp, 'a').close()
                open(row.success_path, 'a').close()
        else:
            logm(message=f'{os.path.basename(sql_query)} has already been downloaded')
