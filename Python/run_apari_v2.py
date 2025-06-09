# -*- coding: utf-8 -*-
"""
Created on Tue Jun  6 16:05:57 2023

@author: ruppert20
"""
import os
from typing import Union
import pandas as pd
import sqlalchemy
from tqdm import tqdm
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import URL
from .Utilities.Database.connect_to_database import omop_engine_bundle, execute_query_in_transaction, get_SQL_database_connection_v2
from .generate_apari_variables import generate_APARI_variables_part1, generate_APARI_variables_part2
from .make_apari_dataset import make_APARI_dataset
from .Utilities.ProjectManagement.setup_project import setup_OMOP_project_and_download_data
from .Utilities.FileHandling.io import get_batches_from_directory, save_data, check_load_df, find_files
from .Utilities.ProjectManagement.completion_monitoring import check_complete
from .Utilities.ResourceManagement.parallelization_helper import run_function_in_parallel_v2
from .Utilities.Reporting.make_tables_from_var_spec import make_tables_from_var_spec
from .apari_model import train_test_model
from .upload_rvu_table import upload_rvu_data


def run_APARI(root_project_dir: str,
              project_name: str,
              database_host_name: str,
              database: str,
              database_username: str,
              database_password: str,
              vocab_schema: str,
              data_schema: str,
              operational_schema: str,
              lookup_schema: str,
              results_schema: str,
              default_facility_zip: str,
              n_gpus: int = 0,
              cdm_version: str = '5.4',
              dset_name: str = 'APARI_v1.0.h5',
              mode: str = 'audit',
              lookup_table: str = 'IC3_Variable_Lookup_Table',
              drug_lookup_table: str = 'IC3_DRUG_LOOKUP_TABLE',
              max_workers: int = 4,
              display_logs: bool = True,
              serial_variable_generation: bool = False,
              subject_id_mode: str = 'procedure_occurrence',
              n_samples: Union[int, None] = None,
              engine_override: bool = False):

    assert mode in ['audit', 'data_retrieval', 'both'], f"Invalid mode: {mode}. Please choose from one of the following: ['audit', 'data_retrieval', 'both']"

    assert subject_id_mode in ['procedure_occurrence', 'visit_detail'], f'Invalid subject_id_mode: {subject_id_mode}'

    if engine_override:
        engine: Engine = get_SQL_database_connection_v2(database=database)
        connection_url = get_SQL_database_connection_v2(database=database, return_connection_url=True)
    else:
        # create connection url
        connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + database_host_name + ';DATABASE=' + database + ';UID=' + database_username + ';PWD={' + database_password + '}'})
        engine: Engine = sqlalchemy.create_engine(connection_url, fast_executemany=True, execution_options={"stream_results": True})

    # define omop engine bundle
    engine_bundle = omop_engine_bundle(engine=engine,
                                       database=database,
                                       vocab_schema=vocab_schema,
                                       data_schema=data_schema,
                                       operational_schema=operational_schema,
                                       lookup_schema=lookup_schema,
                                       results_schema=results_schema,
                                       database_update_table='Not_applicable',
                                       lookup_table=lookup_table,
                                       drug_lookup_table=drug_lookup_table)

    # peform sanity check to verify all tables can be found
    _sanity_check(engine_bundle)

    upload_rvu_data(engine_bundle=engine_bundle,
                     rvu_workbook_fp=os.path.join(os.path.dirname(os.path.dirname(__file__)), 'Resource_Files', "PPRRVU_2012a_2024a.xlsx"))

    dir_dict = setup_OMOP_project_and_download_data(eligibility_query=os.path.join(os.path.dirname(os.path.dirname(__file__)), 'SQL', "procedure_occurrence_eligibility.sql" if subject_id_mode == "procedure_occurrence" else "eligibility_criteria_UF.sql" if database == 'IDEALIST' else f"eligibility_criteria_cdm{cdm_version}.sql"),
                                                    engine_bundle=engine_bundle,
                                                    root_dir=root_project_dir,
                                                    configuration_instructions_fp=os.path.join(os.path.dirname(os.path.dirname(__file__)), 'Resource_Files', "APARI_Variable_Specification.xlsx"),
                                                    project_name=project_name,
                                                    definition_desc='APRARI PROJECT: Inpatient Surgical Encounters',
                                                    subject_id_field=f'{subject_id_mode}_id',
                                                    start_date_field='surgery_start_datetime',
                                                    end_date_field='surgery_end_datetime',
                                                    dev_percent=0.8,
                                                    val_percent=0.1,
                                                    test_percent=0.1,
                                                    split_type='longitudinal',
                                                    unique_index_col=f'{subject_id_mode}_id',
                                                    time_index_col='visit_start_datetime',
                                                    random_state=20,
                                                    query_replacements={'IC3_DRUG_LOOKUP_TABLE': 'IC3_DRUG_LOOKUP_TABLE_v2',
                                                                        'm.unit_source_concept_id': 'NULL',
                                                                        'devE.unit_concept_id': 'NULL'} if database == 'OMOP' else {},
                                                    samples_per_batch=20000,
                                                    min_train_size=10000,
                                                    n_samples=n_samples,
                                                    stratification_columns=['facility_zip'],
                                                    additional_variables_to_generate=['SOFA', 'Outcome_Generation', 'AKI_Phenotype', 'Variable_Generation'],
                                                    required_folders={'Results': {'model': None,
                                                                                  'figures': None,
                                                                                  'tables': ['performance', 'predictions', 'cohort']},
                                                                      'Data': {'generated_data': {'static': ['static_standardization'],
                                                                                                  'time_series': ['time_series_standardization'],
                                                                                                  'outcomes': None},
                                                                               'intermediate_data': None,
                                                                               'dataset': None,
                                                                               'source_data': None,
                                                                               'Audit': ['stat_files', 'audit_source'],
                                                                               'reference_folder': None},
                                                                      'Logs': 'status_files',
                                                                      'Code': None,
                                                                      'SQL': None,
                                                                      'Documenation': None},
                                                    save_audit_queries=True,                                               
                                                    to_add_to_dir_dict={'custom_sql': os.path.join(os.path.dirname(os.path.dirname(__file__)), 'SQL', 'custom_sql')},
                                                    copy_to_project={'SQL': find_files(directory=os.path.join(os.path.dirname(os.path.dirname(__file__)), 'SQL', 'procedure_occurrence_queries'),
                                                                                       patterns=['*.sql'], regex=False, recursive=False),
                                                                     'status_files': find_files(directory=os.path.join(os.path.dirname(os.path.dirname(__file__)), 'SQL', 'procedure_occurrence_queries'),
                                                                                       patterns=['*success__'], regex=False, recursive=False),
                                                                      'intermediate_data': find_files(directory=os.path.join(os.path.dirname(os.path.dirname(__file__)), 'SQL', 'procedure_occurrence_queries'),
                                                                                       patterns=['variable_file_linkage.xlsx'], regex=False, recursive=False)} if subject_id_mode == 'procedure_occurrence' else {},
                                                    quick_audit_n=None,
                                                    cdm_version=cdm_version,
                                                    query_mode=mode)

    if (mode in ['both', 'audit']) and (subject_id_mode == 'visit_detail'):
        print(f"{project_name} AUDIT COMPLETE FOR APARI Variables. Please see {os.path.join(dir_dict.get('Audit'), 'final_report.xlsx')} for the results")

    if mode not in ['both', 'data_retrieval']:
        return

    batches: list = get_batches_from_directory(directory=dir_dict.get('source_data'),
                                               file_name=r'^person_INNER_lookup_join',
                                               batches=None, independent_sub_batches=True)
    print(batches)

    kwargs_list: list = []

    for batch in batches:
        kwargs_list.append({'dir_dict': dir_dict,
                            'project_name': project_name,
                            'engine_bundle': omop_engine_bundle(engine=connection_url,
                                                                database=database,
                                                                vocab_schema=vocab_schema,
                                                                data_schema=data_schema,
                                                                operational_schema=operational_schema,
                                                                lookup_schema=lookup_schema,
                                                                results_schema=results_schema,
                                                                database_update_table='Not_applicable',
                                                                lookup_table=lookup_table,
                                                                drug_lookup_table=drug_lookup_table),
                            'default_facility_zip': default_facility_zip,
                            'cohort_id': batch.split('_chunk_')[0],
                            'subset_id': batch.split('_chunk_')[1],
                            'subject_id_mode': f'{subject_id_mode}_id',
                            'display': display_logs,
                            'log_name': f'APARI_variable_generation_part_1_batch_{batch}'})

    run_function_in_parallel_v2(generate_APARI_variables_part1,
                                kwargs_list=kwargs_list,
                                max_workers=max_workers,
                                update_interval=10,
                                disp_updates=display_logs,
                                log_name='APARI_VARIABLE_GENERATION',
                                list_running_futures=True,
                                debug=serial_variable_generation)

    check_complete(dir_dict=dir_dict.copy(),
                   batch_list=batches,
                   search_clean=False,
                   phenotyping_flag=False,
                   source_dir_key='source_data',
                   dest_dir_key='generated_data',
                   split_into_n_batches=dir_dict.get('split_into_n_batches', None),
                   files_list=['all_surgical_variables'],
                   raise_exception=True,
                   pre_run_check=False,
                   essential_file_name=r'^person_INNER_lookup_join',
                   log_name='APARI_VARIABLE_GENERATION')

    for batch in tqdm(batches, desc='APARI_variable_generation_part_2'):
        generate_APARI_variables_part2(dir_dict=dir_dict,
                                       project_name=project_name,
                                       cohort_id=batch.split('_chunk_')[0],
                                       subset_id=batch.split('_chunk_')[1],
                                       display=display_logs,
                                       log_name=f'APARI_variable_generation_part_1_batch_{batch}')

    make_APARI_dataset(dir_dict=dir_dict,
                       project_name=project_name,
                       combine_dev_and_test_for_kfold=True,
                       dset_name=dset_name,
                       subject_id_type=f'{subject_id_mode}_id',
                       serial=serial_variable_generation,
                       display=display_logs)

    train_test_model(dir_dict=dir_dict,
                     dset_names=['all_APARI_dataset_v1.0.h5'],  # os.listdir(dir_dict.get('dataset')),
                     train_mode=True,
                     n_gpus=n_gpus,
                     n_quick_check=None,
                     n_data_load_workers=max_workers)

    # create cohort summary tables
    make_tables_from_var_spec(instruction_fp=os.path.join(os.path.dirname(os.path.dirname(__file__)), 'Resource_Files', "APARI_Variable_Specification.xlsx"),
                              var_file_link_fp=dir_dict.get('variable_file_link'),
                              source_dir=dir_dict.get('source_data'),
                              project_name='APARI',
                              generated_data_dir=dir_dict.get('generated_data'),
                              out_path=os.path.join(dir_dict.get('cohort'), 'cohort_summarry.xlsx'),
                              subject_id_type=f'{subject_id_mode}_id')


def _sanity_check(engine_bundle: omop_engine_bundle):

    for check, query in {'vocab_check': f'SELECT TOP 1 concept_id FROM {engine_bundle.vocab_schema}.CONCEPT',
                         'data_check': f'SELECT TOP 1 person_id FROM {engine_bundle.data_schema}.PERSON',
                         'var_lookup_check': f'SELECT TOP 1 concept_id FROM {engine_bundle.lookup_schema}.{engine_bundle.lookup_table}',
                         'drug_lookup_check': f'SELECT TOP 1 drug_concept_id FROM {engine_bundle.lookup_schema}.{engine_bundle.drug_lookup_table}',
                         'operational_schema_check': f"SELECT [name] FROM {engine_bundle.operational_schema.split('.')[0] if '.' in engine_bundle.operational_schema else engine_bundle.database}.sys.schemas WHERE name = '{engine_bundle.operational_schema.split('.')[-1]}'",
                         'results_schema_check': f"SELECT TABLE_NAME FROM {engine_bundle.results_schema.split('.')[0] if '.' in engine_bundle.results_schema else engine_bundle.database}.INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'COHORT' AND TABLE_SCHEMA = '{engine_bundle.results_schema.split('.')[-1]}'"
                         }.items():

        try:
            pd.read_sql(query, con=engine_bundle.engine).iloc[0, 0]
        except:
            if check == 'var_lookup_check':
                save_data(df=check_load_df(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'Resource_Files', "lookup_table.csv.gz"),
                                           sep='|', encoding='latin-1'),
                          engine=engine_bundle,
                          dest_table=f'{engine_bundle.lookup_schema}.{engine_bundle.lookup_table}',
                          dtypes={'concept_id': 'INT',
                                  'concept_code': 'VARCHAR(50)',
                                  'concept_name': 'VARCHAR(255)',
                                  'vocabulary_id': 'VARCHAR(20)',
                                  'concept_class_id': 'VARCHAR(20)',
                                  'domain_id': 'VARCHAR(20)',
                                  'ancestor_concept_id': 'INT',
                                  'variable_name': 'VARCHAR(100)',
                                  'variable_desc': 'VARCHAR(255)'})
            elif check == 'drug_lookup_check':
                for statement in check_load_df(os.path.join(os.path.dirname(__file__), 'Utilities', 'OMOP', 'SQL', 'lookup_tables', 'create_drug_dose_lookup_table.sql'),
                                               return_raw_query=True,
                                               replacements={'ReSuLtS_ScHeMa': engine_bundle.results_schema,
                                                             'DaTa_ScHeMa': engine_bundle.data_schema,
                                                             'VoCaB_ScHeMa': engine_bundle.vocab_schema,
                                                             'LoOkUp_ScHeMa': engine_bundle.lookup_schema,
                                                             'OpErAtIoNaL_ScHeMa': engine_bundle.operational_schema}).split('GO')[:-1]:
                    execute_query_in_transaction(engine=engine_bundle.engine, query=statement, raise_exceptions=True)
            else:
                raise Exception(f'The {check} failed. Please check your database configuration such that the following will return the appropriate value: {query}')

        if check == 'results_schema_check':
            try:
                pd.read_sql(f'SELECT TOP 1 subset_id FROM {engine_bundle.results_schema}.COHORT', con=engine_bundle.engine)
            except:
                execute_query_in_transaction(engine=engine_bundle.engine, query=f'ALTER TABLE {engine_bundle.results_schema}.COHORT ADD subset_id INT NULL;', raise_exceptions=True)
