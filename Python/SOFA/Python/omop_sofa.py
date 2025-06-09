# -*- coding: utf-8 -*-
"""
SOFA, eSOFA, and qSOFA Calclation Module from OMOP CDM.

Created on Wed Sep 28 12:35:20 2022

@author: ruppert20
"""
import os
import pandas as pd
from typing import Union
from sqlalchemy.engine.base import Engine
from scipy.sparse.csgraph import connected_components
from sqlalchemy.orm import sessionmaker
from typing import List
from tqdm import tqdm
from .log_messages import log_print_message as logm, _start_logging_to_file

try:
    from ...Utilities.FileHandling.variable_specification_utilities import load_variables_from_var_spec
    from ...Utilities.FileHandling.io import check_load_df
    local_mode_possible: bool = True
except ImportError or ModuleNotFoundError:
    local_mode_possible: bool = False
    pass

# TODO: update documentation string for calculate_SOFA


def calculate_SOFA(engine: Engine,
                   cohort_id: int,
                   data_schema: str = None,
                   tempTableSchema: str = None,
                   results_schema: str = None,
                   lookup_schema: str = None,
                   vocab_schema: str = None,
                   lookup_table: str = None,
                   drug_lookup_table: str = None,
                   keep_zeros: bool = True,
                   subset_id: int = None,
                   sofa_start: str = 'visit_start_datetime',
                   sofa_stop: str = 'visit_end_datetime',
                   continue_n_hours_after_stop: int = None,
                   sofa_frequency: str = '1 h',
                   ff_limit_hours: int = None,
                   custom_table_joins: str = None,
                   save_folder_path: Union[str, None] = None,
                   sofa_versions: List[str] = ['SOFA', 'qSOFA', 'eSOFA'],
                   local_mode: bool = False,
                   dir_dict: dict = None,
                   aki_phenotype_data_key: str = 'intermediate_data',
                   data_source_key: str = 'source_data',
                   var_file_linkage_fp: str = 'variable_file_link',
                   append_subject_id_type_if_missing: Union[str, None] = None,
                   log_dir: str = None,
                   **logging_kwargs) -> pd.DataFrame:
    """
    Calculate SOFA scores (eSOFA, SOFA, and/or qSOFA) on specified visits using an OMOP v5.3 or v5.4 instance.

    Parameters
    ----------
    engine : sqlalchemy.engine.base
        This is the connection used to create temporary tables and read from the database.
    cohort_id : int
        The cohort_definition_id from the COHORT Table. This is typically found in the RESULTS schema, but may be found in other places depending on your OMOP Database configuration.
    data_schema : str
        The Schema where patient data is stored. This is typically called CDM.
    tempTableSchema : str
        A Schema in the database where the user associated with the engine database connection can write tables to. A single table will be created and later removed.
    results_schema : str
        The Schema where the COHORT and COHORT Definition tables can be found.
    lookup_schema : str
        The Schema where the IC3_Variable_Lookup_Table_v2_beta and IC3_Drug_Dose_Lookup table can be found.
    vocab_schema : str
        The Schema where the OMOP Vocabulary tables downloaded from ATHENA can be found. These include tables such as CONCEPT and CONCEPT_ANCESTOR.
    keep_zeros : bool, optional
        Whether sub_scores that are assigned the value of zero are preserved in the output DataFrame.
        The default is True, where the subscore zeros will be retained. When set to false, the subscore zeros will be set to NULL.
        The final scores (SOFA, qSOFA, eSOFA) will always be retained.
    subset_id : int, optional
        If subsets are employed in the project. This field may be used to generated SOFA scores for a portion of the cohort. The default is None.
    sofa_start : str, optional
        The name of the column that corresponds to when the SOFA score generation should begin. The default is 'visit_start_datetime'.
        Out of the Box supported options include 'visit_detail_start_datetime', 'visit_start_datetime', AND 'cohort_start_date'.
        A custom start can be used if the table is joined to the cohort table using the custom_table_joins parameter.
    sofa_stop : str, optional
        The name of the column that corresponds to when the SOFA score generation should stop. The default is 'visit_end_datetime'.
        Out of the Box supported options include 'visit_detail_end_datetime', 'visit_end_datetime', AND 'cohort_end_date'.
        A custom stop can be used if the table is joined to the cohort table using the custom_table_joins parameter.
    continue_n_hours_after_stop : int, optional
        This parameter allows the sofa calculation to continue n hours past the timepoint specified by sofa_stop.
        The default is None, where sofa calculation stops the hour corresponding to the stop timepoint.
        This parameter might be helpful if you wanted a SOFA score from admission until 24 Hours after the surgery, where the surgery is identified by a procedure_occurrence_id or visit_detail_id in the cohort table.
    sofa_frequency : str, optional
        The number of hours between score calculations. The default is '1 h', which will generate a SOFA score for each hour.
        ***This parameter should not be modified until further testing and modification is done to this function.***
    ff_limit_hours : int, optional
        The number of hours values may be filled forward for SOFA score calculation.
        When values are present the worst value in 24 hours is used; however, when there is no value within the past 24 hours, the last observed value will be used.
        This parameter may be used to limit the extend in which values may be fed forward. The default is None, which will used the last observed value for each subscore.
        **This does not apply to the mechical ventilation or vasopressor scores which have defined start and end times.**
    custom_table_joins : str, optional
        A custom join argument which allows additional tables to be joined to the cohort table in order to use custom sofa_start and sofa_stop arguments.
        The default is None.
    save_folder_path: Union[str, None], Optional
        The directory where the output_file should be saved.
    sofa_versions : List[str], optional
        Which versions of SOFA scores should be computed. The default is ['SOFA', 'qSOFA', 'eSOFA'], which will run all versions of SOFA.
        **Due to similarities between them it is most effiicent to calculate all versions one wishes to employ in their project at once instead of running the function multiple times for each score.**

    Returns
    -------
    pd.DataFrame
        Pandas dataframe containing the specifed SOFA score versions for the specified period.

     Requirements
     ------------
     The syntax is based on TSQL (MICROSOFT SQL SERVER), support for alternative SQL dialects can be added upon request.

     The following Tables/Fields are required to be populated.

     Person
         *person_id
         *race_concept_id
         *gender_concept_id
         **birthdatetime

     Visit Occurrence:
         *person_id
         **visit_start_datetime
         **visit_end_datetime

     Visit Detail:
        *person_id
        **visit_occurrence_id
        **visit_detail_id
        **visit_detail_concept_id

     (**Denotes non-required fields/tables per the CDM, but required for this code)

    """
    if isinstance(log_dir, str):
        assert os.path.exists(str)
        assert os.path.isdir(str)
        _start_logging_to_file(directory=log_dir, file_name=f'{"_".join(sofa_versions)}_cohort_{cohort_id}{("_" + str(subset_id)) if isinstance(subset_id, (str, int)) else ""}.log')
    if isinstance(save_folder_path, str):
        assert os.path.exists(save_folder_path)
        assert os.path.isdir(save_folder_path)
        out_path: str = os.path.join(save_folder_path, f'{"_".join(sofa_versions)}_cohort_{cohort_id}{("_" + str(subset_id)) if isinstance(subset_id, (str, int)) else ""}.csv')

        # skip if already done
        if os.path.exists(out_path):
            return

    if 'connect_to_database.omop_engine_bundle' in str(type(engine)):
        data_schema: str = engine.data_schema
        tempTableSchema: str = engine.operational_schema
        results_schema: str = engine.results_schema
        lookup_schema: str = engine.lookup_schema
        vocab_schema: str = engine.vocab_schema
        lookup_table: str = engine.lookup_table
        drug_lookup_table: str = engine.drug_lookup_table
        engine: Engine = engine.engine

    assert isinstance(data_schema, str)
    assert isinstance(tempTableSchema, str)
    assert isinstance(results_schema, str)
    assert isinstance(lookup_schema, str)
    assert isinstance(vocab_schema, str)
    assert isinstance(lookup_table, str)
    assert isinstance(drug_lookup_table, str)
    assert isinstance(engine, Engine)

    if local_mode:
        if not local_mode_possible:
            raise Exception('unable to locate the required dependency: "from ...Utilities.FileHandling.variable_specification_utilities import load_variables_from_var_spec". Please clone the "https://github.com/Prisma-pResearch/Utilities"  repository in the directory where the sofa module is located and try again.')
        else:
            assert isinstance(dir_dict, dict)
            assert os.path.exists(dir_dict.get(data_source_key))
            assert os.path.isdir(dir_dict.get(data_source_key))
            assert os.path.exists(dir_dict.get(var_file_linkage_fp))
            assert os.path.isfile(dir_dict.get(var_file_linkage_fp))

    modes: List[str] = ['SOFA', 'qSOFA', 'eSOFA']
    subject_id_types: List[str] = ['visit_occurrence_id', 'visit_detail_id', 'procedure_occurrence_id']
    supported_start_cols: List[str] = ['visit_detail_start_datetime', 'visit_start_datetime', 'cohort_start_date']
    supported_stop_cols: List[str] = ['visit_detail_end_datetime', 'visit_end_datetime', 'cohort_end_date']
    # durations: List[str] = ['visit', 'visit_detail']
    # required_columns: List[str] = ['visit_occurrence_id', 'period_start_datetime', 'period_end_datetime']

    selected_modes: List[str] = [x for x in sofa_versions if x in modes]

    subject_id_type: str = pd.read_sql(f"SELECT CONCAT(concept_name, '_id') [id_type] FROM {results_schema}.COHORT_DEFINITION cd INNER JOIN {vocab_schema}.CONCEPT c on cd.subject_concept_id = c.concept_id WHERE cohort_definition_id = {cohort_id}", con=engine)
    if subject_id_type.shape[0] == 1:
        subject_id_type: str = subject_id_type.id_type[0]
    else:
        raise Exception('There is a problem with the COHORT Definition entry. Either the subject_concept_id is missing/invalid or there are multiple records in the cohort_defintion table for the specified cohort_id.')

    # check subject type
    assert subject_id_type in subject_id_types, f'The subject_id_type: {subject_id_type} is not supported. Please choose one of the following: {subject_id_types}'

    # check starting point
    assert (((sofa_start in supported_start_cols)
             and
             (subject_id_type in ['visit_detail_id', 'procedure_occurrence_id'])) or
            ((sofa_start == 'visit_start_datetime')
             and
                (subject_id_type == 'visit_occurrence_id')) or
            ((sofa_start == 'cohort_start_date')

                and (subject_id_type == 'person_id')) or
            isinstance(custom_table_joins, str)), f'The specified sofa_start: {sofa_start} is not in one of the built in supported starting points: {supported_start_cols}. Please use one of the pre-supported start columns or add a custom_table_joins specification'

    # check if sofa_stop is for a period of time since start
    try:
        first_n_hours: int = int(pd.to_timedelta(sofa_stop, errors='raise').total_seconds() / 3600)
    except:
        first_n_hours: int = None
    assert (((sofa_stop in supported_stop_cols)
             and
             (subject_id_type in ['visit_detail_id', 'procedure_occurrence_id'])) or
            ((sofa_stop == 'visit_end_datetime')
             and
                (subject_id_type == 'visit_occurrence_id')) or
            ((sofa_stop == 'cohort_end_date')

                and (subject_id_type == 'person_id')) or
            isinstance(first_n_hours, int) or
            isinstance(custom_table_joins, str)), f'The specified sofa_stop: {sofa_stop} is not in one of the built in supported stopping points: {supported_stop_cols}. Please use one of the pre-supported stop columns, a time period since the start (e.g. 72 h) or add a custom_table_joins specification'

    ### Check input Validity ###
    try:
        pd.to_timedelta(sofa_frequency, errors='raise')
    except:
        raise AssertionError(f'The entered sofa_frequency: {sofa_frequency} is invalid.')

    assert len(selected_modes) > 0, f'None of the sofa_versions entered: {sofa_versions} are valid. Please choose one or more from the following: {modes}'

    if 'display' not in list(logging_kwargs.keys()):
        logging_kwargs['display'] = True

    if local_mode:
        batch_id: str = f'{cohort_id}_chunk_{subset_id}' if isinstance(subset_id, (int, str)) else str(cohort_id)
        patterns: List[str] = [r'_{}\.csv'.format(batch_id), r'_{}_chunk_[0-9]+\.csv'.format(batch_id), r'_{}_[0-9]+\.csv'.format(batch_id), r'_[0-9]+_chunk_[0-9]+\.csv', r'_[0-9]+_[0-9]+\.csv', r'_[0-9]+\.csv', r'\.csv']
        temp_table_name: str = None

        local_kwargs: dict = {'dir_dict': dir_dict,
                              'var_spec_key': var_file_linkage_fp,
                              'data_source_key': data_source_key,
                              'patterns': patterns,
                              'project': 'SOFA'}

        local_kwargs.update(logging_kwargs)

        visit_df = load_variables_from_var_spec(variables_columns=['visit_start_datetime', 'visit_end_datetime'],
                                                **local_kwargs,
                                                append_subject_id_type_if_missing=append_subject_id_type_if_missing,
                                                cdm_tables=['visit_occurrence'],
                                                mute_duplicate_var_warnings=True,
                                                desired_types={**{x: 'datetime' for x in ['visit_start_datetime', 'visit_end_datetime']},
                                                               **{x: 'sparse_int' for x in ['person_id', 'visit_occurrence_id', 'subject_id']}},
                                                allow_empty_files=True, regex=True, dtype=None, ds_type='pandas').drop_duplicates(subset=['subject_id'])

        if (sofa_start in ['visit_detail_start_datetime', 'visit_detail_end_datetime']) or (sofa_stop in ['visit_detail_start_datetime', 'visit_detail_end_datetime']):
            visit_df = visit_df.merge(load_variables_from_var_spec(variables_columns=['visit_detail_start_datetime', 'visit_detail_end_datetime'],
                                                                   cdm_tables=['visit_detail'],
                                                                   mute_duplicate_var_warnings=True,
                                                                   desired_types={**{x: 'datetime' for x in ['visit_detail_start_datetime', 'visit_detail_end_datetime']},
                                                                                  **{x: 'sparse_int' for x in ['person_id', 'visit_occurrence_id', 'subject_id']}},
                                                                   **local_kwargs,
                                                                   allow_empty_files=True, regex=True, dtype=None, ds_type='pandas').drop_duplicates(subset=['subject_id']),
                                      how='inner',
                                      on=['person_id', 'visit_occurrence_id', 'subject_id'])

        if isinstance(first_n_hours, int):
            visit_df['period_end'] = visit_df[sofa_start] + pd.to_timedelta(f'{first_n_hours} Hours')
        elif isinstance(continue_n_hours_after_stop, int):
            visit_df['period_end'] = visit_df[sofa_stop] + pd.to_timedelta(f'{continue_n_hours_after_stop} Hours')
        else:
            visit_df['period_end'] = visit_df[sofa_stop]

        visit_df['period_start'] = visit_df[sofa_start]

    else:
        local_kwargs: dict = {}
        logm('establishing temporary SOFA table', **logging_kwargs)
        vo_id_source: str = 'vo' if subject_id_type == 'visit_occurrence_id' else 'vd' if subject_id_type == 'visit_detail_id' else 'po' if subject_id_type == 'procedure_occurrence_id' else ''

        stop_col: str = f'DATEADD(HOUR, {first_n_hours}, {sofa_start})' if isinstance(first_n_hours, int) else f'DATEADD(HOUR, {continue_n_hours_after_stop}, {sofa_stop})' if isinstance(continue_n_hours_after_stop, int) else sofa_stop
        subject_id_table: str = (f'INNER JOIN {data_schema}.' +
                                 ('PERSON p on p.person_id = c.subject_id' if subject_id_type == 'person_id' else
                                  'VISIT_OCCURRENCE vo on vo.visit_occurrence_id = c.subject_id' if subject_id_type == 'visit_occurrence_id' else
                                  'VISIT_DETAIL vd on vd.visit_detail_id = c.subject_id' if subject_id_type == 'visit_detail_id' else
                                  'PROCEDURE_OCCURRENCE po on po.procedure_occurrence_id = c.subject_id' if subject_id_type == 'procedure_occurrence_id' else ''))

        vo_join: str = f'INNER JOIN {data_schema}.VISIT_OCCURRENCE vo on {vo_id_source}.visit_occurrence_id = vo.visit_occurrence_id' if ((subject_id_type in ['visit_detail_id', 'procedure_occurrence_id'])

                                                                                                                                          and ((sofa_stop == 'visit_end_datetime') or
                                                                                                                                               (sofa_start == 'visit_start_datetime'))) else ''
        p_join: str = f'INNER JOIN {data_schema}.PERSON p on {vo_id_source}.person_id = p.person_id' if subject_id_type != 'person_id' else ''

        # check if a database is specified using '.' syntax
        temp_db_name: str = tempTableSchema.split('.')[0] if '.' in tempTableSchema else ''
        temp_schema: str = tempTableSchema.split('.')[1] if '.' in tempTableSchema else tempTableSchema

        temp_table_name: str = f'temp_SOFA_table_{cohort_id}_{subset_id}'

        # check if temp table exists already
        if pd.read_sql(
                f"SELECT * FROM {(temp_db_name + '.') if temp_db_name != '' else ''}INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{temp_table_name}' AND TABLE_SCHEMA = '{temp_schema}'", engine).shape[0] > 0:
            pass
        else:
            # create the final temp table
            execute_query_in_transaction(query=f'''SELECT
                                                        {(vo_id_source + '.person_id,') if subject_id_type != 'person_id' else 'subject_id,'}
                                                        {(vo_id_source + '.visit_occurrence_id,') if isinstance(vo_id_source, str) else ''}
                                                        {(subject_id_type + ',') if subject_id_type in ['visit_detail_id', 'procedure_occurrence_id'] else ''}
                                                        {sofa_start} [period_start] ,
                                                        {stop_col} [period_end] ,
                                                        DATEDIFF (year , p.birth_datetime , {sofa_start} ) [age],
                                                        CASE
                                                            WHEN p.gender_concept_id = 8532 THEN 1
                                                            ELSE 0 END [female],
                                                        CASE
                                                            WHEN p.race_concept_id IN (38003598, 38003599) THEN 1
                                                            ELSE 0 END [black]
                                                    INTO
                                                        {tempTableSchema}.{temp_table_name}
                                                    FROM
                                                        {results_schema}.COHORT c
                                                        {subject_id_table}
                                                        {vo_join}
                                                        {p_join}
                                                    WHERE
                                                        cohort_definition_id = {cohort_id}
                                                        {('AND subset_id = ' + str(subset_id)) if isinstance(subset_id, int) else ''};''',
                                         engine=engine)

        # pull visit_table
        visit_df = pd.read_sql(f'SELECT * FROM {tempTableSchema}.{temp_table_name}', engine)

    ### determine renal score ###
    if ('SOFA' in selected_modes) or ('eSOFA' in selected_modes):
        logm('Pulling visit Creatinines', **logging_kwargs)
        # pull creatinine labs for visit_occurrence
        visit_creatinine: pd.DataFrame = _get_visit_labs(engine=engine, lab_type='creatinine', data_schema=data_schema,
                                                         lookup_schema=lookup_schema, lookup_table=lookup_table,
                                                         append_subject_id_type_if_missing=append_subject_id_type_if_missing,
                                                         tempTableSchema=tempTableSchema, temp_table_name=temp_table_name, visit_df=visit_df.copy(deep=True) if local_mode else None, **local_kwargs)

        logm('Pulling ESRD information', **logging_kwargs)
        # check if aki phenotyping has already been ran on the subset, if so then include that result
        if local_mode and isinstance(dir_dict.get(aki_phenotype_data_key), str):
            if os.path.exists(dir_dict.get(aki_phenotype_data_key)) and os.path.isdir(dir_dict.get(aki_phenotype_data_key)):
                esrd_df_pt1: pd.DataFrame = check_load_df('visit_occurrence_id_without_race_correction_v2_final_aki',
                                                          usecols=['person_id', 'inferred_specimen_datetime', 'final_class'],
                                                          directory=dir_dict.get(aki_phenotype_data_key),
                                                          patterns=patterns,
                                                          allow_empty_files=True,
                                                          desired_types={'inferred_specimen_datetime': 'datetime'},
                                                          ds_type='pandas',
                                                          **logging_kwargs)\
                    .merge(visit_df[['person_id', 'period_start', 'visit_occurrence_id']], on='person_id', how='inner')\
                    .query('inferred_specimen_datetime <= period_start')\
                    .drop(columns=['inferred_specimen_datetime', 'person_id', 'period_start'])\
                    .query('final_class.str.contains("ESRD", case=False, regex=False, na=False)', engine='python')\
                    .rename(columns={'final_class': 'esrd'})

                esrd_df_pt1.esrd = '1' if esrd_df_pt1.shape[0] > 0 else None
            else:
                esrd_df_pt1 = None

        else:
            esrd_df_pt1 = None

        esrd_df_pt2: pd.DataFrame = _determine_esrd_status(data_schema=data_schema, tempTableSchema=tempTableSchema, engine=engine,
                                                           lookup_schema=lookup_schema, temp_table_name=temp_table_name, lookup_table=lookup_table,
                                                           visit_df=visit_df.copy(deep=True) if local_mode else None, **local_kwargs)

        # take the ESRD status from either condition or phenotype source
        esrd_df: pd.DataFrame = pd.concat([esrd_df_pt1, esrd_df_pt2], axis=0, ignore_index=True).drop_duplicates() if isinstance(esrd_df_pt1, pd.DataFrame) else esrd_df_pt2

        if 'eSOFA' in selected_modes:
            logm('Calculating eSOFA Renal Score', **logging_kwargs)

            eSOFA_renal_scores = _compute_lab_ratio(baseline_df=_get_baseline_labs(engine=engine, lab_type='creatinine',
                                                                                   data_schema=data_schema,
                                                                                   lookup_schema=lookup_schema,
                                                                                   tempTableSchema=tempTableSchema,
                                                                                   temp_table_name=temp_table_name,
                                                                                   lookup_table=lookup_table,
                                                                                   append_subject_id_type_if_missing=append_subject_id_type_if_missing,
                                                                                   visit_df=visit_df.copy(deep=True) if local_mode else None,
                                                                                   **local_kwargs),
                                                    visit_df=visit_creatinine,
                                                    merging_key=['visit_occurrence_id'],
                                                    lab_type='creatinine',
                                                    min_thresholds_for_score=1,
                                                    minimum_ratio_threshold=2.0,
                                                    score_label='eSOFA_renal_score')

            if esrd_df.shape[0] > 0:
                # set renal score to 1 for ESRD patients
                eSOFA_renal_scores = eSOFA_renal_scores.merge(esrd_df, on='visit_occurrence_id', how='left')
                esrd_mask: pd.Series = (eSOFA_renal_scores.esrd == 1)

                if esrd_mask.any():
                    eSOFA_renal_scores.loc[esrd_mask, 'eSOFA_renal_score'] = 1
                eSOFA_renal_scores.drop(columns=['esrd'], inplace=True)
                del esrd_mask

        if 'SOFA' in selected_modes:
            logm('Calculating SOFA Renal Score', **logging_kwargs)
            SOFA_renal_scores = visit_creatinine.copy(deep=True)
            SOFA_renal_scores['sofa_renal'] = None
            SOFA_renal_scores.loc[(SOFA_renal_scores['creatinine'] > 5), 'sofa_renal'] = 4
            SOFA_renal_scores.loc[(pd.isnull(SOFA_renal_scores['sofa_renal'])) & (SOFA_renal_scores['creatinine'] >= 3.5), 'sofa_renal'] = 3
            SOFA_renal_scores.loc[(pd.isnull(SOFA_renal_scores['sofa_renal'])) & (SOFA_renal_scores['creatinine'] >= 2), 'sofa_renal'] = 2
            SOFA_renal_scores.loc[(pd.isnull(SOFA_renal_scores['sofa_renal'])) & (SOFA_renal_scores['creatinine'] >= 1.2), 'sofa_renal'] = 1
            SOFA_renal_scores['sofa_renal'].fillna(0, inplace=True)

            if esrd_df.shape[0] > 0:
                # set renal score to 1 for ESRD patients
                SOFA_renal_scores = SOFA_renal_scores.merge(esrd_df, on='visit_occurrence_id', how='left')
                esrd_mask: pd.Series = (SOFA_renal_scores.esrd == 1)

                if esrd_mask.any():
                    SOFA_renal_scores.loc[esrd_mask, 'sofa_renal'] = 4
                SOFA_renal_scores.drop(columns=['esrd'], inplace=True)
                del esrd_mask

        del esrd_df

    if ('qSOFA' in selected_modes):
        logm('Calculating qSOFA Respiratory Score', **logging_kwargs)
        # pull respiratory rate information from server and calculate when it is >= 22
        qSOFA_resp_scores: pd.DataFrame = _get_visit_labs(engine=engine, lab_type='respiratory_rate', data_schema=data_schema,
                                                          lookup_schema=lookup_schema,
                                                          tempTableSchema=tempTableSchema, temp_table_name=temp_table_name,
                                                          lookup_table=lookup_table,
                                                          append_subject_id_type_if_missing=append_subject_id_type_if_missing,
                                                          visit_df=visit_df.copy(deep=True) if local_mode else None,
                                                          **local_kwargs).query('respiratory_rate > 0')
        qSOFA_resp_scores['qSOFA_resp_score'] = (qSOFA_resp_scores.respiratory_rate >= 22).astype(int).copy(deep=True)
        qSOFA_resp_scores.drop(columns=['respiratory_rate'], inplace=True)

        logm('Calculating qSOFA Systolic BP Score', **logging_kwargs)
        # pull systolic BP from server and calcualte Systolic BP ≤100
        qSOFA_bp_scores: pd.DataFrame = _get_visit_labs(engine=engine, lab_type='systolic_blood_pressure', data_schema=data_schema,
                                                        lookup_schema=lookup_schema,
                                                        append_subject_id_type_if_missing=append_subject_id_type_if_missing,
                                                        tempTableSchema=tempTableSchema, temp_table_name=temp_table_name,
                                                        lookup_table=lookup_table,
                                                        visit_df=visit_df.copy(deep=True) if local_mode else None,
                                                        **local_kwargs).query('systolic_blood_pressure > 0')
        qSOFA_bp_scores['qSOFA_bp_score'] = (qSOFA_bp_scores.systolic_blood_pressure <= 100).astype(int)
        qSOFA_bp_scores.drop(columns=['systolic_blood_pressure'], inplace=True)

    if ('SOFA' in selected_modes) or ('eSOFA' in selected_modes):
        logm('Pulling bilirubin total', **logging_kwargs)

        visit_billirubin: pd.DataFrame = _get_visit_labs(engine=engine, lab_type='bilirubin_total', data_schema=data_schema,
                                                         tempTableSchema=tempTableSchema,
                                                         temp_table_name=temp_table_name, lookup_schema=lookup_schema,
                                                         lookup_table=lookup_table,
                                                         append_subject_id_type_if_missing=append_subject_id_type_if_missing,
                                                         visit_df=visit_df.copy(deep=True) if local_mode else None,
                                                         **local_kwargs)

        if 'SOFA' in selected_modes:
            logm('Calculating SOFA Hepatic Score', **logging_kwargs)
            SOFA_hepatic_scores = visit_billirubin.copy(deep=True)
            SOFA_hepatic_scores['SOFA_hepatic'] = None
            SOFA_hepatic_scores.loc[(pd.isnull(SOFA_hepatic_scores['SOFA_hepatic'])) & (SOFA_hepatic_scores['bilirubin_total'] > 12), 'SOFA_hepatic'] = 4
            SOFA_hepatic_scores.loc[(pd.isnull(SOFA_hepatic_scores['SOFA_hepatic'])) & (SOFA_hepatic_scores['bilirubin_total'] >= 6), 'SOFA_hepatic'] = 3
            SOFA_hepatic_scores.loc[(pd.isnull(SOFA_hepatic_scores['SOFA_hepatic'])) & (SOFA_hepatic_scores['bilirubin_total'] >= 2), 'SOFA_hepatic'] = 2
            SOFA_hepatic_scores.loc[(pd.isnull(SOFA_hepatic_scores['SOFA_hepatic'])) & (SOFA_hepatic_scores['bilirubin_total'] >= 1.2), 'SOFA_hepatic'] = 1
            SOFA_hepatic_scores['SOFA_hepatic'].fillna(0, inplace=True)

        if 'eSOFA' in selected_modes:
            # determine hepatic score
            logm('Calculating eSOFA Hepatic Score', **logging_kwargs)
            eSOFA_hepatic_scores = _compute_lab_ratio(baseline_df=_get_baseline_labs(engine=engine, lab_type='bilirubin_total', data_schema=data_schema, lookup_schema=lookup_schema,
                                                                                     tempTableSchema=tempTableSchema, temp_table_name=temp_table_name,
                                                                                     lookup_table=lookup_table,
                                                                                     append_subject_id_type_if_missing=append_subject_id_type_if_missing,
                                                                                     visit_df=visit_df.copy(deep=True) if local_mode else None,
                                                                                     **local_kwargs),
                                                      visit_df=visit_billirubin,
                                                      merging_key=['visit_occurrence_id'],
                                                      lab_type='bilirubin_total',
                                                      min_thresholds_for_score=2,
                                                      minimum_absolute_thresh=2.0,
                                                      minimum_ratio_threshold=2.0,
                                                      default_baseline_value=1.2,
                                                      score_label='eSOFA_hepatic_score')

    ### determine coagulation score ###
    if ('SOFA' in selected_modes) or ('eSOFA' in selected_modes):
        logm('Pulling Platelets', **logging_kwargs)

        visit_platelets: pd.DataFrame = _get_visit_labs(engine=engine, lab_type='platelets', data_schema=data_schema, 
                                                        lookup_schema=lookup_schema,
                                                        tempTableSchema=tempTableSchema, temp_table_name=temp_table_name,
                                                        lookup_table=lookup_table,
                                                        append_subject_id_type_if_missing=append_subject_id_type_if_missing,
                                                        visit_df=visit_df.copy(deep=True) if local_mode else None,
                                                        **local_kwargs)

        if 'SOFA' in selected_modes:
            logm('Calculating SOFA Coagulation Score', **logging_kwargs)
            SOFA_coag_scores: pd.DataFrame = visit_platelets.copy(deep=True)
            SOFA_coag_scores['SOFA_coag_score'] = None
            SOFA_coag_scores.loc[(pd.isnull(SOFA_coag_scores['SOFA_coag_score'])) & (SOFA_coag_scores['platelets'] < 20), 'SOFA_coag_score'] = 4
            SOFA_coag_scores.loc[(pd.isnull(SOFA_coag_scores['SOFA_coag_score'])) & (SOFA_coag_scores['platelets'] < 50), 'SOFA_coag_score'] = 3
            SOFA_coag_scores.loc[(pd.isnull(SOFA_coag_scores['SOFA_coag_score'])) & (SOFA_coag_scores['platelets'] < 100), 'SOFA_coag_score'] = 2
            SOFA_coag_scores.loc[(pd.isnull(SOFA_coag_scores['SOFA_coag_score'])) & (SOFA_coag_scores['platelets'] < 150), 'SOFA_coag_score'] = 1
            SOFA_coag_scores['SOFA_coag_score'].fillna(0, inplace=True)

        if 'eSOFA' in selected_modes:

            logm('Calculating eSOFA Coagulation score', **logging_kwargs)
            coag_scores = _compute_lab_ratio(baseline_df=_get_baseline_labs(engine=engine, lab_type='platelets', data_schema=data_schema, lookup_schema=lookup_schema,
                                                                            tempTableSchema=tempTableSchema, temp_table_name=temp_table_name,
                                                                            lookup_table=lookup_table,
                                                                            append_subject_id_type_if_missing=append_subject_id_type_if_missing,
                                                                            visit_df=visit_df.copy(deep=True) if local_mode else None,
                                                                            **local_kwargs),
                                             visit_df=visit_platelets,
                                             merging_key=['visit_occurrence_id'],
                                             lab_type='platelets',
                                             min_thresholds_for_score=2,
                                             maximum_absolute_thresh=100,
                                             maximum_ratio_threshold=0.5,
                                             default_baseline_value=300,
                                             minimum_baseline_value=100,
                                             score_label='eSOFA_coag_score')

    ### determine perfusion score ###
    if 'eSOFA' in selected_modes:
        logm('Calculating eSOFA Perfusion Score', **logging_kwargs)
        perfusion_scores: pd.DataFrame = _get_visit_labs(engine=engine, lab_type='lactate', data_schema=data_schema,
                                                         lookup_schema=lookup_schema,
                                                         tempTableSchema=tempTableSchema, temp_table_name=temp_table_name,
                                                         append_subject_id_type_if_missing=append_subject_id_type_if_missing,
                                                         lookup_table=lookup_table,
                                                         visit_df=visit_df.copy(deep=True) if local_mode else None,
                                                         **local_kwargs)

    ### determine pressor score ###
    if 'eSOFA' in selected_modes:
        logm('Calculating eSOFA Pressor Score', **logging_kwargs)
        pressor_scores = _pressors_for_visit(engine=engine, data_schema=data_schema, tempTableSchema=tempTableSchema,
                                             append_subject_id_type_if_missing=append_subject_id_type_if_missing,
                                             lookup_schema=lookup_schema, temp_table_name=temp_table_name, lookup_table=lookup_table,
                                             visit_df=visit_df.copy(deep=True) if local_mode else None,
                                             **local_kwargs)

    ### determine mechanical ventilation score ###
    if ('eSOFA' in selected_modes):
        logm('Calculating eSOFA Mechanical Ventilation Score', **logging_kwargs)
        mv_scores = _mv_for_visit(engine=engine, data_schema=data_schema, tempTableSchema=tempTableSchema,
                                  temp_table_name=temp_table_name, mode='eSOFA',
                                  append_subject_id_type_if_missing=append_subject_id_type_if_missing,
                                  lookup_schema=lookup_schema, lookup_table=lookup_table,
                                  visit_df=visit_df.copy(deep=True) if local_mode else None,
                                  **local_kwargs)

    ### determine Mental score ###
    if ('SOFA' in selected_modes) or ('qSOFA' in selected_modes):
        logm('PUlling GCS Scores', **logging_kwargs)
        visit_gcs_scores: pd.DataFrame = _get_visit_labs(engine=engine, lab_type='gcs_score', data_schema=data_schema,
                                                         lookup_schema=lookup_schema,
                                                         append_subject_id_type_if_missing=append_subject_id_type_if_missing,
                                                         tempTableSchema=tempTableSchema, temp_table_name=temp_table_name,
                                                         lookup_table=lookup_table,
                                                         visit_df=visit_df.copy(deep=True) if local_mode else None,
                                                         **local_kwargs)

        if 'SOFA' in selected_modes:
            logm('Calculating SOFA CNS Score', **logging_kwargs)
            SOFA_cns_scores: pd.DataFrame = visit_gcs_scores.copy(deep=True)

            SOFA_cns_scores['SOFA_cns_score'] = None
            SOFA_cns_scores.loc[(pd.isnull(SOFA_cns_scores['SOFA_cns_score'])) & (SOFA_cns_scores['gcs_score'] < 6), 'SOFA_cns_score'] = 4
            SOFA_cns_scores.loc[(pd.isnull(SOFA_cns_scores['SOFA_cns_score'])) & (SOFA_cns_scores['gcs_score'] <= 9), 'SOFA_cns_score'] = 3
            SOFA_cns_scores.loc[(pd.isnull(SOFA_cns_scores['SOFA_cns_score'])) & (SOFA_cns_scores['gcs_score'] <= 12), 'SOFA_cns_score'] = 2
            SOFA_cns_scores.loc[(pd.isnull(SOFA_cns_scores['SOFA_cns_score'])) & (SOFA_cns_scores['gcs_score'] <= 14), 'SOFA_cns_score'] = 1
            SOFA_cns_scores['SOFA_cns_score'].fillna(0, inplace=True)

        if 'qSOFA' in selected_modes:
            logm('Calculating qSOFA CNS Score', **logging_kwargs)
            qSOFA_cns_scores: pd.DataFrame = visit_gcs_scores.copy(deep=True)

            qSOFA_cns_scores['qSOFA_cns_score'] = None
            qSOFA_cns_scores.loc[(SOFA_cns_scores['gcs_score'] < 15), 'qSOFA_cns_score'] = 1
            qSOFA_cns_scores['qSOFA_cns_score'].fillna(0, inplace=True)

    ### determine resp score ###
    if ('SOFA' in selected_modes):
        logm('Calculating SOFA Respiratory Score', **logging_kwargs)
        # Pull PaO2, fio2, spo2
        visit_pao2: pd.DataFrame = _get_visit_labs(engine=engine, lab_type='pao2', data_schema=data_schema, lookup_schema=lookup_schema,
                                                   tempTableSchema=tempTableSchema, temp_table_name=temp_table_name,
                                                   lookup_table=lookup_table,
                                                   append_subject_id_type_if_missing=append_subject_id_type_if_missing,
                                                   visit_df=visit_df.copy(deep=True) if local_mode else None,
                                                   **local_kwargs)\
            .sort_values('measurement_datetime', ascending=True)

        visit_spo2: pd.DataFrame = _get_visit_labs(engine=engine, lab_type='spo2', data_schema=data_schema, lookup_schema=lookup_schema,
                                                   tempTableSchema=tempTableSchema, temp_table_name=temp_table_name,
                                                   lookup_table=lookup_table,
                                                   append_subject_id_type_if_missing=append_subject_id_type_if_missing,
                                                   visit_df=visit_df.copy(deep=True) if local_mode else None,
                                                   **local_kwargs)\
            .sort_values('measurement_datetime', ascending=True)

        visit_fio2: pd.DataFrame = _get_visit_labs(engine=engine, lab_type='fio2', data_schema=data_schema, lookup_schema=lookup_schema,
                                                   append_subject_id_type_if_missing=append_subject_id_type_if_missing,
                                                   tempTableSchema=tempTableSchema, temp_table_name=temp_table_name,
                                                   lookup_table=lookup_table,
                                                   visit_df=visit_df.copy(deep=True) if local_mode else None,
                                                   **local_kwargs)\
            .sort_values('measurement_datetime', ascending=True)

        # Calculate PF
        visit_pf = pd.merge_asof(visit_pao2, visit_fio2, on='measurement_datetime', by='visit_occurrence_id', direction='backward')
        visit_pf['fio2'].fillna(21, inplace=True)
        visit_pf['pf'] = visit_pf['pao2'] / (visit_pf['fio2'] / 100)
        visit_pf.drop(columns=['pao2', 'fio2'], inplace=True)

        # Calculate SPF
        visit_spf = pd.merge_asof(visit_spo2, visit_fio2, on='measurement_datetime', by='visit_occurrence_id', direction='backward')
        visit_spf['fio2'].fillna(21, inplace=True)
        visit_spf['spf'] = (visit_spf['spo2'] / (visit_spf['fio2'] / 100) - 64) / 0.84
        visit_spf.drop(columns=['spo2', 'fio2'], inplace=True)
        visit_spf = visit_spf[visit_spf['spf'] > 0]

        # get mv for visit
        sofa_mv = _mv_for_visit(engine=engine, data_schema=data_schema, tempTableSchema=tempTableSchema, temp_table_name=temp_table_name, mode='SOFA',
                                lookup_schema=lookup_schema, lookup_table=lookup_table,
                                visit_df=visit_df.copy(deep=True) if local_mode else None,
                                **local_kwargs)

        SOFA_resp_score: pd.DataFrame = visit_spf.merge(visit_pf, on=['visit_occurrence_id', 'measurement_datetime'], how='outer')\
            .merge(sofa_mv.rename(columns={'device_exposure_start_datetime': 'measurement_datetime'}),
                   on=['visit_occurrence_id', 'measurement_datetime'], how='outer')

        SOFA_resp_score[['pf_score', 'spf_score']] = None

        SOFA_resp_score.loc[((SOFA_resp_score.pf < 100) & SOFA_resp_score.pf_score.isnull()), 'pf_score'] = 4
        SOFA_resp_score.loc[((SOFA_resp_score.pf < 200) & SOFA_resp_score.pf_score.isnull()), 'pf_score'] = 3
        SOFA_resp_score.loc[((SOFA_resp_score.pf < 300) & SOFA_resp_score.pf_score.isnull()), 'pf_score'] = 2
        SOFA_resp_score.loc[((SOFA_resp_score.pf < 400) & SOFA_resp_score.pf_score.isnull()), 'pf_score'] = 1
        SOFA_resp_score.loc[((SOFA_resp_score.pf >= 400) & SOFA_resp_score.pf_score.isnull()), 'pf_score'] = 0

        SOFA_resp_score.loc[((SOFA_resp_score.spf < 100) & SOFA_resp_score.spf_score.isnull()), 'spf_score'] = 4
        SOFA_resp_score.loc[((SOFA_resp_score.spf < 200) & SOFA_resp_score.spf_score.isnull()), 'spf_score'] = 3
        SOFA_resp_score.loc[((SOFA_resp_score.spf < 300) & SOFA_resp_score.spf_score.isnull()), 'spf_score'] = 2
        SOFA_resp_score.loc[((SOFA_resp_score.spf < 400) & SOFA_resp_score.spf_score.isnull()), 'spf_score'] = 1
        SOFA_resp_score.loc[((SOFA_resp_score.spf >= 400) & SOFA_resp_score.spf_score.isnull()), 'spf_score'] = 0

        SOFA_resp_score.drop(columns=['spf', 'pf'], inplace=True)

        del sofa_mv, visit_spf, visit_pf, visit_spo2, visit_pao2, visit_fio2

    ### determine cardio score ###
    if ('SOFA' in selected_modes):
        # Calculate SOFA cario score
        logm('Calculating SOFA Cardio Score', **logging_kwargs)
        SOFA_cardio_score = _cardio_SOFA(tempTableSchema=tempTableSchema, lookup_schema=lookup_schema, drug_lookup_table=drug_lookup_table,
                                         data_schema=data_schema, engine=engine, temp_table_name=temp_table_name, lookup_table=lookup_table,
                                         append_subject_id_type_if_missing=append_subject_id_type_if_missing,
                                         visit_df=visit_df.copy(deep=True) if local_mode else None,
                                         **local_kwargs)

    SOFA_out: List[pd.DataFrame] = []
    eSOFA_out: List[pd.DataFrame] = []
    qSOFA_out: List[pd.DataFrame] = []

    with tqdm(total=visit_df.shape[0], desc=f'Calculating {", ".join(sofa_versions)} Scores') as pbar:
        for _, row in visit_df.iterrows():

            # summarize scores in 24 hr windows
            basis = pd.DataFrame(index=[row.period_start, row.period_end] if (row.period_start != row.period_end) else [row.period_start])\
                .resample(sofa_frequency)\
                .ffill()

            if 'eSOFA' in selected_modes:
                # append all eSOFA variables
                eSOFA = basis.copy(deep=True)\
                    .merge(pressor_scores.loc[pressor_scores.visit_occurrence_id == row.visit_occurrence_id,
                                              ['pressor_score', 'drug_exposure_start_datetime']].copy().set_index('drug_exposure_start_datetime'),
                           left_index=True,
                           right_index=True,
                           how='left')\
                    .rename(columns={'pressor_score': 'eSOFA_pressor_score'})\
                    .merge(eSOFA_renal_scores.loc[eSOFA_renal_scores.visit_occurrence_id == row.visit_occurrence_id,
                                                  ['measurement_datetime', 'eSOFA_renal_score']].copy().set_index('measurement_datetime'),
                           left_index=True,
                           right_index=True,
                           how='left')\
                    .merge(mv_scores.loc[mv_scores.visit_occurrence_id == row.visit_occurrence_id,
                                         ['device_exposure_start_datetime', 'mv_score']].copy().set_index('device_exposure_start_datetime'),
                           left_index=True,
                           right_index=True,
                           how='left')\
                    .rename(columns={'mv_score': 'eSOFA_mv_score'})\
                    .merge(perfusion_scores.loc[perfusion_scores.visit_occurrence_id == row.visit_occurrence_id,
                                                ['measurement_datetime', 'perfusion_score']].copy().set_index('measurement_datetime'),
                           left_index=True,
                           right_index=True,
                           how='left')\
                    .merge(coag_scores.loc[coag_scores.visit_occurrence_id == row.visit_occurrence_id,
                                           ['measurement_datetime', 'eSOFA_coag_score']].copy().set_index('measurement_datetime'),
                           left_index=True,
                           right_index=True,
                           how='left')\
                    .merge(eSOFA_hepatic_scores.loc[eSOFA_hepatic_scores.visit_occurrence_id == row.visit_occurrence_id,
                                                    ['measurement_datetime', 'eSOFA_hepatic_score']].copy().set_index('measurement_datetime'),
                           left_index=True,
                           right_index=True,
                           how='left')\
                    .rename(columns={'perfusion_score': 'eSOFA_perfusion_score'})\
                    .groupby(level=0).max()

                # fillna 0 for pressor and mv score since they are dependent on when started, not the value itself
                eSOFA.loc[:, ['eSOFA_pressor_score', 'eSOFA_mv_score']] = eSOFA.loc[:, ['eSOFA_pressor_score', 'eSOFA_mv_score']].fillna(0)

                # Roll the lab derived value scores, then FFIll based on the limit. This strategy allows for the worst values within 24 hours if present to be used. Otherwise they are forward filled.
                eSOFA = eSOFA\
                    .rolling('24h').max().ffill(limit=ff_limit_hours)

                # calculate eSOFA score
                eSOFA['eSOFA_score'] = eSOFA.sum(axis=1, skipna=True)

                # move the time index to a column
                eSOFA = eSOFA.reset_index(drop=False).rename(columns={'index': 'SOFA_datetime'})

                # label the visit and person ids
                eSOFA['person_id'] = row.person_id
                eSOFA['visit_occurrence_id'] = row.visit_occurrence_id

                if 'visit_detail_id' in row.index:
                    eSOFA['visit_detail_id'] = row.visit_detail_id

                # append to output
                eSOFA_out.append(eSOFA)

            if 'SOFA' in selected_modes:
                # append all eSOFA variables
                SOFA = basis.copy(deep=True)\
                    .merge(SOFA_cardio_score.loc[SOFA_cardio_score.visit_occurrence_id == row.visit_occurrence_id,
                                                 ['SOFA_map_score', 'SOFA_pressor_score', 'SOFA_cardio_score', 'observation_datetime']].copy()
                           .set_index('observation_datetime'),
                           left_index=True,
                           right_index=True,
                           how='left')\
                    .merge(SOFA_renal_scores.loc[SOFA_renal_scores.visit_occurrence_id == row.visit_occurrence_id,
                                                 ['measurement_datetime', 'sofa_renal']].copy().set_index('measurement_datetime'),
                           left_index=True,
                           right_index=True,
                           how='left')\
                    .rename(columns={'sofa_renal': 'SOFA_renal_score'})\
                    .merge(SOFA_resp_score.loc[SOFA_resp_score.visit_occurrence_id == row.visit_occurrence_id,
                                               ['measurement_datetime', 'mv_score', 'pf_score', 'spf_score']].copy().set_index('measurement_datetime'),
                           left_index=True,
                           right_index=True,
                           how='left')\
                    .rename(columns={'mv_score': 'SOFA_mv_score', 'pf_score': 'SOFA_pf_score', 'spf_score': 'SOFA_spf_score'})\
                    .merge(SOFA_cns_scores.loc[SOFA_cns_scores.visit_occurrence_id == row.visit_occurrence_id,
                                               ['measurement_datetime', 'SOFA_cns_score']].copy().set_index('measurement_datetime'),
                           left_index=True,
                           right_index=True,
                           how='left')\
                    .merge(SOFA_coag_scores.loc[SOFA_coag_scores.visit_occurrence_id == row.visit_occurrence_id,
                                                ['measurement_datetime', 'SOFA_coag_score']].copy().set_index('measurement_datetime'),
                           left_index=True,
                           right_index=True,
                           how='left')\
                    .merge(SOFA_hepatic_scores.loc[SOFA_hepatic_scores.visit_occurrence_id == row.visit_occurrence_id,
                                                   ['measurement_datetime', 'SOFA_hepatic']].copy().set_index('measurement_datetime'),
                           left_index=True,
                           right_index=True,
                           how='left')\
                    .rename(columns={'SOFA_hepatic': 'SOFA_hepatic_score'})\
                    .groupby(level=0).max()

                # fillna 0 for pressor and mv score since they are dependent on when they were taken and should not be filled forward
                SOFA.loc[:, ['SOFA_pressor_score', 'SOFA_mv_score']] = SOFA.loc[:, ['SOFA_pressor_score', 'SOFA_mv_score']].fillna(0)

                # Roll the lab derived value scores, then FFIll based on the limit. This strategy allows for the worst values within 24 hours if present to be used. Otherwise they are forward filled.
                SOFA = SOFA\
                    .rolling('24h').max().ffill(limit=ff_limit_hours)

                # Compute the final resp score, The maximum resp score is limited to 2, when mechanical ventilation is not being employed
                SOFA['SOFA_resp_score'] = SOFA[['SOFA_mv_score', 'SOFA_pf_score', 'SOFA_spf_score']].apply(lambda row: row.max() if row.SOFA_mv_score == 1 else min(row.max(), 2), axis=1)

                # calculate eSOFA score
                SOFA['SOFA_score'] = SOFA[['SOFA_cardio_score', 'SOFA_renal_score', 'SOFA_cns_score',
                                           'SOFA_coag_score', 'SOFA_hepatic_score', 'SOFA_resp_score']].sum(axis=1, skipna=True)

                # move the time index to a column
                SOFA = SOFA.reset_index(drop=False).rename(columns={'index': 'SOFA_datetime'})

                # label the visit and person ids
                SOFA['person_id'] = row.person_id
                SOFA['visit_occurrence_id'] = row.visit_occurrence_id

                if 'visit_detail_id' in row.index:
                    SOFA['visit_detail_id'] = row.visit_detail_id

                # append to output
                SOFA_out.append(SOFA)

            if 'qSOFA' in selected_modes:
                # append all eSOFA variables
                qSOFA = basis.copy(deep=True)\
                    .merge(qSOFA_resp_scores.loc[qSOFA_resp_scores.visit_occurrence_id == row.visit_occurrence_id,
                                                 ['qSOFA_resp_score', 'measurement_datetime']].copy()
                           .set_index('measurement_datetime'),
                           left_index=True,
                           right_index=True,
                           how='left')\
                    .merge(qSOFA_cns_scores.loc[qSOFA_cns_scores.visit_occurrence_id == row.visit_occurrence_id,
                                                ['measurement_datetime', 'qSOFA_cns_score']].copy().set_index('measurement_datetime'),
                           left_index=True,
                           right_index=True,
                           how='left')\
                    .merge(qSOFA_bp_scores.loc[qSOFA_bp_scores.visit_occurrence_id == row.visit_occurrence_id,
                                               ['measurement_datetime', 'qSOFA_bp_score']].copy().set_index('measurement_datetime'),
                           left_index=True,
                           right_index=True,
                           how='left')\
                    .groupby(level=0).max()

                # Roll the  value scores, then FFIll based on the limit. This strategy allows for the worst values within 24 hours if present to be used. Otherwise they are forward filled.
                qSOFA = qSOFA\
                    .rolling('24h').max().ffill(limit=ff_limit_hours)

                # calculate eSOFA score
                qSOFA['qSOFA_score'] = qSOFA.sum(axis=1, skipna=True)

                # move the time index to a column
                qSOFA = qSOFA.reset_index(drop=False).rename(columns={'index': 'SOFA_datetime'})

                # label the visit and person ids
                qSOFA['person_id'] = row.person_id
                qSOFA['visit_occurrence_id'] = row.visit_occurrence_id

                if 'visit_detail_id' in row.index:
                    qSOFA['visit_detail_id'] = row.visit_detail_id

                # append to output
                qSOFA_out.append(qSOFA)
            pbar.update(1)

    # Always drop the temp table
    if not local_mode:
        execute_query_in_transaction(query=f'DROP TABLE {tempTableSchema}.[{temp_table_name}];', engine=engine)

    out: pd.DataFrame = None

    if 'eSOFA' in selected_modes:
        out: pd.DataFrame = pd.concat(eSOFA_out, axis=0, sort=False)

    if 'SOFA' in selected_modes:
        if isinstance(out, pd.DataFrame):
            out: pd.DataFrame = out.merge(pd.concat(SOFA_out, axis=0, sort=False),
                                          on=out.columns.intersection(SOFA_out[0].columns).tolist(),
                                          how='left')
        else:
            out: pd.DataFrame = pd.concat(SOFA_out, axis=0, sort=False)
    if 'qSOFA' in selected_modes:
        if isinstance(out, pd.DataFrame):
            out: pd.DataFrame = out.merge(pd.concat(qSOFA_out, axis=0, sort=False),
                                          on=out.columns.intersection(qSOFA_out[0].columns).tolist(),
                                          how='left')
        else:
            out: pd.DataFrame = pd.concat(qSOFA_out, axis=0, sort=False)

    col_order: List[str] = ['person_id', 'visit_occurrence_id', 'visit_detail_id', 'SOFA_datetime',
                            'SOFA_score', 'eSOFA_score', 'qSOFA_score', 'eSOFA_pressor_score', 'SOFA_map_score',
                            'SOFA_pressor_score', 'SOFA_cardio_score', 'qSOFA_bp_score', 'eSOFA_renal_score', 'SOFA_renal_score',
                            'eSOFA_mv_score', 'SOFA_mv_score', 'SOFA_resp_score', 'SOFA_pf_score', 'SOFA_spf_score', 'qSOFA_resp_score',
                            'eSOFA_perfusion_score', 'SOFA_cns_score', 'qSOFA_cns_score', 'eSOFA_coag_score',
                            'SOFA_coag_score', 'eSOFA_hepatic_score', 'SOFA_hepatic_score']

    out = out[[x for x in col_order if x in out.columns]]

    if keep_zeros:
        out.fillna(0, inplace=True)
    else:
        subscore_cols: List[str] = ['eSOFA_pressor_score', 'eSOFA_renal_score', 'eSOFA_mv_score', 'eSOFA_perfusion_score', 'eSOFA_coag_score',
                                    'eSOFA_hepatic_score', 'SOFA_map_score', 'SOFA_pressor_score', 'SOFA_cardio_score', 'SOFA_renal_score',
                                    'SOFA_mv_score', 'SOFA_pf_score', 'SOFA_spf_score', 'SOFA_cns_score', 'SOFA_coag_score', 'SOFA_hepatic_score',
                                    'SOFA_resp_score', 'qSOFA_resp_score', 'qSOFA_cns_score', 'qSOFA_bp_score']
        out.loc[:, out.columns.intersection(subscore_cols, sort=False)] = out.loc[:, out.columns.intersection(subscore_cols, sort=False)].replace({0: None})

    if isinstance(save_folder_path, str):
        out.to_csv(out_path, index=False)

    return out


def _get_baseline_labs(engine: Engine, data_schema: str, tempTableSchema: str, temp_table_name: str,
                       lookup_schema: str, lookup_table: str, append_subject_id_type_if_missing: Union[str, None],
                       visit_df: pd.DataFrame = None, lab_type: str = 'creatinine', **local_kwargs) -> pd.DataFrame:
    """
    Calculate the baseline creatinine for eSOFA score calculation.

    Parameters
    ----------
    engine : sqlalchemy.engine.base or equivalent database connection object.
        This is the connection used to create temporary tables and read from the database.
    data_schema : str
        The Schema which contains your CDM tables (e.g. Person, Visit_Occurrence, etc.). This may included the database using dot syntax e.g. database.schema or schema.
    tempTableSchema : str
        The Schema which you wish to use for the management of temporary tables, this may be the same as your CDM. This may included the database using dot syntax e.g. database.schema or schema.
    lab_type: str
        The lab type to be retrieved. Currently supports the following options [Bilirubin, Creatinine, Platelet]

    Returns
    -------
    pd.DataFrame
        dataframe containing baseline creatinine values for each encounter of interest.

    """
    if isinstance(visit_df, pd.DataFrame):
        hist_labs = load_variables_from_var_spec(variables_columns=['measurement_datetime', lab_type.lower()],
                                                 **local_kwargs,
                                                 append_subject_id_type_if_missing=append_subject_id_type_if_missing,
                                                 cdm_tables=['measurement'],
                                                 mute_duplicate_var_warnings=True,
                                                 desired_types={**{x: 'sparse_int' for x in ['person_id', 'visit_occurrence_id', 'visit_detail_id', 'subject_id',]},
                                                                **{'measurement_datetime': 'datetime', 'value_as_number': 'float', 'unit_concept_id': 'sparse_int'}},
                                                 allow_empty_files=True,
                                                 regex=True, dtype=None,
                                                 ds_type='pandas')\
            .merge(visit_df[['subject_id', 'visit_start_datetime', 'period_start']],
                   on='subject_id',
                   how='inner')\
            .query('measurement_datetime <= period_start')\
            .drop(columns=['subject_id', 'variable_name', 'visit_start_datetime'], errors='ignore')\
            .dropna(subset=['value_as_number'])

        hist_labs.unit_concept_id = hist_labs.unit_concept_id.fillna('-999').astype(int)

        if lab_type.lower() == 'creatinine':

            hist_labs = hist_labs.merge(load_variables_from_var_spec(variables_columns=['gender_concept_id', 'race_concept_id', 'birth_date'],
                                                                     **local_kwargs,
                                                                     cdm_tables=['person'],
                                                                     mute_duplicate_var_warnings=True,
                                                                     desired_types={**{x: 'sparse_int' for x in ['person_id', 'visit_occurrence_id', 'visit_detail_id', 'subject_id', 'gender_concept_id', 'race_concept_id']},
                                                                                    **{'birth_date': 'datetime', }},
                                                                     allow_empty_files=True,
                                                                     regex=True, dtype=None,
                                                                     ds_type='pandas').drop(columns=['subject_id'], errors='ignore').drop_duplicates(),
                                        how='inner',
                                        on='person_id')\
                .drop(columns=['person_id'])

            hist_labs['age'] = (hist_labs['period_start'] - hist_labs['birth_date']).astype('timedelta64[Y]').astype(int)
            hist_labs['female'] = hist_labs.gender_concept_id.apply(lambda x: 1 if x == '8532' else 0)

            hist_labs.drop(columns=['birth_date', 'period_start'], inplace=True)

            agg_dict: dict = {'baseline_creatinine': 'median',
                              'age': 'first',
                              'female': 'first'}

        elif lab_type.lower() in ['bilirubin_total', 'platelets']:

            agg_dict: dict = {f'baseline_{lab_type.lower()}': 'median'}

        else:
            raise Exception(f'Unsuported lab_type specified: {lab_type.lower()}. Currently supported types include bilirubin, creatinine, lactate, platelet')

        hist_labs.rename(columns={'value_as_number': f'baseline_{lab_type.lower()}'}, inplace=True)

    else:

        if lab_type.lower() == 'creatinine':
            custom_query: str = 'b.female, b.black, b.age,'
            agg_dict: dict = {'baseline_creatinine': 'median',
                              'age': 'first',
                              'female': 'first',
                              'black': 'first'}
            filters: str = ';'
        elif lab_type.lower() in ['bilirubin_total', 'platelets']:
            custom_query: str = ''
            agg_dict: dict = {f'baseline_{lab_type.lower()}': 'median'}
            filters: str = 'WHERE m.value_as_number IS NOT NULL;'
        else:
            raise Exception(f'Unsuported lab_type specified: {lab_type.lower()}. Currently supported types include bilirubin, creatinine, lactate, platelet')
        # attempt to get baseline creatine based on median value from past year
        hist_labs = pd.read_sql(f'''SELECT
                                           b.visit_occurrence_id,
                                           {custom_query}
                                           m.measurement_datetime,
                                           m.value_as_number [baseline_{lab_type.lower()}],
                                           COALESCE(unit_concept_id, -999) unit_concept_id
                                       FROM
                                           {tempTableSchema}.{temp_table_name} b
                                           LEFT JOIN {data_schema}.measurement m on (b.person_id = m.person_id
                                                                                   AND
                                                                                   m.measurement_datetime BETWEEN DATEADD(year, -1, b.period_start) and b.period_start
                                                                                   AND
                                                                                   m.measurement_concept_id IN (SELECT
                                                                                                                    concept_id
                                                                                                                FROM
                                                                                                                    {lookup_schema}.IC3_Variable_Lookup_Table_v2_beta
                                                                                                                WHERE
                                                                                                                    variable_name = '{lab_type}'))
                                           {filters}''',
                                con=engine,
                                parse_dates=['measurement_datetime'])

    if hist_labs.shape[0] > 0:

        if lab_type.lower() in ['bilirubin_total', 'creatinine', 'lactate']:
            hist_labs.unit_concept_id.replace({-999: 8840}, inplace=True)

            # fix creatinine unit error
            if lab_type.lower() == 'creatinine':
                hist_labs.unit_concept_id.replace({8861: 8840}, inplace=True)  # likely entry error of mg/ml instead of mg/dl

            # standardize lactate unit
            mmolL: pd.Series = hist_labs.unit_concept_id == 8753
            if any(mmolL):
                hist_labs.loc[mmolL, f'baseline_{lab_type.lower()}'] = hist_labs.loc[mmolL, f'baseline_{lab_type.lower()}'] / 0.111  # conversion factor for mmole/L to mg/dl lactate
                hist_labs.loc[mmolL, 'unit_concept_id'] = 8840
        elif lab_type.lower() == 'platelets':
            hist_labs.unit_concept_id.replace({-999: 8848,  # set default to thousand/uL if missing
                                               8938: 8848,  # likely data entry error of thousandth per uL
                                               9433: 8848,  # likely data entry error of 10x
                                               8961: 8848  # replace thousand/mm3 with equivalent thousand/uL
                                               }, inplace=True)

    hist_labs = hist_labs\
        .groupby(['visit_occurrence_id', 'unit_concept_id'])\
        .agg(agg_dict)\
        .reset_index(drop=False)

    if lab_type.lower() == 'creatinine':

        # if none avialable calculate using age, sex, and race
        missing_creat_mask = hist_labs.baseline_creatinine.isnull()

        if missing_creat_mask.any():
            hist_labs.loc[missing_creat_mask, 'baseline_creatinine'] = (0.74 -
                                                                        (0.2 * hist_labs.loc[missing_creat_mask, 'female']) +
                                                                        (0.003 * hist_labs.loc[missing_creat_mask, 'age']))
        del missing_creat_mask
    else:
        check = hist_labs.visit_occurrence_id.value_counts()

        if (check > 1).any():
            raise Exception(f'Error with baseline {lab_type} function, please ammend the script to consolidate/convert units: {hist_labs.unit_concept_id.unique()}')

    return hist_labs[['visit_occurrence_id', f'baseline_{lab_type.lower()}']].copy()


def _get_visit_labs(engine: Engine, data_schema: str, tempTableSchema: str, temp_table_name: str, lookup_schema: str,
                    lookup_table: str, append_subject_id_type_if_missing: Union[str, None], lab_type: str = 'creatinine', visit_df: pd.DataFrame = None, **local_kwargs) -> pd.DataFrame:
    """
    Get the specified lab values for the period of interest.

    Parameters
    ----------
    engine : sqlalchemy.engine.base or equivalent database connection object.
        This is the connection used to create temporary tables and read from the database.
    data_schema : str
        The Schema which contains your CDM tables (e.g. Person, Visit_Occurrence, etc.). This may included the database using dot syntax e.g. database.schema or schema.
    tempTableSchema : str
        The Schema which you wish to use for the management of temporary tables, this may be the same as your CDM. This may included the database using dot syntax e.g. database.schema or schema.
    temp_table_name: str,
        The name of the temporary SOFA Table
    lab_type: str
        The lab type to be retrieved. Currently supports the following options [Bilirubin, Creatinine, Lactate, Platelet]

    Returns
    -------
    pd.DataFrame
        dataframe containing visit lab values for each encounter of interest.

    """
    if isinstance(visit_df, pd.DataFrame):
        visit_labs = load_variables_from_var_spec(variables_columns=['measurement_datetime', lab_type.lower()],
                                                  **local_kwargs,
                                                  cdm_tables=['measurement'],
                                                  mute_duplicate_var_warnings=True,
                                                  append_subject_id_type_if_missing=append_subject_id_type_if_missing,
                                                  desired_types={**{x: 'sparse_int' for x in ['person_id', 'visit_occurrence_id', 'visit_detail_id', 'subject_id',]},
                                                                 **{'measurement_datetime': 'datetime', 'value_as_number': 'float', 'unit_concept_id': 'sparse_int'}},
                                                  allow_empty_files=True,
                                                  regex=True, dtype=None,
                                                  ds_type='pandas')\
            .merge(visit_df[['subject_id', 'visit_start_datetime', 'period_end']],
                   on='subject_id',
                   how='inner')\
            .query('(measurement_datetime >= visit_start_datetime) & (measurement_datetime <= period_end)')\
            .drop(columns=['subject_id', 'person_id', 'variable_name', 'visit_start_datetime', 'period_end'], errors='ignore')

        visit_labs.unit_concept_id = visit_labs.unit_concept_id.fillna('-999').astype(int)

        if lab_type.lower() == 'lactate':
            visit_labs.query('value_as_number >= 2', inplace=True)

        visit_labs.rename(columns={'value_as_number': lab_type.lower()}, inplace=True)

    else:
        # attempt to get baseline creatine based on median value from past year
        if lab_type.lower() == 'lactate':
            filters: str = 'AND m.value_as_number >= 2;'
        else:
            filters: str = ';'

        visit_labs = pd.read_sql(f'''SELECT
                                           b.visit_occurrence_id,
                                           m.measurement_datetime,
                                           m.value_as_number [{lab_type}],
                                           COALESCE(unit_concept_id, -999) unit_concept_id
                                       FROM
                                           {tempTableSchema}.{temp_table_name} b
                                           LEFT JOIN {data_schema}.measurement m on (b.visit_occurrence_id = m.visit_occurrence_id
                                                                                   AND
                                                                                   m.measurement_concept_id IN (SELECT
                                                                                                                    concept_id
                                                                                                                FROM
                                                                                                                    {lookup_schema}.{lookup_table}
                                                                                                                WHERE
                                                                                                                    variable_name = '{lab_type}'))
                                       WHERE
                                           m.value_as_number IS NOT NULL {filters}''',
                                 con=engine,
                                 parse_dates=['measurement_datetime'])

    if visit_labs.shape[0] > 0:
        # floor the measurement times to resample to the hour
        visit_labs.measurement_datetime = visit_labs.measurement_datetime.dt.floor('h')

        if lab_type.lower() in ['bilirubin_total', 'creatinine', 'lactate']:
            visit_labs.unit_concept_id.replace({-999: 8840}, inplace=True)

            # fix creatinine unit error
            if lab_type.lower() == 'creatinine':
                visit_labs.unit_concept_id.replace({8861: 8840}, inplace=True)  # likely entry error of mg/ml instead of mg/dl

            # standardize lactate unit
            mmolL: pd.Series = visit_labs.unit_concept_id == 8753
            if any(mmolL):
                visit_labs.loc[mmolL, f'{lab_type}'] = visit_labs.loc[mmolL, f'{lab_type}'] / 0.111  # conversion factor for mmole/L to mg/dl lactate
                visit_labs.loc[mmolL, 'unit_concept_id'] = 8840
        elif lab_type.lower() == 'platelets':
            visit_labs.unit_concept_id.replace({-999: 8848,  # set default to thousand/uL if missing
                                               8938: 8848,  # likely data entry error of thousandth per uL
                                               9433: 8848,  # likely data entry error of 10x
                                               8961: 8848  # replace thousand/mm3 with equivalent thousand/uL
                                                }, inplace=True)
        if lab_type.lower() == 'spo2':
            visit_labs.unit_concept_id.replace({-999: 8554,  # set default to %
                                               8728: 8554,  # convert % sat to %
                                                }, inplace=True)

    # verify units are consistent
    if visit_labs.unit_concept_id.nunique() > 1:
        raise Exception(f'Please ammend the _get_visit_lab function to standardize {lab_type} units to ensure accurate results. The following units were observed: {visit_labs.unit_concept_id.unique()}')

    if lab_type.lower() == 'lactate':
        if visit_labs.shape[0] > 0:
            visit_labs['perfusion_score'] = 1
        else:
            visit_labs['perfusion_score'] = None

    return visit_labs[visit_labs.columns.intersection(['visit_occurrence_id', 'measurement_datetime', lab_type, 'perfusion_score'])].copy()


def _pressors_for_visit(engine: Engine, data_schema: str, tempTableSchema: str, lookup_schema: str, temp_table_name: str,
                        lookup_table: str, append_subject_id_type_if_missing: Union[str, None], visit_df: pd.DataFrame = None, **local_kwargs) -> pd.DataFrame:
    """
    Get pressor scores for the visits.

    Parameters
    ----------
    engine : sqlalchemy.engine.base or equivalent database connection object.
        This is the connection used to create temporary tables and read from the database.
    data_schema : str
        The Schema which contains your CDM tables (e.g. Person, Visit_Occurrence, etc.). This may included the database using dot syntax e.g. database.schema or schema.
    tempTableSchema : str
        The Schema which you wish to use for the management of temporary tables, this may be the same as your CDM. This may included the database using dot syntax e.g. database.schema or schema.

    Returns
    -------
    meds : pd.DataFrame
        Pandas DataFrame containing the following columns:
            *'visit_occurrence_id
            *drug_exposure_start_datetime
            *pressor_score

    """
    if isinstance(visit_df, pd.DataFrame):
        meds = load_variables_from_var_spec(variables_columns=['drug_exposure_start_datetime', 'pressors_inotropes', 'drug_exposure_end_datetime'],
                                            **local_kwargs,
                                            append_subject_id_type_if_missing=append_subject_id_type_if_missing,
                                            cdm_tables=['drug_exposure'],
                                            mute_duplicate_var_warnings=True,
                                            desired_types={**{x: 'sparse_int' for x in ['person_id', 'visit_occurrence_id', 'visit_detail_id', 'subject_id',]},
                                                           **{'drug_exposure_start_datetime': 'datetime', 'drug_exposure_end_datetime': 'datetime', 'unit_concept_id': 'sparse_int'}},
                                            allow_empty_files=True,
                                            regex=True, dtype=None,
                                            ds_type='pandas')\
            .drop(columns=['subject_id', 'person_id', 'variable_name'], errors='ignore')\
            .groupby(['visit_occurrence_id'])

    else:
        meds = pd.read_sql(f'''SELECT
                                    d.visit_occurrence_id,
                                    drug_exposure_start_datetime,
                                    drug_exposure_end_datetime
                                FROM
                                    {tempTableSchema}.{temp_table_name} b
                                    INNER JOIN {data_schema}.drug_exposure d ON (b.visit_occurrence_id = d.visit_occurrence_id
                                                                                 AND
                                                                                 d.route_concept_id IN (4112421, 4171047, 3565812, 4170113, 4171884, 3562075, 3562058, 3449595, 3431013, 3442124)
                                                                                 AND
                                                                                 d.drug_concept_id IN (SELECT DISTINCT concept_id FROM {lookup_schema}.{lookup_table} WHERE variable_name = 'pressors_inotropes'));''',
                           con=engine,
                           parse_dates=['drug_exposure_start_datetime', 'drug_exposure_end_datetime'])\
            .groupby(['visit_occurrence_id'])

    if meds.ngroups == 0:
        return pd.DataFrame(columns=['visit_occurrence_id', 'drug_exposure_start_datetime', 'pressor_score'])

    meds = meds\
        .apply(_condense_overlapping_segments,
               start_col='drug_exposure_start_datetime',
               end_col='drug_exposure_end_datetime',
               grouping=['visit_occurrence_id'],
               single_group=False if meds.ngroups > 1 else True)\
        .drop(columns=['drug_exposure_end_datetime'])

    if 'visit_occurrence_id' not in meds.columns:
        meds.reset_index(inplace=True, drop=False)

    # add pressor score
    if meds.shape[0] > 0:
        meds['pressor_score'] = 1

        # floor the measurement times to resample to the hour
        meds.drug_exposure_start_datetime = meds.drug_exposure_start_datetime.dt.floor('h')
    else:
        meds['pressor_score'] = None

    return meds[['visit_occurrence_id', 'drug_exposure_start_datetime', 'pressor_score']].copy()


def _mv_for_visit(engine: Engine, data_schema: str, tempTableSchema: str, temp_table_name: str, lookup_schema: str,
                  mode: str, lookup_table: str, append_subject_id_type_if_missing: Union[str, None],
                  visit_df: pd.DataFrame = None, **local_kwargs) -> pd.DataFrame:
    """
    Retreve MV episodes from device exposure table and calculate mv score.

    Parameters
    ----------
    engine : sqlalchemy.engine.base or equivalent database connection object.
        This is the connection used to create temporary tables and read from the database.
    data_schema : str
        The Schema which contains your CDM tables (e.g. Person, Visit_Occurrence, etc.). This may included the database using dot syntax e.g. database.schema or schema.
    tempTableSchema : str
        The Schema which you wish to use for the management of temporary tables, this may be the same as your CDM. This may included the database using dot syntax e.g. database.schema or schema.

    Returns
    -------
    pd.DataFrame
        Pandas dataframe containing the following columns
            *visit_occurrence_id
            *device_exposure_start_datetime
            *mv_score

    """
    print("Starting _mv_for_visit function")
    if isinstance(visit_df, pd.DataFrame):
        mv = load_variables_from_var_spec(variables_columns=['device_exposure_start_datetime', 'mechanical_ventilation', 'device_exposure_end_datetime', 'visit_detail_id'],
                                          **local_kwargs,
                                          cdm_tables=['device_exposure'],
                                          append_subject_id_type_if_missing=append_subject_id_type_if_missing,
                                          mute_duplicate_var_warnings=True,
                                          desired_types={**{x: 'sparse_int' for x in ['person_id', 'visit_occurrence_id', 'visit_detail_id', 'subject_id',]},
                                                         **{'device_exposure_start_datetime': 'datetime', 'device_exposure_end_datetime': 'datetime', 'unit_concept_id': 'sparse_int'}},
                                          allow_empty_files=True,
                                          regex=True, dtype=None,
                                          ds_type='pandas')\
            .drop(columns=['subject_id', 'person_id', 'variable_name'], errors='ignore')\
            .merge(load_variables_from_var_spec(variables_columns=['surgery', 'operating_room', 'procedure_suite', 'visit_detail_id'],
                                                **local_kwargs,
                                                cdm_tables=['visit_detail'],
                                                mute_duplicate_var_warnings=True,
                                                desired_types={**{x: 'sparse_int' for x in ['person_id', 'visit_occurrence_id', 'visit_detail_id', 'subject_id',]},
                                                               **{'device_exposure_start_datetime': 'datetime', 'device_exposure_end_datetime': 'datetime', 'unit_concept_id': 'sparse_int'}},
                                                allow_empty_files=True,
                                                regex=True, dtype=None,
                                                ds_type='pandas')
                   .drop(columns=['subject_id', 'person_id', 'visit_occurrence_id'], errors='ignore'),
                   how='left',
                   on='visit_detail_id')

        # filter out the procedure/surgery related mv for esofa
        if mode == 'eSOFA':
            mv = mv[mv.variable_name.isnull()]

        mv.drop(columns=['visit_detail_id', 'variable_name'], inplace=True, errors='ignore')

        mv = mv.groupby('visit_occurrence_id', group_keys=False)

    else:
        eSOFA_proc_filter: str = '''WHERE
                                        vd.visit_detail_concept_id NOT IN (2000000027, --Surgery
                                                                           4021813, -- Operating Room
                                                                           4331156 -- Procedure Suite
                                                                           )
                                        OR
                                        vd.visit_detail_concept_id IS NULL'''
        mv = pd.read_sql(f'''SELECT
                                   b.visit_occurrence_id,
                                   device_exposure_start_datetime,
                                   device_exposure_end_datetime
                               FROM
                                   {tempTableSchema}.{temp_table_name} b
                                   INNER JOIN {data_schema}.device_exposure d ON (b.visit_occurrence_id = d.visit_occurrence_id
                                                                              AND
                                                                              d.device_concept_id IN (SELECT DISTINCT concept_id FROM {lookup_schema}.IC3_Variable_Lookup_Table_v2_beta WHERE variable_name = 'mechanical_ventilation'))
                                   LEFT JOIN {data_schema}.VISIT_DETAIL vd on (vd.visit_occurrence_id = b.visit_occurrence_id
                                                                               AND
                                                                               d.device_exposure_start_datetime BETWEEN vd.visit_detail_start_datetime AND vd.visit_detail_end_datetime)
                               {eSOFA_proc_filter if mode == 'eSOFA' else ''};''',
                         con=engine,
                         parse_dates=['device_exposure_start_datetime', 'device_exposure_end_datetime'])\
            .groupby('visit_occurrence_id', group_keys=False)

    if mv.ngroups == 0:
        return pd.DataFrame(columns=['visit_occurrence_id', 'device_exposure_start_datetime', 'mv_score'])
    
    print(f"Before condensing, mv head: {mv.head()}")
    mv = mv.apply(_condense_overlapping_segments,
                  start_col='device_exposure_start_datetime',
                  end_col='device_exposure_end_datetime',
                  gap_tolerance='1 day' if mode == 'eSOFA' else '1 hour',
                  grouping=['visit_occurrence_id'])
    logging.debug(f"After condensing, mv head: {mv.head()}")
    print(f"After condensing, mv head: {mv.head()}")

    if 'visit_occurrence_id' not in mv.columns:
        mv.reset_index(inplace=True)
        assert 'visit_occurrence_id' in mv.columns, 'The condensed Mechancial Ventilation DataFrame does not contain visit_occurrence_id, Please debug the _condense_overlapping_segments_function'
    print(f"Final mv head: {mv.head()}")

    mv.drop(columns=['level_1'], inplace=True, errors='ignore')

    # add pressor score
    if mv.shape[0] > 0:

        # floor the measurement times to resample to the hour
        mv.device_exposure_start_datetime = mv.device_exposure_start_datetime.dt.floor('h')
        mv.device_exposure_end_datetime = mv.device_exposure_end_datetime.dt.ceil('h') - pd.to_timedelta('1s')

        if mode == 'SOFA':
            if isinstance(visit_df, pd.DataFrame):
                stations = load_variables_from_var_spec(variables_columns=['surgery', 'operating_room', 'procedure_suite', 'ward', 'icu', 'visit_detail_start_datetime', 'visit_detail_end_datetime'],
                                                        **local_kwargs,
                                                        cdm_tables=['visit_detail'],
                                                        mute_duplicate_var_warnings=True,
                                                        desired_types={**{x: 'sparse_int' for x in ['person_id', 'visit_occurrence_id', 'visit_detail_id', 'subject_id',]},
                                                                       **{'visit_detail_start_datetime': 'datetime', 'visit_detail_end_datetime': 'datetime'}},
                                                        allow_empty_files=True,
                                                        regex=True, dtype=None,
                                                        ds_type='pandas')\
                    .drop_duplicates(subset=['visit_detail_id'])\
                    .drop(columns=['subject_id', 'person_id', 'visit_detail_id', 'visit_detail_concept_id'], errors='ignore')

                stations['priority'] = stations.variable_name.apply(lambda x: 1 if x in ['operating_room', 'surgery'] else 2 if x == 'procedure_suite' else 3 if x == 'icu' else 4)
            else:

                # pull station information
                stations = pd.read_sql(f'''SELECT
                                           b.visit_occurrence_id,
                                           visit_detail_start_datetime,
                                           visit_detail_end_datetime,
                                           variable_name,
                                           CASE WHEN variable_name = 'operating_room' THEN 1
                                                WHEN variable_name = 'procedure_suite'  THEN 2
                                                WHEN variable_name = 'icu' THEN 3
                                                ELSE 4 END priority
                                       FROM
                                           {tempTableSchema}.{temp_table_name} b
                                           INNER JOIN {data_schema}.VISIT_DETAIL vd ON b.visit_occurrence_id = vd.visit_occurrence_id
                                           INNER JOIN {lookup_schema}.{lookup_table} l on (l.concept_id = vd.visit_detail_concept_id
                                                                                                      AND
                                                                                                      l.variable_name IN ('procedure_suite', 'operating_room', 'ward', 'icu'))
                                       WHERE
                                           visit_detail_end_datetime IS NOT NULL''',
                                       con=engine,
                                       parse_dates=['visit_detail_start_datetime', 'visit_detail_end_datetime'])

            # floor start, ceil end and subtract 1 second to help avoid overlaps
            stations.visit_detail_start_datetime = stations.visit_detail_start_datetime.dt.floor('h')
            stations.visit_detail_end_datetime = stations.visit_detail_end_datetime.dt.ceil('h')

            stations = stations.groupby('visit_occurrence_id', group_keys=False).apply(_condense_overlapping_segments,
                                                                     start_col='visit_detail_start_datetime',
                                                                     end_col='visit_detail_end_datetime',
                                                                     gap_tolerance='0 h',
                                                                     custom_overlap_function=_resolve_overlaps,
                                                                     priority_col='priority',
                                                                     grouping=['visit_occurrence_id'])
            if 'visit_occurrence_id' not in stations.columns:
                stations.reset_index(inplace=True)
                stations.drop(columns=['level_1'], inplace=True, errors='ignore')
            else:
                stations.reset_index(drop=True, inplace=True)

            resampled_stations = pd.concat([pd.DataFrame(data={'variable_name': [row.variable_name, row.variable_name],
                                                               'visit_occurrence_id': [row.visit_occurrence_id, row.visit_occurrence_id]},
                                                         index=[row.visit_detail_start_datetime, row.visit_detail_end_datetime]).resample('1h').ffill() for _, row in stations.iterrows()], axis=0)\
                .dropna(subset=['visit_occurrence_id'])\
                .reset_index(drop=False)\
                .rename(columns={'index': 'observation_datetime'})

            # filter out mv rows that were during procedures
            mv = pd.concat([pd.DataFrame(data={'visit_occurrence_id': [row.visit_occurrence_id, row.visit_occurrence_id]},
                                         index=[row.device_exposure_start_datetime, row.device_exposure_end_datetime]).resample('1h').ffill() for _, row in mv.iterrows()], axis=0)\
                .dropna(subset=['visit_occurrence_id'])\
                .reset_index(drop=False)\
                .rename(columns={'index': 'observation_datetime'})\
                .merge(resampled_stations, on=['visit_occurrence_id', 'observation_datetime'], how='left')\
                .query('~variable_name.isin(["ward", "procedure_suite", "operating_room"])', engine='python')\
                .rename(columns={'observation_datetime': 'device_exposure_start_datetime'})

        # add score
        mv['mv_score'] = 1

    else:
        mv['mv_score'] = None

    return mv.drop(columns=['device_exposure_end_datetime', 'level_1', 'proc_ind', 'variable_name'], errors='ignore')


# def _condense_overlapping_segments(df: pd.DataFrame, start_col: str, end_col: str, grouping: list = [],
#                                     gap_tolerance: str = '0 min', single_group: bool = False,
#                                     custom_overlap_function: callable = None, **kwargs) -> pd.DataFrame:
#     """
#     Merge Overlapping time intervals in a pandas dataframe.

#     This function does the following actions:
#         1. Creates a 2D connectivity graph between start and end columns with a variable hour padding added to the end column date
#         2. Overlapping rows are merged into one row with the cell contents specified by the col_action_dict, if no col_action_dict is provided it will be the first row

#     Parameters
#     ----------
#     df:pd.DataFrame
#         pandas data frame that is groupedby grouping columns as index with atleast the folling columns
#         -start_col
#         -end_col

#     start_col: str
#         datetime which marks the start of the time interval

#     end_col: str
#         datetime which marks the end of hte time interval

#     Returns
#     -------
#     pd.DataFrame
#         returns a data frame with overlapping rows merged into one row

#     """
#     # get numpy array of admission dates
#     start = pd.to_datetime(df[start_col], errors='coerce').to_numpy()

#     # get numpy array of discharge dates with a 24 hour padding
#     end = (pd.to_datetime(df[end_col], errors='coerce') + pd.to_timedelta(gap_tolerance)).to_numpy()

#     # make graph
#     graph = (start <= end[:, None]) & (end >= start[:, None])

#     # find connected components in this graph
#     n_components, indices = connected_components(graph)

#     # return the df as is if there are no connected pieces
#     if n_components == df.shape[0]:

#         return df.reset_index(drop=True)

#     # run custom overlap resolution function
#     if custom_overlap_function is not None:
#         return df.groupby(indices).apply(lambda df: custom_overlap_function(df=df, start_col=start_col, end_col=end_col, **kwargs)).reset_index(drop=True)

#     dic = _create_dict(col_action_dict={'grouping': grouping}, start_col=start_col, end_col=end_col)

#     # group the results by these connected components
#     return df.groupby(indices).aggregate(dic).reset_index(drop=True)

import logging

# Setup logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def _condense_overlapping_segments(df: pd.DataFrame, start_col: str, end_col: str, grouping: list = [],
                                   gap_tolerance: str = '0 min', single_group: bool = False,
                                   custom_overlap_function: callable = None, **kwargs) -> pd.DataFrame:
    """
    Merge Overlapping time intervals in a pandas dataframe.

    This function does the following actions:
        1. Creates a 2D connectivity graph between start and end columns with a variable hour padding added to the end column date
        2. Overlapping rows are merged into one row with the cell contents specified by the col_action_dict, if no col_action_dict is provided it will be the first row

    Parameters
    ----------
    df: pd.DataFrame
        pandas data frame that is groupedby grouping columns as index with atleast the folling columns
        -start_col
        -end_col

    start_col: str
        datetime which marks the start of the time interval

    end_col: str
        datetime which marks the end of the time interval

    Returns
    -------
    pd.DataFrame
        returns a data frame with overlapping rows merged into one row
    """
    print(f"Initial df columns: {df.columns.tolist()}")
    print(f"Initial df head: {df.head()}")

    # Check if the required columns exist
    assert start_col in df.columns, f"{start_col} is not in DataFrame columns"
    assert end_col in df.columns, f"{end_col} is not in DataFrame columns"

    # get numpy array of admission dates
    start = pd.to_datetime(df[start_col], errors='coerce').to_numpy()

    # get numpy array of discharge dates with a 24 hour padding
    end = (pd.to_datetime(df[end_col], errors='coerce') + pd.to_timedelta(gap_tolerance)).to_numpy()

    # make graph
    graph = (start <= end[:, None]) & (end >= start[:, None])

    # find connected components in this graph
    n_components, indices = connected_components(graph)

    print(f"Number of components: {n_components}")
    print(f"Indices: {indices}")

    # return the df as is if there are no connected pieces
    if n_components == df.shape[0]:
        print("No overlapping components found.")
        return df.reset_index(drop=True)

    # run custom overlap resolution function
    if custom_overlap_function is not None:
        result_df = df.groupby(indices).apply(lambda df: custom_overlap_function(df=df, start_col=start_col, end_col=end_col, **kwargs)).reset_index(drop=True)
        print(f"Result df after custom overlap function: {result_df.head()}")
        return result_df

    #dic = _create_dict(col_action_dict={'grouping': grouping}, start_col=start_col, end_col=end_col)
    dic = _create_dict(col_action_dict={'grouping': grouping + ['visit_occurrence_id']}, start_col=start_col, end_col=end_col)


    # group the results by these connected components
    result_df = df.groupby(indices).aggregate(dic).reset_index(drop=True)
    print(f"Result df after aggregation: {result_df.head()}")

    return result_df






def _resolve_overlaps(df: pd.DataFrame, end_col: str, start_col: str,
                      priority_col: str = 'priority', return_initial_index: bool = False) -> pd.DataFrame:
    """
    Resolve information from rows with overlapping time intervals.

    Parameters
    ----------
    df : pd.DataFrame
        DESCRIPTION.
    end_col : str
        end column for the time interval described in each row.
    start_col : str
        start column for the time interval described in each row.
    priority_col : str, optional
        DESCRIPTION. The default is 'tmp_priority'.
    return_initial_index : bool, optional
        DESCRIPTION. The default is False.

    Returns
    -------
    pd.DataFrame
        DESCRIPTION.

    """
    if df.shape[0] == 1:
        return df.reset_index(drop=True)

    # make a template to insert the relevant information into
    temp: pd.DataFrame = pd.DataFrame(pd.Series({df[start_col].min(): 999, df[end_col].max(): 999}).resample('1 min').ffill()).drop(columns=[0])

    # make a deep copy of the df with the index reset to ensure it is unique
    df = df.copy().sort_values(start_col).reset_index(drop=True)

    # check if the priority_col exists, if not add one
    if priority_col not in df.columns:
        df[priority_col] = 1

    # get the relevant information from each row
    for index, row in df.iterrows():
        temp[index] = pd.Series({row[start_col]: row[priority_col],
                                 row[end_col]: row[priority_col]}).resample('1 min').ffill()

    temp = temp.idxmin(axis=1).reset_index().rename(columns={'index': 'time_series', 0: 'df_index'})

    temp['grouping_col'] = 'temp'

    temp['time_group'] = temp.groupby('grouping_col', group_keys=False)['df_index'].apply(lambda x: (x != x.shift()).astype(int).cumsum())

    temp.loc[:, end_col] = temp.loc[:, 'time_series']

    temp.rename(columns={'time_series': start_col}, inplace=True)

    temp_out = temp.groupby('time_group', group_keys=False).agg({start_col: 'min', end_col: 'max', 'df_index': 'first'}).reset_index(drop=True)

    out = temp_out.merge(df.reset_index().drop(columns=[start_col, end_col]), left_on='df_index', right_on='index', how='left')[df.columns.tolist() + ['df_index'] if return_initial_index else df.columns.tolist()]

    return out


def _compute_lab_ratio(baseline_df: pd.DataFrame, visit_df: pd.DataFrame,
                       merging_key: list, lab_type: str, min_thresholds_for_score: int,
                       minimum_absolute_thresh: float = None,
                       minimum_ratio_threshold: float = None,
                       maximum_absolute_thresh: float = None,
                       maximum_ratio_threshold: float = None,
                       default_baseline_value: float = None,
                       minimum_baseline_value: float = None,
                       score_label: str = None) -> pd.DataFrame:

    # label the baseline
    out = visit_df\
        .merge(baseline_df,
               on=merging_key,
               how='left')

    # fill default value (if present)
    if isinstance(default_baseline_value, (float, int)):
        out[f'baseline_{lab_type.lower()}'].fillna(default_baseline_value, inplace=True)

    # calculate lab ratio
    out['lab_ratio'] = (out[f'{lab_type.lower()}'] / out[f'baseline_{lab_type.lower()}']).values

    stat_dict: dict = {'minimum_ratio_threshold': minimum_ratio_threshold,
                       'minimum_absolute_thresh': minimum_absolute_thresh,
                       'maximum_ratio_threshold': maximum_ratio_threshold,
                       'maximum_absolute_thresh': maximum_absolute_thresh,
                       'minimum_baseline_value': minimum_baseline_value}

    for nm, thresh in stat_dict.items():

        if isinstance(thresh, (float, int)):
            if 'minimum' in nm:
                score_idx: pd.Series = out['lab_ratio' if 'ratio' in nm else
                                           f'baseline_{lab_type.lower()}' if 'baseline' in nm else
                                           f'{lab_type.lower()}'] >= thresh
            else:
                score_idx: pd.Series = out['lab_ratio' if 'ratio' in nm else
                                           f'baseline_{lab_type.lower()}' if 'baseline' in nm else
                                           f'{lab_type.lower()}'] <= thresh

            if score_idx.any():  # account for possiblity of None True
                out.loc[score_idx, nm] = 1

            if not score_idx.all():  # account for possiblity of all True
                out.loc[~score_idx, nm] = 0

    out[score_label] = out[out.columns.intersection(list(stat_dict.keys()))].apply(lambda row: 1 if row.sum() >= min_thresholds_for_score else 0, axis=1)

    return out.drop(columns=list(stat_dict.keys()), errors='ignore')


# def get_sample_visits(engine: Engine, data_schema: str) -> pd.DataFrame:
#     """Get a random Sample of the first 200 encounters that are atleast 3 days long. Note this is not truely random, and is meant to provide a quick sample for testing purposes."""
#     return pd.read_sql(sql=f'''SELECT DISTINCT TOP 2000
#                                    vo.VISIT_OCCURRENCE_ID visit_occurrence_id,
#                                    vo.VISIT_START_DATETIME,
#                                    vo.VISIT_END_DATETIME,
#                                    vo.VISIT_SOURCE_VALUE
#                                FROM
#                                    {data_schema}.visit_occurrence vo
#                                    INNER JOIN {data_schema}.visit_detail vd on vd.VISIT_OCCURRENCE_ID = vo.VISIT_OCCURRENCE_ID
#                                WHERE
#                                    DATEDIFF(day, vo.VISIT_START_DATETIME, vo.VISIT_END_DATETIME) >= 3
#                                    AND
#                                    vo.VISIT_CONCEPT_ID IN (8717, 9201, 262)
#                                    AND
#                                    vd.care_site_type_concept_id = 4148981;''', con=engine, parse_dates=['VISIT_START_DATETIME', 'VISIT_END_DATETIME'])


def execute_query_in_transaction(engine: Engine, query: str):
    """Excecute SQL query in transaction."""
    if engine.name == 'mysql':
        query = query.replace('[', '').replace(']', '')

    if str(type(engine)) == "<class 'sqlite3.Connection'>":
        c = engine.cursor()
        c.execute("begin")
        try:
            c.execute(query)
            c.execute('commit')
            print('executed successfully')
        except engine.Error:
            print("failed!")
            c.execute("rollback")
        finally:
            c.close()
    else:
        Session = sessionmaker(bind=engine)

        session = Session()
        try:
            session.execute(query)
            session.commit()
        except Exception as e:
            print(e)
            session.rollback()
        finally:
            session.close()


# def _create_dict(col_action_dict: dict, start_col: str = None, end_col: str = None) -> dict:

#     d: dict = {}
#     for key in col_action_dict:
#         if key != 'grouping':
#             d.update({x: key for x in col_action_dict[key]})

#     for col in col_action_dict['grouping']:
#         try:
#             del d[col]
#         except KeyError:
#             pass

#     else:
#         if isinstance(start_col, str):
#             d[start_col] = 'min'

#         if isinstance(end_col, str):
#             d[end_col] = 'max'

#     return d

def _create_dict(col_action_dict: dict, start_col: str = None, end_col: str = None) -> dict:
    d: dict = {}
    
    # Update the dictionary with actions for each column except 'grouping'
    for key in col_action_dict:
        if key != 'grouping':
            d.update({x: key for x in col_action_dict[key]})
    
    # Ensure 'grouping' columns are included with 'first' action
    for col in col_action_dict['grouping']:
        d[col] = 'first'
    
    # Ensure start_col and end_col have 'min' and 'max' actions respectively
    if isinstance(start_col, str):
        d[start_col] = 'min'

    if isinstance(end_col, str):
        d[end_col] = 'max'
    
    return d



def _determine_esrd_status(tempTableSchema: str, data_schema: str, engine: Engine, temp_table_name: str, lookup_schema: str, lookup_table: str, visit_df: pd.DataFrame = None, **local_kwargs) -> pd.DataFrame:
    if isinstance(visit_df, pd.DataFrame):
        out: pd.DataFrame = load_variables_from_var_spec(variables_columns=['condition_start_date', 'esrd'],
                                                         **local_kwargs,
                                                         cdm_tables=['condition_occurrence'],
                                                         mute_duplicate_var_warnings=True,
                                                         desired_types={**{x: 'sparse_int' for x in ['person_id', 'visit_occurrence_id', 'visit_detail_id', 'subject_id',]},
                                                                        **{'condition_start_date': 'datetime'}},
                                                         allow_empty_files=True,
                                                         regex=True, dtype=None,
                                                         ds_type='pandas')\
            .merge(visit_df[['subject_id', 'period_start']],
                   on='subject_id',
                   how='inner')\
            .query('condition_start_date <= period_start')\
            .drop(columns=['subject_id', 'person_id', 'variable_name', 'period_start', 'condition_start_date', 'condition_concept_id'], errors='ignore')\
            .drop_duplicates()

        out['esrd'] = '1' if out.shape[0] > 0 else None

        return out

    return pd.read_sql(f'''SELECT DISTINCT
                                       b.visit_occurrence_id,
                                       1 'esrd'
                                   FROM
                                       {tempTableSchema}.{temp_table_name} b
                                       INNER JOIN {data_schema}.condition_occurrence c on (b.person_id = c.person_id
                                                                               AND
                                                                               c.condition_start_date <= b.period_start
                                                                               )
                                       WHERE
                                           c.condition_concept_id IN (SELECT DISTINCT concept_id FROM {lookup_schema}.{lookup_table} WHERE variable_name = 'esrd')''',
                       con=engine)


def _cardio_SOFA(tempTableSchema: str, lookup_schema: str, data_schema: str, engine: Engine, temp_table_name: str,
                 drug_lookup_table: str, lookup_table: str, 
                 append_subject_id_type_if_missing: Union[str, None], visit_df: pd.DataFrame = None, **local_kwargs) -> pd.DataFrame:

    if isinstance(visit_df, pd.DataFrame):
        pressors = load_variables_from_var_spec(variables_columns=['drug_exposure_start_datetime', 'drug_exposure_end_datetime', 'dopamine', 'dobutamine', 'norepinephrine', 'epinephrine', 'numerator_unit_concept_id',
                                                                   'conversion_factor'],
                                                **local_kwargs,
                                                cdm_tables=['drug_exposure'],
                                                mute_duplicate_var_warnings=True,
                                                desired_types={**{x: 'sparse_int' for x in ['person_id', 'visit_occurrence_id', 'visit_detail_id', 'subject_id', 'numerator_unit_concept_id']},
                                                               **{'drug_exposure_start_datetime': 'datetime', 'drug_exposure_end_datetime': 'datetime', 'conversion_factor': 'float', 'quantity': 'float'}},
                                                allow_empty_files=True,
                                                regex=True, dtype=None,
                                                ds_type='pandas')\
            .rename(columns={'variable_name': 'Ingredient_name'})

        pressors['adjusted_dose'] = pressors.apply(lambda row: row.conversion_factor *
                                                   row.quantity *
                                                   (1000 if row.numerator_unit_concept_id == '8576' else 1) /
                                                   ((row.drug_exposure_end_datetime - (row.drug_exposure_start_datetime if row.drug_exposure_start_datetime != row.drug_exposure_end_datetime else (row.drug_exposure_start_datetime - pd.to_timedelta('1 minute')))).total_seconds() / 60),
                                                   axis=1) if pressors.shape[0] > 0 else None
        pressors['numerator_unit_concept_id'] = pressors.numerator_unit_concept_id.apply(lambda x: '8774' if x == '8576' else x) if pressors.shape[0] > 0 else None

        pressors.drop(columns=['subject_id', 'person_id', 'conversion_factor', 'quantity'], inplace=True)

        first_weight_of_visit: pd.DataFrame = load_variables_from_var_spec(variables_columns=['weight', 'unit_concept_id'],
                                                                           **local_kwargs,
                                                                           cdm_tables=['measurement'],
                                                                           mute_duplicate_var_warnings=True,
                                                                           desired_types={**{x: 'sparse_int' for x in ['person_id', 'visit_occurrence_id', 'visit_detail_id', 'subject_id', 'unit_concept_id']},
                                                                                          **{'value_as_number': 'float'}},
                                                                           allow_empty_files=True,
                                                                           regex=True, dtype=None,
                                                                           ds_type='pandas')
        first_weight_of_visit['weight_kgs'] = first_weight_of_visit.apply(lambda row: (row.value_as_number / 2.205) if row.unit_concept_id == '8739' else row.value_as_number, axis=1)

        assert all([x in ['9529', '8739'] for x in first_weight_of_visit.unit_concept_id.unique()])

        first_weight_of_visit.drop(columns=['person_id', 'subject_id', 'unit_concept_id', 'value_as_number'], inplace=True, errors='ignore')

    else:
        pressors: pd.DataFrame = pd.read_sql(f'''SELECT
                                                     D.visit_occurrence_id,
                                                     Ingredient_name,
                                                     --DL.Drug_name,
                                                     drug_exposure_start_datetime,
                                                     drug_exposure_end_datetime,
                                                     CASE WHEN DL.numerator_unit_concept_id = 8576 THEN (D.quantity * DL.conversion_factor) * 1000
                                                         ELSE (D.quantity * DL.conversion_factor) END / DATEDIFF(minute, drug_exposure_start_datetime,
                                                                                                                 CASE WHEN drug_exposure_start_datetime = drug_exposure_end_datetime THEN DATEADD(minute, 1, drug_exposure_start_datetime)
                                                                                                                 ELSE drug_exposure_end_datetime END)  [adjusted_dose],
                                                     --D.dose_unit_source_value,
                                                     CASE WHEN DL.numerator_unit_concept_id = 8576 THEN 8774 ELSE DL.numerator_unit_concept_id END [numerator_unit_concept_id]
                                                     --DL.numerator_unit
                                                     --DL.denominator_unit
                                                 FROM
                                                     {tempTableSchema}.{temp_table_name} t
                                                     INNER JOIN {data_schema}.DRUG_EXPOSURE D on t.visit_occurrence_id = D.visit_occurrence_id
                                                     INNER JOIN {lookup_schema}.{drug_lookup_table} DL on DL.Drug_concept_id = D.drug_concept_id
                                                     INNER JOIN {lookup_schema}.{lookup_table} L on (L.concept_id = D.route_concept_id AND L.variable_name IN ('infusion','intramuscular'))
                                                 WHERE
                                                     DL.Ingredient_name IN ('dopamine', 'dobutamine', 'norepinephrine', 'epinephrine')
                                                     AND
                                                     D.quantity <> 0''',
                                             con=engine).drop_duplicates()

    assert all([x in [8774, "8774"] for x in pressors.numerator_unit_concept_id.unique().tolist()]) or (pressors.shape[0] == 0), f'There are one or more unsopported units in the query result: {[x for x in pressors.numerator_unit_concept_id.unique().tolist() if x not in [8774, "8774"]]}. Please ensure they are all in mcg/min'

    pressors['SOFA_pressor_score'] = None

    if pressors.shape[0] > 0:
        if not isinstance(visit_df, pd.DataFrame):
            first_weight_of_visit: pd.DataFrame = pd.read_sql(f'''with partitioned as(
                                                                    SELECT
                                                                        m.visit_occurrence_id,
                                                                        CASE WHEN m.unit_concept_id = 8739 THEN m.value_as_number / 2.205 -- convert pounds to kg
                                                                            ELSE m.value_as_number END [weight_kgs],
                                                                        ROW_NUMBER() OVER(PARTITION BY m.visit_occurrence_id ORDER BY  m.measurement_datetime ASC) AS seq
                                                                    FROM
                                                                        {tempTableSchema}.{temp_table_name} t
                                                                        INNER JOIN {data_schema}.MEASUREMENT m on m.visit_occurrence_id = t.visit_occurrence_id
                                                                        INNER JOIN {lookup_schema}.{lookup_table} l on (m.measurement_concept_id = l.concept_id
                                                                                                              AND
                                                                                                              l.variable_name IN ('weight'))
                                                                    WHERE
                                                                        m.unit_concept_id IN (9529, 8739)
                                                                    )
                                                                    SELECT
                                                                    visit_occurrence_id,
                                                                    [weight_kgs]
                                                                    FROM
                                                                        partitioned
                                                                    WHERE
                                                                        seq = 1 -- Ensure only the last observation of each type''',
                                                              con=engine)

        pressors = pressors.merge(first_weight_of_visit, on='visit_occurrence_id', how='left')

        pressors['weight_kgs'].fillna(70, inplace=True)  # assume 70 kg adult if missing

        pressors['final_dose'] = pressors['adjusted_dose'] / pressors['weight_kgs']

        p4_idx: pd.Series = (((pressors.Ingredient_name == 'epinephrine') & (pressors.final_dose > 0.1))

                             | ((pressors.Ingredient_name == 'dopamine') & (pressors.final_dose > 15))

                             | ((pressors.Ingredient_name == 'norepinephrine') & (pressors.final_dose > 0.1)))

        p3_idx: pd.Series = (((pressors.Ingredient_name == 'epinephrine') & (pressors.final_dose <= 0.1))

                             | ((pressors.Ingredient_name == 'dopamine') & (pressors.final_dose > 5))

                             | ((pressors.Ingredient_name == 'norepinephrine') & (pressors.final_dose <= 0.1)))

        p2_idx: pd.Series = ((pressors.Ingredient_name == 'dobutamine')

                             | ((pressors.Ingredient_name == 'dopamine') & (pressors.final_dose <= 5)))

        pressors.loc[p4_idx, 'SOFA_pressor_score'] = 4
        pressors.loc[p3_idx, 'SOFA_pressor_score'] = 3
        pressors.loc[p2_idx, 'SOFA_pressor_score'] = 2

        del p2_idx, p3_idx, p4_idx

        pressors.drug_exposure_start_datetime = pressors.drug_exposure_start_datetime.dt.floor('h')
        pressors.drug_exposure_end_datetime = pressors.drug_exposure_end_datetime.dt.ceil('h')

        # resample pressors to the hour
        resampled_pressors = pd.concat([pd.DataFrame(data={'SOFA_pressor_score': [row.SOFA_pressor_score, row.SOFA_pressor_score],
                                                           'visit_occurrence_id': [row.visit_occurrence_id, row.visit_occurrence_id]},
                                                     index=[row.drug_exposure_start_datetime, row.drug_exposure_end_datetime]).resample('1h').ffill() for _, row in pressors.iterrows()], axis=0)\
            .dropna(how='any')\
            .reset_index(drop=False)\
            .rename(columns={'index': 'observation_datetime'})\
            .groupby(['visit_occurrence_id', 'observation_datetime'])\
            .agg({'SOFA_pressor_score': 'max'})\
            .reset_index(drop=False)
    else:
        resampled_pressors: pd.DataFrame = pd.DataFrame(columns=['visit_occurrence_id', 'observation_datetime', 'SOFA_pressor_score'])

    visit_map: pd.DataFrame = _get_visit_labs(engine=engine, lab_type='mean_arterial_pressure', data_schema=data_schema,
                                              lookup_schema=lookup_schema,
                                              append_subject_id_type_if_missing=append_subject_id_type_if_missing,
                                              tempTableSchema=tempTableSchema, temp_table_name=temp_table_name, lookup_table=lookup_table,
                                              visit_df=visit_df, **local_kwargs)

    visit_map['SOFA_map_score'] = None

    if visit_map.shape[0] > 0:
        p1_idx: pd.Series = visit_map.mean_arterial_pressure < 70

        visit_map.loc[p1_idx, 'SOFA_map_score'] = 1

        visit_map['SOFA_map_score'].fillna(0, inplace=True)

        visit_map = visit_map\
            .rename(columns={'measurement_datetime': 'observation_datetime'})\
            .groupby(['visit_occurrence_id', 'observation_datetime'])\
            .agg({'SOFA_map_score': 'max'})\
            .reset_index(drop=False)

    cardio_score: pd.DataFrame = visit_map.merge(resampled_pressors, on=['visit_occurrence_id', 'observation_datetime'], how='outer')
    cardio_score['SOFA_cardio_score'] = cardio_score[['SOFA_map_score', 'SOFA_pressor_score']].sum(axis=1).astype(int)

    return cardio_score


if __name__ == '__main__':
    from sqlalchemy import create_engine
    engine: Engine = create_engine('dialect+driver://username:password@host:port/database')

    # from Database.connect_to_database import get_SQL_database_connection_v2
    # database: str = 'IDEALIST_OMOP'
    # engine = get_SQL_database_connection_v2(database=database,
    #                                         fast_engine=True)

    test = calculate_SOFA(engine=engine,
                          cohort_id=1050,
                          subject_id_type='visit_detail_id',
                          data_schema='CDM',
                          tempTableSchema='dbo',
                          results_schema='RESULTS',
                          lookup_schema='dbo',
                          vocab_schema='VOCAB')
    pass
