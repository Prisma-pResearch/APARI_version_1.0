# -*- coding: utf-8 -*-
"""
Module to upload cohort to results schema.

Created on Tue May 17 08:29:32 2022

@author: ruppert20
"""
import pandas as pd
from ..FileHandling.io import load_data, save_data
from sqlalchemy.engine.base import Engine
from datetime import datetime as dt


def populate_omop_cohort(sql_file: str,
                         results: pd.DataFrame,
                         results_schema: str,
                         engine: Engine,
                         cohort_name: str,
                         definition_desc: str,
                         subject_id_field: str,
                         start_date_field: str,
                         end_date_field: str):
    """
    Populate OMOP cohort table using a specified query.

    Parameters
    ----------
    sql_file : str
        file path to a .sql file or the query itself in string format.
    results: pd.DataFrame
        formatted query results after subset labeling and cohort labeling
    results_schema : str
        Schema and or database where the cohort should be uploaded. e.g. database.schema or schema
    engine : Engine
        SQLALCHEMY connection engine.
    cohort_name : str
        Name to identify to cohort by.
    definition_desc : str
        Narrative description for the cohort.
    subject_id_field : str
        How the subjects for the cohort will be identified. It must either be 'visit_detail_id', 'person_id', or 'visit_occurrence_id'.
    start_date_field : str
        the field used as the start of the cohort.
    end_date_field : str
        the field used as the end of the cohort.

    Returns
    -------
    cohort_id: int.

    """
    # check assertions
    id_fields: dict = {'visit_detail_id': 1147637,
                       'person_id': 1147314,
                       'visit_occurrence_id': 1147332,
                       'procedure_occurrence_id': 1147301,
                        'condition_occurrence_id': 1147333,
                        'drug_exposure_id': 1147339,
                         'device_exposure_id': 1147305}
    
    assert subject_id_field in id_fields.keys(), f'You must identify the subjects via {id_fields.keys()}, however; {subject_id_field} was found'

    date: str = dt.now().strftime('%Y-%m-%d')

    if '.' in results_schema:
        database: str = f'[{results_schema.split(".")[0]}].'
        schema: str = f'[{results_schema.split(".")[1]}]'
    else:
        schema: str = f'[{results_schema}]'
        database: str = ''

    assert load_data(f"""SELECT *
                           FROM {database}{schema}.COHORT_DEFINITION
                           WHERE cohort_definition_name = '{cohort_name}' AND cohort_initiation_date = '{date}'""", engine=engine).shape[0] == 0, 'A cohort by that name has already been initiated today'

    assert results.shape[0] > 0, 'There were no subjects found by your eligibility query, Aborting'

    for col in [end_date_field, start_date_field, subject_id_field]:
        assert col in results.columns, f'The column: {col} was not found in the query results. Columns include: {results.columns.tolist()}'

    # upload cohort metadata to cohort definition
    save_data(df=pd.DataFrame({'cohort_definition_name': [cohort_name],
                               'cohort_definition_description': [definition_desc],
                               'definition_type_concept_id': [0],  # TODO: Figure out what this means
                               'cohort_definition_syntax': [sql_file if 'SELECT' in sql_file else load_data(sql_file)],
                               'subject_concept_id': [id_fields.get(subject_id_field)],
                               
                               'cohort_initiation_date': [date]}),
              dest_table='COHORT_DEFINITION',
              dest_schema=f'{database}{schema}',
              engine=engine)

    # retrieve cohort ID
    cohort_id: int = load_data(f'''SELECT
                                       cohort_definition_id
                                   FROM
                                       {database}{schema}.COHORT_DEFINITION
                                   WHERE
                                       cohort_initiation_date = '{date}'
                                       AND
                                       cohort_definition_name = '{cohort_name}';''',
                               engine=engine).cohort_definition_id.iloc[0]

    results['cohort_definition_id'] = cohort_id

    # upload results to cohort table
    save_data(df=results[['cohort_definition_id', subject_id_field, start_date_field, end_date_field, 'subset_id']]
              .rename(columns={subject_id_field: 'subject_id',
                               start_date_field: 'cohort_start_date',
                               end_date_field: 'cohort_end_date'}),
              dest_table='COHORT',
              dest_schema=f'{database}{schema}',
              engine=engine)

    return cohort_id