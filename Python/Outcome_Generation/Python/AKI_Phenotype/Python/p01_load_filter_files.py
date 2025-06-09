# -*- coding: utf-8 -*-
"""
Created on Mon Nov 23 21:52:34 2020.

@author: renyuanfang
@Editor: Ruppert20
"""
import os
import pandas as pd
from typing import Union
from .Utilities.FileHandling.io import check_load_df, save_data
from .Utilities.PreProcessing.data_format_and_manipulation import stack_df
from .Utilities.PreProcessing.clean_labs import clean_labs
from .Utilities.FileHandling.variable_specification_utilities import load_variables_from_var_spec


def p01_filter_encounter(encounters: pd.DataFrame, eid: str, pid: str):
    '''
    This function keep only encounters with non-missing admit & discharge datetime.

    1. Remove the rows with admit_datetime and dischg_datetime that is null.
    2. Remove duplicate rows and reset dataframe index.

    Parameters
    ----------
        encounters: pandas.DataFrame
            encounters dataframe, must contains following columns:
                * pid
                * eid
                * admit_datetime
                * dischg_datetime
                * birth_date
                * sex
                * race
                * ethnicity
        eid: str
            encounter id column name
        pid: str
            patient id column name

    Returns
    -------
        encounters_simple: pandas.DataFrame
            filtered encounters dataframe with only pid, eid, 'admit_datetime', 'dischg_datetime', 'birth_date', 'sex', 'race', 'ethnicity' columns
        encounters: pandas.DataFrame
            filtered encounters dataframe
    '''
    encounters = encounters[(encounters['admit_datetime'].notnull()) & (encounters['dischg_datetime'].notnull())]
    encounters = encounters.drop_duplicates([eid]).reset_index(drop=True)
    encounters_simple = check_load_df(input_v=encounters.rename(columns={'gender_concept_id': 'sex',
                                                                         'race_concept_id': 'race',
                                                                         'ethnicity_concept_id': 'ethnicity'}),
                                      use_col_intersection=True,
                                      usecols=[pid, eid, 'admit_datetime', 'dischg_datetime', 'birth_date', 'sex',
                                               'race', 'ethnicity'])\
        .drop_duplicates([eid]).reset_index(drop=True)
    return encounters_simple, encounters


def p01_filter_lab(labs: pd.DataFrame) -> pd.DataFrame:
    '''
    This function keep labs with 2160-0 LOINC code and non-missing lab result.

    1. Locate specific lab_id and update stamped_and_inferred_loinc_code accordingly.
        * for lab_id: '969', '1526296', '1510379', '3412', '3156', update stamped_and_inferred_loinc_code to '2160-0'.
        * for lab_id: '3028', update stamped_and_inferred_loinc_code to '38483-4'.
    2. Filter all the labs that has stamped_and_inferred_loinc_code as '2160-0'.
    3. Clean 'lab_result' and 'lab_unit' columns. Remove rows that does not have value in 'lab_result'.

    Parameters
    ----------
        labs: pandas.DataFrame
            Labs dataframe, must contains the following columns:
                * lab_id
                * stamped_and_inferred_loinc_code
                * lab_result
                * lab_unit

    Returns
    -------
    pandas.DataFrame
        filtered lab dataframe
    '''
    labs.loc[:, 'stamped_and_inferred_loinc_code'] = labs['stamped_and_inferred_loinc_code'].astype(str).copy()

    if 'lab_id' in labs.columns:
        labs.loc[:, 'lab_id'] = labs['lab_id'].astype(str).copy()
        labs.loc[labs['lab_id'] == '969', 'stamped_and_inferred_loinc_code'] = '2160-0'
        labs.loc[labs['lab_id'] == '1526296', 'stamped_and_inferred_loinc_code'] = '2160-0'
        labs.loc[labs['lab_id'] == '3028', 'stamped_and_inferred_loinc_code'] = '38483-4'
        labs.loc[labs['lab_id'] == '1510379', 'stamped_and_inferred_loinc_code'] = '2160-0'
        labs.loc[labs['lab_id'] == '3412', 'stamped_and_inferred_loinc_code'] = '2160-0'
        labs.loc[labs['lab_id'] == '3156', 'stamped_and_inferred_loinc_code'] = '2160-0'
    labs = labs[labs['stamped_and_inferred_loinc_code'].isin(['2160-0'])]

    labs.loc[:, 'lab_unit'] = labs.loc[:, 'lab_unit'].str.lower()
    labs.loc[:, 'lab_result'] = labs['lab_result'].astype(str).copy()
    con1 = labs['lab_result'].str.contains('<|>', na=False)
    labs.loc[con1, 'lab_result'] = labs.loc[con1, 'lab_result'].str.strip('<>').values
    con2 = (labs['lab_unit'] == 'mg/ml')
    labs.loc[con2, 'lab_unit'] == 'mg/dl'
    labs.loc[:, 'lab_result'] = pd.to_numeric(labs['lab_result'], errors='coerce').values
    con3 = (labs['lab_result'] * 100 > 0.0) & (labs['lab_result'] * 100 <= 30.0)
    labs.loc[con2 & con3, 'lab_result'] = labs.loc[con2 & con3, 'lab_result'] * 100
    labs = labs[(labs['lab_result'].notnull()) & (labs['lab_result'] > 0)]
    return labs


def p01_load_filter_file(in_dir: str, inmd_dir: str, var_file_linkage_fp: str, batch: str, patterns: list, pid: str = 'person_id', eid: str = 'visit_occurrence_id', **logging_kwargs):
    '''
    This function load and filter encounter, labs, diagnosis, procedure, and dialysis files and save filtered files to intermediate directory in csv format.

    1. Load encounter, labs, diagnosis, procedure, and dialysis files.
    2. Apply filter_encounter and filter_lab functions to filter encounter and lab files.
    3. Filter the rest of the files by keeping all patients in the filtered encounter file.
    4. Save all the filtered files batch by batch to intermediate directory in csv format.

    Parameters
    ----------
        in_dir: str
            input directory location
        inmd_dir:
            intermediate directory location
        var_file_linkage_fp: str
            var_file_linkage_fp file name
        batch: int
            batch number, default value = 0
        eid: str
            encounter id column name, default value = 'merged_enc_id'
        pid: str
            patient id column name
        in_batch: bool
            indicator of batched data, default value = True

    Returns
    -------
    None
    '''

    encounters = load_variables_from_var_spec(variables_columns=['visit_start_date', 'visit_start_datetime', 'visit_end_date', 'visit_end_datetime', 'discharged_to_concept_id'],
                                              var_spec_key=var_file_linkage_fp,
                                              data_source_key=in_dir,
                                              project='AKI_Phenotype',
                                              mute_duplicate_var_warnings=True,
                                              patterns=patterns,
                                              allow_empty_files=True, regex=True, dtype=None, ds_type='pandas', **logging_kwargs)\
        .rename(columns={'visit_start_datetime': 'admit_datetime',
                         'visit_end_datetime': 'dischg_datetime',
                         'visit_start_date': 'encounter_effective_date'})\
        .merge(load_variables_from_var_spec(variables_columns=['birth_date', 'gender_concept_id', 'race_concept_id', 'ethnicity_concept_id'],
                                            var_spec_key=var_file_linkage_fp,
                                            data_source_key=in_dir,
                                            project='AKI_Phenotype',
                                            mute_duplicate_var_warnings=True,
                                            patterns=patterns,
                                            allow_empty_files=True, regex=True, dtype=None, ds_type='pandas', **logging_kwargs),
               how='left',
               on=pid)

    enc_map = encounters[[pid, eid, 'encounter_effective_date']]\
        .drop_duplicates()\
        .dropna()\
        .copy()

    labs: pd.DataFrame = load_variables_from_var_spec(variables_columns=['creatinine'],
                                                      usecols=[pid, eid, "lab_id", 'variable_name', 'lab_type',
                                                               "stamped_and_inferred_loinc_code",
                                                               'result_datetime', 'value_as_number', 'unit_concept_id', 'unit_source_concept_id',
                                                               'unit_source_value', 'value_source_value',
                                                               'measurement_datetime'],
                                                      var_spec_key=var_file_linkage_fp,
                                                      filter_variables=True,
                                                      data_source_key=in_dir,
                                                      project='AKI_Phenotype',
                                                      mute_duplicate_var_warnings=True,
                                                      patterns=patterns,
                                                      allow_empty_files=True, regex=True, dtype=None, ds_type='pandas', **logging_kwargs)

    if 'stamped_and_inferred_loinc_code' not in labs.columns:
        if 'lab_type' not in labs.columns:
            labs = clean_labs(df=labs,
                              id_cols=[pid, eid],
                              debug_conversion=False,
                              return_labs_only=True,
                              **logging_kwargs)
        labs['stamped_and_inferred_loinc_code'] = '2160-0' if labs.shape[0] > 0 else None
        labs.drop(columns=['lab_type'], inplace=True)

    diagnosis: pd.DataFrame = load_variables_from_var_spec(variables_columns=['aki', 'ckd', 'esrd', 'renal_transplant', 'dialysis'],
                                                           usecols=[pid, 'condition_start_date', 'variable_name', 'condition_concept_id'],
                                                           var_spec_key=var_file_linkage_fp,
                                                           data_source_key=in_dir,
                                                           cdm_tables=['condition_occurrence'],
                                                           project='AKI_Phenotype',
                                                           filter_variables=True,
                                                           mute_duplicate_var_warnings=True,
                                                           patterns=patterns,
                                                           allow_empty_files=True, regex=True, dtype=None, ds_type='pandas', **logging_kwargs)\
        .rename(columns={'condition_start_date': 'start_date'})

    procedure: pd.DataFrame = load_variables_from_var_spec(variables_columns=['renal_transplant', 'dialysis'],
                                                           usecols=[pid, 'procedure_datetime', 'variable_name', 'procedure_concept_id'],
                                                           var_spec_key=var_file_linkage_fp,
                                                           data_source_key=in_dir,
                                                           cdm_tables=['procedure_occurrence'],
                                                           project='AKI_Phenotype',
                                                           filter_variables=True,
                                                           mute_duplicate_var_warnings=True,
                                                           patterns=patterns,
                                                           allow_empty_files=True, regex=True, dtype=None, ds_type='pandas', **logging_kwargs)\
        .rename(columns={'procedure_datetime': 'proc_date'})

    dialysis: pd.DataFrame = load_variables_from_var_spec(variables_columns=['dialysis'],
                                                          usecols=[pid, 'variable_name', 'device_exposure_start_datetime', 'observation_datetime'],
                                                          var_spec_key=var_file_linkage_fp,
                                                          data_source_key=in_dir,
                                                          cdm_tables=['observation', 'device_exposure'],
                                                          filter_variables=True,
                                                          project='AKI_Phenotype',
                                                          mute_duplicate_var_warnings=True,
                                                          patterns=patterns,
                                                          coalesce_fields={'start_datetime': ['device_exposure_start_datetime', 'observation_datetime']},
                                                          allow_empty_files=True, regex=True, dtype=None, ds_type='pandas', **logging_kwargs)

    # filter encounters
    encounters, encounters_all = p01_filter_encounter(encounters=encounters, eid=eid, pid=pid)

    # filter labs
    labs = labs[labs[pid].isin(encounters[pid])]
    labs = p01_filter_lab(labs.rename(columns={'unit_source_value': "lab_unit",
                                               'value_source_value': "lab_result",
                                               'measurement_datetime': "inferred_specimen_datetime"})).drop_duplicates().reset_index(drop=True)

    # filter diagnosis, procedure, dialysis
    diagnosis = diagnosis[diagnosis[pid].isin(encounters[pid])].drop_duplicates().reset_index(drop=True)
    procedure = procedure[procedure[pid].isin(encounters[pid])].drop_duplicates().reset_index(drop=True)
    dialysis = dialysis[dialysis[pid].isin(encounters[pid])].drop_duplicates().reset_index(drop=True)

    # save files
    save_data(df=encounters, out_path=os.path.join(inmd_dir, 'filtered_encounters', 'filtered_encounters_{}.csv'.format(batch)))
    save_data(df=encounters_all, out_path=os.path.join(inmd_dir, 'filtered_encounters', 'filtered_encounters_all_{}.csv'.format(batch)), index=False)
    save_data(df=labs, out_path=os.path.join(inmd_dir, 'filtered_labs', 'filtered_labs_{}.csv'.format(batch)), index=False)
    save_data(df=diagnosis, out_path=os.path.join(inmd_dir, 'filtered_diagnosis', 'filtered_diagnosis_{}.csv'.format(batch)), index=False)
    save_data(df=procedure, out_path=os.path.join(inmd_dir, 'filtered_procedure', 'filtered_procedure_{}.csv'.format(batch)), index=False)
    save_data(df=dialysis, out_path=os.path.join(inmd_dir, 'filtered_dialysis', 'filtered_dialysis_{}.csv'.format(batch)), index=False)
    save_data(df=enc_map, out_path=os.path.join(inmd_dir, 'filtered_encounters', 'enc_id_map_file_{}.csv'.format(batch)), index=False)
