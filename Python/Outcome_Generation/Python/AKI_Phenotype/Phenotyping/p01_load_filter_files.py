# -*- coding: utf-8 -*-
"""
Created on Mon Nov 23 21:52:34 2020

@author: renyuanfang
"""
import os
import pandas as pd
from Utils.file_operations import load_data

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
                * height_cm
                * weight_kgs
                * patient_type
        eid: str
            encounter id column name
        pid: str
            patient id column name

    Returns
    -------
        encounters_simple: pandas.DataFrame
            filtered encounters dataframe with only pid, eid, 'admit_datetime', 'dischg_datetime', 'birth_date', 'sex', 'race', 'ethnicity', 'height_cm', 'weight_kgs', 'patient_type' columns
        encounters: pandas.DataFrame
            filtered encounters dataframe
    '''
    encounters = encounters[(encounters['admit_datetime'].notnull()) & (encounters['dischg_datetime'].notnull())]
    cols = [pid, eid, 'admit_datetime', 'dischg_datetime', 'birth_date', 'sex',
            'race', 'ethnicity', 'height_cm', 'weight_kgs', 'patient_type']
    encounters = encounters.drop_duplicates([eid]).reset_index(drop=True)
    encounters_simple = encounters[cols].drop_duplicates([eid]).reset_index(drop=True)
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
    if 'lab_id' in labs.columns:
        labs.loc[:, 'lab_id'] = labs['lab_id'].astype(str).copy()
        labs.loc[:, 'stamped_and_inferred_loinc_code'] = labs['stamped_and_inferred_loinc_code'].astype(str).copy()
        labs.loc[labs['lab_id'] == '969', 'stamped_and_inferred_loinc_code'] = '2160-0'
        labs.loc[labs['lab_id'] == '1526296', 'stamped_and_inferred_loinc_code'] = '2160-0'
        labs.loc[labs['lab_id'] == '3028', 'stamped_and_inferred_loinc_code'] = '38483-4'
        labs.loc[labs['lab_id'] == '1510379', 'stamped_and_inferred_loinc_code'] = '2160-0'
        labs.loc[labs['lab_id'] == '3412', 'stamped_and_inferred_loinc_code'] = '2160-0'
        labs.loc[labs['lab_id'] == '3156', 'stamped_and_inferred_loinc_code'] = '2160-0'
        labs = labs[labs['stamped_and_inferred_loinc_code'].isin(['2160-0'])]
    else:
        labs = labs[labs['stamped_and_inferred_loinc_code'].isin(['2160-0'])]

    labs.loc[:, 'lab_result'] = labs['lab_result'].astype(str).copy()
    con1 = labs['lab_result'].str.contains('<|>', na=False)
    labs.loc[con1, 'lab_result'] = labs.loc[con1, 'lab_result'].str.strip('<>')
    con2 = (labs['lab_unit'] == 'mg/ml') | (labs['lab_unit'] == 'mg/mL')
    labs.loc[con2, 'lab_unit'] == 'mg/dl'
    labs['lab_result'] = pd.to_numeric(labs['lab_result'], errors='coerce')
    con3 = (labs['lab_result'] * 100 > 0.0) & (labs['lab_result'] * 100 <= 30.0)
    labs.loc[con2 & con3, 'lab_result'] = labs.loc[con2 & con3, 'lab_result'] * 100
    labs = labs[(labs['lab_result'].notnull()) & (labs['lab_result'] > 0)]
    return labs


def p01_load_filter_file(in_dir: str, inmd_dir: str, eid: str, pid: str,
                         enc_name: str, labs_name: str, diagnosis_name: str, procedure_name: str,
                         crrt_name: str, dialysis_name: str, batch: int):
    '''
    This function load and filter encounter, labs, diagnosis, procedure, crrt, and dialysis files and save filtered files to intermediate directory in csv format.

    1. Load encounter, labs, diagnosis, procedure, crrt, and dialysis files.
    2. Apply filter_encounter and filter_lab functions to filter encounter and lab files.
    3. Filter the rest of the files by keeping all patients in the filtered encounter file.
    4. Save all the filtered files batch by batch to intermediate directory in csv format.

    Parameters
    ----------
        in_dir: str
            input directory location
        inmd_dir:
            intermediate directory location
        enc_name: str
            encounter file name
        labs_name: str
            labs file name
        diagnosis_name: str
            diagnosis file name
        procedure_name: str
            procedure file name
        crrt_name: str
            crrt file name
        dialysis_name: str
            dialysis file name
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
    patterns: list = [r'_' + str(batch) + r'_[0-9]+\.csv',
                      r'_' + str(batch) + r'\.csv',
                      r'\.csv']

    encounters = load_data(directory=in_dir, file_path_query=enc_name, patterns=patterns, return_log=False,
                           pid=pid, eid=eid, regex=True)

    enc_map = encounters[[pid, eid, 'encounter_effective_date']]\
        .drop_duplicates()\
        .dropna()\
        .copy()

    labs = load_data(directory=in_dir, file_path_query=labs_name, patterns=patterns, return_log=False,
                     pid=pid, eid=eid, regex=True,
                     usecols=[pid, eid,
                              "lab_name", "lab_id",
                              "stamped_and_inferred_loinc_code",
                              "stamped_and_inferred_loinc_desc_code",
                              'result_datetime',
                              "lab_unit", "lab_result",
                              "inferred_specimen_datetime"])

    diagnosis = load_data(directory=in_dir, file_path_query=diagnosis_name, patterns=patterns, return_log=False,
                          pid=pid, eid=eid, regex=True,
                          usecols=[pid, "start_date",
                                   "diag_code", 'flag_poa_no',
                                   'diag_icd_type'])

    procedure = load_data(directory=in_dir, file_path_query=procedure_name, patterns=patterns, return_log=False,
                          pid=pid, eid=eid, regex=True)

    # load dialysis information
    crrt_cols = [pid, eid, 'recorded_time', 'vital_sign_group_name', 'vital_sign_measure_name', 'meas_value']
    crrt = load_data(directory=in_dir, file_path_query=crrt_name, patterns=patterns, usecols=crrt_cols,
                     pid=pid, eid=eid, regex=True)

    dialysis_cols = [pid, eid, 'observation_datetime', 'hemodialysis_intake', 'hemodialysis_output',
                     'peritoneal_dialysis_intake', 'peritoneal_dialysis_output']

    dialysis = load_data(directory=in_dir, file_path_query=dialysis_name, patterns=patterns, return_log=False,
                         pid=pid, eid=eid, regex=True)

    if isinstance(dialysis, pd.DataFrame):

        if len(list(dialysis.columns.intersection(['vital_sign_group_name', 'vital_sign_measure_name']))) == 2:

            crrt = dialysis[dialysis.vital_sign_group_name.isnull()]\
                .reset_index(drop=True)\
                .rename(columns={'observation_datetime': 'recorded_time'})[crrt_cols]

            dialysis = dialysis[dialysis.vital_sign_group_name.isnull()]\
                .drop(columns=['vital_sign_group_name'])\
                .pivot(index=list(dialysis.columns.intersection([pid, eid, 'observation_datetime',
                                                                'intraop_y_n', 'merged_enc_id', 'idr_intraop_y_n'])),
                       columns=['vital_sign_measure_name'])

            dialysis.columns = dialysis.columns.get_level_values(1)

            dialysis.reset_index(drop=False, inplace=True)

            for col in dialysis_cols:

                if col not in dialysis.columns:
                    dialysis[col] = None

            dialysis = dialysis[dialysis_cols]

    else:
        dialysis = pd.DataFrame(columns=dialysis_cols)

    if not isinstance(crrt, pd.DataFrame):
        crrt = pd.DataFrame(columns=crrt_cols)

    # filter encounters
    encounters, encounters_all = p01_filter_encounter(encounters=encounters, eid=eid, pid=pid)

    # filter labs
    labs = labs[labs[pid].isin(encounters[pid])]
    labs = p01_filter_lab(labs).drop_duplicates().reset_index(drop=True)

    # filter diagnosis, procedure, crrt, dialysis
    diagnosis = diagnosis[diagnosis[pid].isin(encounters[pid])].drop_duplicates().reset_index(drop=True)
    procedure = procedure[procedure[pid].isin(encounters[pid])].drop_duplicates().reset_index(drop=True)
    crrt = crrt[crrt[pid].isin(encounters[pid])].drop_duplicates().reset_index(drop=True)
    dialysis = dialysis[dialysis[pid].isin(encounters[pid])].drop_duplicates().reset_index(drop=True)

    dirs = ['filtered_encounters', 'filtered_labs', 'filtered_diagnosis', 'filtered_procedure', 'filtered_crrt', 'filtered_dialysis']
    for path in dirs:
        path = os.path.join(inmd_dir, path)
        if not os.path.exists(path):
            os.makedirs(path)
    encounters.to_csv(os.path.join(inmd_dir, 'filtered_encounters', 'filtered_encounters_{}.csv'.format(batch)), index=False)
    encounters_all.to_csv(os.path.join(inmd_dir, 'filtered_encounters', 'filtered_encounters_all_{}.csv'.format(batch)), index=False)
    labs.to_csv(os.path.join(inmd_dir, 'filtered_labs', 'filtered_labs_{}.csv'.format(batch)), index=False)
    diagnosis.to_csv(os.path.join(inmd_dir, 'filtered_diagnosis', 'filtered_diagnosis_{}.csv'.format(batch)), index=False)
    procedure.to_csv(os.path.join(inmd_dir, 'filtered_procedure', 'filtered_procedure_{}.csv'.format(batch)), index=False)
    crrt.to_csv(os.path.join(inmd_dir, 'filtered_crrt', 'filtered_crrt_{}.csv'.format(batch)), index=False)
    dialysis.to_csv(os.path.join(inmd_dir, 'filtered_dialysis', 'filtered_dialysis_{}.csv'.format(batch)), index=False)
    enc_map.to_csv(os.path.join(inmd_dir, 'filtered_encounters', 'enc_id_map_file_{batch}.csv'), index=False)
