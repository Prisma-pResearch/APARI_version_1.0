# -*- coding: utf-8 -*-
"""
Created on Wed Nov 25 00:38:56 2020.

@author: renyuanfang
"""
import os
import pandas as pd
from datetime import timedelta
from Utils.file_operations import load_data


def p06_generate_rrt_summary(inmd_dir: str, eid: str, pid: str, batch: int):
    '''
    This function is to find all results related to dialysis including dialysis indicator in one calander day frequency, all time periods under dialysis, all datetime under dialysis recorded in crrt, dialysis, and procedure files, brief dialysis statistic summary.
    1. Read all encounters, must contain following columns:
        * pid
        * eid
        * admit_datetime
        * dischg_datetime
    2. Read and filter crrt file related to dialysis, must contain following columns:
        * pid
        * eid
        * recorded_time
        * vital_sign_measure_name
        * meas_value
    3. Read and filter dialysis file, must contain following columns:
        * pid
        * eid
        * observation_datetime
        * hemodialysis_intake
        * hemodialysis_output
        * peritoneal_dialysis_intake
        * peritoneal_dialysis_output
    4. Read and filter procedure file related to dialysis, must contain following columns:
        * pid
        * proc_date
        * proc_code
       Then map procedure file with encounter file.
    5. Combine crrt, dialysis and procedure file a compelte dialysis file with each row containing eid, dialysis time and save the file
    6. Generate summary of dialysis statistic including 'rrt_overall', 'rrt_24h' and save the file

    Parameters
    ----------
        inmd_dir: str
            intermediate file directory
        batch: int
            batch number, initial value = 0
        eid: str
            'merged_enc_id'
        pid: str
            patient id column name, default value = 'patient_deiden_id'
    Returns
    -------
    None
    '''
    encounter = load_data(os.path.join(inmd_dir, 'filtered_encounters', 'filtered_encounters_{}.csv'.format(batch)),
                          usecols=[pid, eid, 'admit_datetime', 'dischg_datetime'],
                          parse_dates=['admit_datetime', 'dischg_datetime'],
                          pid=pid, eid=eid)

    crrt = load_data(os.path.join(inmd_dir, 'filtered_crrt', 'filtered_crrt_{}.csv'.format(batch)),
                     parse_dates=['recorded_time'], pid=pid, eid=eid)
    crrt = crrt[crrt['vital_sign_measure_name'] == 'Treatment Type']
    crrt = crrt[crrt['meas_value'].isin(['CVVH', 'CVVHD', 'CVVHDF'])]
    crrt = crrt[crrt[eid].notnull()]
    crrt = crrt[[eid, 'recorded_time']].drop_duplicates().rename(columns={'recorded_time': 'dialysis_time'})

    dialysis = load_data(os.path.join(inmd_dir, 'filtered_dialysis', 'filtered_dialysis_{}.csv'.format(batch)),
                         parse_dates=['observation_datetime'], pid=pid, eid=eid)
    dialysis = dialysis[[pid, eid, 'observation_datetime',
                         'hemodialysis_intake', 'hemodialysis_output', 'peritoneal_dialysis_intake', 'peritoneal_dialysis_output']]
    dialysis = dialysis[pd.notnull(dialysis[['hemodialysis_intake', 'hemodialysis_output', 'peritoneal_dialysis_intake', 'peritoneal_dialysis_output']]).any(axis=1)]
    dialysis = dialysis[dialysis[eid].notnull()]
    dialysis = dialysis[[eid, 'observation_datetime']].drop_duplicates().rename(columns={'observation_datetime': 'dialysis_time'})

    rrt_proc_codes_cpt: list = ['90935', '90937', '90945', '90947', '90999']
    procedure = load_data(os.path.join(inmd_dir, 'filtered_procedure', 'filtered_procedure_{}.csv'.format(batch)),
                          pid=pid, eid=eid)
    procedure = procedure[(procedure['proc_code_type'] == 'CPT') & (procedure['proc_code'].isin(rrt_proc_codes_cpt))]
    procedure = procedure[[pid, 'proc_date']].drop_duplicates()
    procedure = procedure.merge(encounter, on=pid, how='inner')
    for x in ['proc_date', 'admit_datetime', 'dischg_datetime']:
        procedure[x] = pd.to_datetime(procedure[x]).dt.date
    procedure = procedure[(procedure['proc_date'] >= procedure['admit_datetime']) & (procedure['proc_date'] <= procedure['dischg_datetime'])]
    procedure = procedure[[eid, 'proc_date']].drop_duplicates().rename(columns={'proc_date': 'dialysis_time'})
    procedure['dialysis_time'] = pd.to_datetime(procedure['dialysis_time'])

    dialysis_df = pd.concat([crrt, dialysis, procedure], ignore_index=True)
    dialysis_df = dialysis_df.sort_values([eid, 'dialysis_time']).drop_duplicates().reset_index(drop=True)
    dialysis_df = dialysis_df.merge(encounter, on=eid, how='left')
    dialysis_df = dialysis_df[dialysis_df['dialysis_time'] <= dialysis_df['dischg_datetime']]
    dialysis_df = dialysis_df[[eid, 'dialysis_time']]

    if not os.path.exists(os.path.join(inmd_dir, 'dialysis_time')):
        os.makedirs(os.path.join(inmd_dir, 'dialysis_time'))

    dialysis_df.to_csv(os.path.join(inmd_dir, 'dialysis_time', 'dialysis_time_{}.csv'.format(batch)), index=False)

    # dialysis summary
    dialysis_summary = encounter[[eid, 'admit_datetime', 'dischg_datetime']]
    dialysis_summary['rrt_overall'] = 0
    dialysis_summary['rrt_24h'] = 0

    dialysis_summary.loc[dialysis_summary[eid].isin(dialysis_df[eid]), 'rrt_overall'] = 1
    temp = dialysis_df.merge(dialysis_summary, on=eid, how='left')
    dialysis_24h = temp[temp['dialysis_time'] < temp['admit_datetime'] + timedelta(hours=24)]
    dialysis_summary.loc[dialysis_summary[eid].isin(dialysis_24h[eid]), 'rrt_24h'] = 1
    dialysis_summary = dialysis_summary.drop(columns=['admit_datetime', 'dischg_datetime'])
    dialysis_summary.to_csv(os.path.join(inmd_dir, 'dialysis_time', 'dialysis_summary_{}.csv'.format(batch)), index=False)
