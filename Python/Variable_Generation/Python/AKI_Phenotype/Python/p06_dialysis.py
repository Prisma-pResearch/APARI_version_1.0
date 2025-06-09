# -*- coding: utf-8 -*-
"""
Created on Wed Nov 25 00:38:56 2020.

@author: renyuanfang
"""
import os
import pandas as pd
from datetime import timedelta
from .Utilities.FileHandling.io import check_load_df, save_data


def p06_generate_rrt_summary(inmd_dir: str, eid: str, pid: str, batch: int, **logging_kwargs):
    '''
    This function is to find all results related to dialysis including dialysis indicator in one calander day frequency, all time periods under dialysis, all datetime under dialysis recorded in dialysis, and procedure files, brief dialysis statistic summary.
    1. Read all encounters, must contain following columns:
        * pid
        * eid
        * admit_datetime
        * dischg_datetime
    2. Read dialysis, information:
        * pid
        * eid
        * start_datetime
    3. Read and filter procedure file related to dialysis, must contain following columns:
        * pid
        * procedure_datetime
       Then map procedure file with encounter file.
    5. Combine dialysis and procedure file a compelte dialysis file with each row containing eid, dialysis time and save the file
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
    encounter = check_load_df(os.path.join(inmd_dir, 'filtered_encounters', 'filtered_encounters_{}.csv'.format(batch)),
                              usecols=[pid, eid, 'admit_datetime', 'dischg_datetime'],
                              parse_dates=['admit_datetime', 'dischg_datetime'],
                              pid=pid, eid=eid, dtype=None, **logging_kwargs)

    dialysis = check_load_df(os.path.join(inmd_dir, 'filtered_dialysis', 'filtered_dialysis_{}.csv'.format(batch)),
                             parse_dates=['start_datetime'], pid=pid, eid=eid, dtype=None, **logging_kwargs)

    dialysis.rename(columns={'start_datetime': 'dialysis_time'}, inplace=True)


    procedure = check_load_df(os.path.join(inmd_dir, 'filtered_procedure', 'filtered_procedure_{}.csv'.format(batch)),
                              usecols=[pid, 'proc_date'], parse_dates=['proc_date'],
                              pid=pid, eid=eid, dtype=None, **logging_kwargs).drop_duplicates()
    
    procedure = procedure.merge(encounter, on=pid, how='inner')
    for x in ['proc_date', 'admit_datetime', 'dischg_datetime']:
        procedure[x] = pd.to_datetime(procedure[x]).dt.date
    procedure = procedure[(procedure['proc_date'] >= procedure['admit_datetime']) & (procedure['proc_date'] <= procedure['dischg_datetime'])]
    procedure = procedure[[eid, 'proc_date']].drop_duplicates().rename(columns={'proc_date': 'dialysis_time'})
    procedure['dialysis_time'] = pd.to_datetime(procedure['dialysis_time'])

    dialysis_df = pd.concat([dialysis, procedure], ignore_index=True)
    dialysis_df = dialysis_df.sort_values([eid, 'dialysis_time']).drop_duplicates().reset_index(drop=True)
    dialysis_df = dialysis_df.merge(encounter, on=eid, how='left')
    dialysis_df = dialysis_df[dialysis_df['dialysis_time'] <= dialysis_df['dischg_datetime']]
    dialysis_df = dialysis_df[[eid, 'dialysis_time']]

    save_data(df=dialysis_df,
              out_path=os.path.join(inmd_dir, 'dialysis_time', 'dialysis_time_{}.csv'.format(batch)), index=False, **logging_kwargs)

    # dialysis summary
    dialysis_summary = encounter[[eid, 'admit_datetime', 'dischg_datetime']]
    dialysis_summary['rrt_overall'] = 0
    dialysis_summary['rrt_24h'] = 0

    dialysis_summary.loc[dialysis_summary[eid].isin(dialysis_df[eid]), 'rrt_overall'] = 1
    temp = dialysis_df.merge(dialysis_summary, on=eid, how='left')
    dialysis_24h = temp[temp['dialysis_time'] < temp['admit_datetime'] + timedelta(hours=24)]
    dialysis_summary.loc[dialysis_summary[eid].isin(dialysis_24h[eid]), 'rrt_24h'] = 1
    dialysis_summary = dialysis_summary.drop(columns=['admit_datetime', 'dischg_datetime'])
    save_data(df=dialysis_summary,
              out_path=os.path.join(inmd_dir, 'dialysis_time', 'dialysis_summary_{}.csv'.format(batch)), index=False, **logging_kwargs)
