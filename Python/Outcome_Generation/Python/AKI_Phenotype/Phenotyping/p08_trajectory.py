# -*- coding: utf-8 -*-
"""
Created on Mon Dec  7 11:19:17 2020

@author: renyuanfang
"""
import os
import pandas as pd
import numpy as np
from Utils.file_operations import load_data
from datetime import timedelta


def p08_cal_mortality(encounter, pid: str):
    '''
    This function is used to calculate mortality including 30 day, 90 day, 6 month, 1 year, 2 year and 3 year.

    1. Impute death date by first using encounter death date if exists and ssdi death date otherwise; carry forward and backward death date for same patients.
    2. Calcluate time from admission to death time, and from discharge to death time.
    3. Patient has hospital mortality if
        i) death date between admission date and discharge date
        ii) discharge disposition belongs to ['EXPIRED', 'EXPIRED AUT', 'EXPIRED AUT UNK', 'EXPIRED NO AUT'] and discharge time to death time is less than or equal to 1 day
        iii) discharge disposition belongs to ['TO HOSPICE FACILITY', 'TO HOSPICE HOME'] and discharge time to death time is less than or equal to 7 days.
    4. Determine 30 day, 90 day, 6 month, 1 year, 2 year and 3 year mortality if patient does not have hospital mortality and admission time to death time is less than or equal to specific time limitation.

    Parameters
    ----------
        encounters: pandas.DataFrame
            encounters dataframe, must contain following columns:
            * pid
            * eid
            * admit_datetime
            * dischg_datetime
            * death_date
            * ssdi_death_date
            * dischg_disposition
        pid: str
            patient id column name, default value = 'patient_deiden_id'

    Returns
    encounter: pandas.Dataframe
        encounter dataframe with newly generated mortality columns
    '''
    encounter['death_date_combined'] = encounter['death_date']
    if 'ssdi_death_date' in encounter.columns.tolist():
        con = (encounter['death_date_combined'].isnull()) & (encounter['ssdi_death_date'].notnull())
        encounter.loc[con, 'death_date_combined'] = encounter['ssdi_death_date']
    encounter['death_date_combined'] = encounter.groupby([pid])['death_date_combined'].ffill()
    encounter['death_date_combined'] = encounter.groupby([pid])['death_date_combined'].bfill()
    for x in ['admit_datetime', 'dischg_datetime', 'death_date_combined']:
        encounter[x] = pd.to_datetime(encounter[x])
    con = encounter['death_date_combined'].notnull()
    encounter['dischg_to_death_time'] = np.nan
    encounter['admission_to_death_time'] = np.nan
    encounter.loc[con, 'dischg_to_death_time'] = encounter.loc[con, ["death_date_combined", "dischg_datetime"]].apply(lambda x: (x[0] - x[1]) / timedelta(days=1.0), axis=1)
    encounter.loc[con, 'admission_to_death_time'] = encounter.loc[con, ["death_date_combined", "admit_datetime"]].apply(lambda x: (x[0] - x[1]) / timedelta(days=365.2425), axis=1)
    encounter['admission_to_death_time'] = pd.to_numeric(encounter['admission_to_death_time'], errors='coerce')
    encounter['dischg_to_death_time'] = pd.to_numeric(encounter['dischg_to_death_time'], errors='coerce')

    encounter['hospital_mortality'] = 'N'
    con = (encounter['death_date_combined'].notnull()) & (encounter['death_date_combined'].dt.date >= encounter['admit_datetime'].dt.date) & (encounter['death_date_combined'].dt.date <= encounter['dischg_datetime'].dt.date)
    encounter.loc[con, 'hospital_mortality'] = 'Y'

    con = (encounter['dischg_to_death_time'] <= 1) & (encounter['hospital_mortality'] == 'N') & ((encounter['dischg_disposition'] == 'EXPIRED') | (encounter['dischg_disposition'] == 'EXPIRED AUT') | (encounter['dischg_disposition'] == 'EXPIRED AUT UNK') | (encounter['dischg_disposition'] == 'EXPIRED NO AUT'))
    encounter.loc[con, 'hospital_mortality'] = 'Y'

    con = (encounter['dischg_to_death_time'] <= 7) & (encounter['hospital_mortality'] == 'N') & ((encounter['dischg_disposition'] == 'TO HOSPICE FACILITY') | (encounter['dischg_disposition'] == 'TO HOSPICE HOME'))
    encounter.loc[con, 'hospital_mortality'] = 'Y'

    for x, y in zip([0.085, 0.25, 0.5, 1, 2, 3], ['30d', '90d', '6m', '1y', '2y', '3y']):
        mort_name = 'mort_status_' + y
        encounter[mort_name] = 0
        con = (encounter['death_date_combined'].notnull()) & (encounter['admission_to_death_time'] <= x)
        encounter.loc[con, mort_name] = 1
    return encounter


def p08_get_dischg_position(encounter):
    '''
    This function is used to category discharge disposition into three classes including 'Dead' (hospital dead), 'Home/Rehab' and 'ANOTHER HOSPITAL/LTAC/SNF/HOSPICE'.

    Parameters
    ----------
        encounter: pandas.DataFrame
            encounter dataframe, must contain following following columns:
            * dischg_disposition
            * hospital_mortality

    Returns
    -------
    encounter: pandas.Dataframe
        dataframe with newly generated column 'dischg_place'
    '''
    encounter['dischg_place'] = np.nan
    encounter.loc[encounter['hospital_mortality'] == 'Y', 'dischg_place'] = 'Dead'
    con = (encounter['dischg_place'].isnull()) & (encounter['dischg_disposition'].str.contains('HOME|HOMECARE|COURT|CUSTODIAL|REHAB|AMA|LWBS', regex=True, na=False))
    encounter.loc[con, 'dischg_place'] = 'Home/Rehab'
    encounter.loc[(encounter['dischg_place'].isnull()) & (encounter['dischg_disposition'].isnull()), 'dischg_place'] = 'Home/Rehab'
    encounter['dischg_place'] = encounter['dischg_place'].fillna('ANOTHER HOSPITAL/LTAC/SNF/HOSPICE')
    return encounter


def p08_generateTrajectory(inmd_dir: str, in_dir: str, batch: int, eid: str, pid: str, ssdi_name: str = None):
    '''
    This function is used to generate AKI trajectory groups including 'pAKI with recovery' (persistent AKI with renal recovery), 'pAKI without recovery' (persistent AKI without renal recovery), 'non-pAKI with recovery' (rapidly reversed AKI), and 'No AKI'.

    1. Read aki episodes and summary file.
    2. Generate indicators of 'aki_recovery' and 'paki'. A patient has aki_recovery if patient has aki and recover by discharge. A patient has paki if patient has at least one episode lasting more than 2 days.
    3. Determine discharge place.
    4. Determine trajectory groups 'overall_type' by combining 'paki', 'aki_recovery' and 'dischg_place'.
        * A patient has 'paki without renal recovery' if 1) patient has paki but no aki_recovery 2) patient has aki, no paki, no aki_recovery, but dishg_place is not home/rehab, we still consider patient has persistent aki without renal recovery.
        * A patient has 'paki with renal recovery' if patient has paki and aki_recovery.
        * Other AKI patients without trajectory groups are considered as 'non-pAKI with recovery'
    5. Determine akd indicator if patient has aki last longer than 7 days.
    6. Save the trajectory file with newly generated columns including 'overall_type', 'aki_recovery', 'paki', 'akd_greater_than_7_days','aki_recovery_undetermined'.
    7. Save the encounter file with dischg place and mortality.

    Parameters
    ----------
        inmd_dir:
            intermediate directory location
        in_dir: str
            input directory location
        batch: int
            batch number, default value = 0
        eid: str
            encounter id column name, default value = 'merged_enc_id'
        pid: str
            patient id column name, default value = 'patient_deiden_id'
        in_batch: bool
            indicator for batched data, default value = True
        ssid_name: str
            file name containing columns 'pid' and 'ssdi_death_date', default value = np.nan

    Returns
    -------
    None
    '''
    aki_episodes = load_data(os.path.join(inmd_dir, 'encounter_aki', 'encounter_aki_episodes_{}.csv'.format(batch)),
                             pid=pid, eid=eid)
    aki_summary = load_data(os.path.join(inmd_dir, 'encounter_aki', 'encounter_aki_summary_{}.csv'.format(batch)),
                            pid=pid, eid=eid)

    aki_trajectory = aki_summary[[eid, 'aki_overall', 'discharge_aki_status']]
    aki_trajectory['aki_recovery'] = np.nan
    aki_trajectory.loc[(aki_trajectory['aki_overall'] == 1) & (aki_trajectory['discharge_aki_status'] == 0), 'aki_recovery'] = 1
    aki_trajectory.loc[(aki_trajectory['aki_overall'] == 1) & (aki_trajectory['discharge_aki_status'] == 1), 'aki_recovery'] = 0

    aki_trajectory['paki'] = np.nan
    paki_episodes = aki_episodes[aki_episodes['episode_days'] > 2]
    aki_trajectory.loc[aki_trajectory[eid].isin(paki_episodes[eid]), 'paki'] = 1
    aki_trajectory.loc[(aki_trajectory['aki_overall'] == 1) & (aki_trajectory['paki'].isnull()), 'paki'] = 0

    encounter = load_data(os.path.join(inmd_dir, 'filtered_encounters', 'filtered_encounters_all_{}.csv'.format(batch)),
                          usecols=[pid, eid, 'admit_datetime', 'dischg_datetime', 'death_date', 'dischg_disposition'],
                          pid=pid, eid=eid)

    # read ssdi death
    if 'ssdi_death' not in encounter.columns.tolist() and ssdi_name and len(ssdi_name) > 0:
        ssdi_death = load_data(directory=in_dir, file_path_query=ssdi_name,
                               regex=True,
                               patterns=[r'_' + str(batch) + r'_[0-9]+\.csv',
                                         r'_' + str(batch) + r'\.csv',
                                         r'\.csv'],
                               usecols=[pid, 'ssdi_death_date'],
                               pid=pid)
        if not isinstance(ssdi_death, pd.DataFrame):
            ssdi_death = pd.DataFrame(columns=[pid, 'ssdi_death_date'])
        ssdi_death[pid] = ssdi_death[pid]
        encounter = encounter.merge(ssdi_death, on=pid, how='left')
    encounter = p08_cal_mortality(encounter, pid)
    encounter = p08_get_dischg_position(encounter)

    aki_trajectory = aki_trajectory.merge(encounter[[eid, 'dischg_place']], on=eid, how='left')
    aki_trajectory['overall_type'] = np.nan
    aki_trajectory.loc[aki_trajectory['aki_overall'] == 0, 'overall_type'] = 'No AKI'
    aki_trajectory.loc[(aki_trajectory['paki'] == 1) & (aki_trajectory['aki_recovery'] == 1), 'overall_type'] = 'pAKI with recovery'
    aki_trajectory.loc[(aki_trajectory['paki'] == 1) & (aki_trajectory['aki_recovery'] == 0), 'overall_type'] = 'pAKI without recovery'
    aki_trajectory.loc[(aki_trajectory['paki'] == 0) & (aki_trajectory['aki_recovery'] == 1), 'overall_type'] = 'non-pAKI with recovery'
    aki_trajectory.loc[(aki_trajectory['paki'] == 0) & (aki_trajectory['aki_recovery'] == 0) & (aki_trajectory['aki_overall'] == 1) & (aki_trajectory['dischg_place'] == 'Home/Rehab'), 'overall_type'] = 'non-pAKI with recovery'
    aki_trajectory.loc[aki_trajectory['overall_type'].isnull(), 'overall_type'] = 'pAKI without recovery'

    aki_trajectory['akd_greater_than_7_days'] = 0
    akd_episodes = aki_episodes[aki_episodes['episode_days'] > 7]
    aki_trajectory.loc[aki_trajectory[eid].isin(akd_episodes[eid]), 'akd_greater_than_7_days'] = 1

    aki_trajectory['aki_recovery_undetermined'] = aki_trajectory['aki_recovery']
    aki_trajectory.loc[(aki_trajectory['paki'] == 0) & (aki_trajectory['aki_recovery'] == 0) & (aki_trajectory['aki_overall'] == 1), 'aki_recovery_undetermined'] = 'undetermined'

    if not os.path.exists(os.path.join(inmd_dir, 'aki_trajectory')):
        os.makedirs(os.path.join(inmd_dir, 'aki_trajectory'))

    aki_trajectory = aki_trajectory.drop(columns=['discharge_aki_status'])
    aki_trajectory.to_csv(os.path.join(inmd_dir, 'aki_trajectory', 'aki_trajectory_{}.csv'.format(batch)), index=False)
    encounter[[eid, 'death_date_combined', 'hospital_mortality', 'mort_status_30d', 'mort_status_90d', 'mort_status_6m',
               'mort_status_1y', 'mort_status_2y', 'mort_status_3y', 'dischg_place']]\
        .to_csv(os.path.join(inmd_dir, 'aki_trajectory', 'encounter_dischg_place_{}.csv'.format(batch)), index=False)
