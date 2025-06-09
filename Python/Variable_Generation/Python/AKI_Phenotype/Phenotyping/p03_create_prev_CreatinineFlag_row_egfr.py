# -*- coding: utf-8 -*-
"""
Created on Tue Nov 24 00:18:54 2020

@author: renyuanfang
"""
import pandas as pd
import os
from datetime import timedelta
import numpy as np
from Phenotyping.utils import eGFR_fun
from Utils.file_operations import load_data


def p03_add_prev_creatinine_and_egfr_flags(inmd_dir: str, eid: str, pid: str, batch: int, race_correction: bool):
    '''
    This function add previousCreatinine Flag as indicator for patient previous creatinine taken and generate patient eGFR parameters.

    1. Inner join encounter and creatinine file on pid
    2. Add PreviousCreatinineFlag by filtering inferred_specimen_datetime is between 365 days and 1 day before admission and esrd_admin_flag != 1. Label rows satisfied the condition as 1 else 0.
    3. Calculate patient age and apply eGFR_fun function to calculate eGFR value.
    4. Add uncertain_ckd flag column based on the following condition: If finalICD910 = 1 and esrd_admin_flag, kidneyTransplant_admin_flag, and ckd_admin_flag = 0 or If finalICD910 = 0 and PreviousCreatinineFlag = 1, label uncertain_ckd column as 1. Else label as 0.
    5. Add 3 main areas of creatinine value
        * Locate encounter ids that do not have two eGFR at least 90 days apart as insufficient information and locate encounter ids that finalICD910 = 0. Label those rows for insufficient_data_flag = 1.
        * Locate encounters have two eGFR <= 60, and 90 days apart, not within 30 days before admission. Label those rows for egfr_90d_apart_p30d = 1. Also record egfr_90d_apart_p30d_date as specimen_date, max_specimen_date
        * Locate recent egfr within 30 days before admission, Label those rows for egfr_30d = 1. Also record egfr_30d_date as maximum of specimen_date.
    6. Save final files to intermediate directory in csv format. New columns introduced:
        * PreviousCreatinineFlag: flag column to indicate 1 as previous creatinine present and 0 as not
        * sample_age: patient age using inferred_specimen_datetime - birth_date
        * row_egfr: calculated eGFR value
        * uncertain_ckd: flag column to indicate 1 as uncertain CKD and 0 as not
        * insufficient_data_flag: flag column to indicate 1 as insufficient data (encounter ids that do not have two eGFR at least 90 days apart) and 0 as not
        * egfr_90d_apart_p30d: flag column to indicate 1 as encounters have two eGFR <= 60, and 90 days apart, not within 30 days before admission and 0 as not
        * egfr_90d_apart_p30d_date: specimen_date, max_specimen_date for encounters satisfy the requirement
        * egfr_30d: flag column to indicate 1 as recent egfr within 30 days before admission and 0 as not
        * egfr_30d_date: specimen_date for encounters satisfy the requirement

    Parameters
    ----------
        inmd_dir: str
            intermediate file directory
        batch: int
            batch number, default value = 0
        race_correction: bool
            flag for using race agnostic for eGFR calculation, default value = True
        eid: str
            encouter id column name
        pid: str
            patient id column name

    Returns
    -------
    None
    '''
    encounter = load_data(os.path.join(inmd_dir, 'encounter_admin_flags', 'encounter_admin_flags_{}.csv'.format(batch)),
                          parse_dates=['admit_datetime', 'birth_date'],
                          pid=pid, eid=eid, preserve_case=True)

    creatinine = load_data(os.path.join(inmd_dir, 'filtered_labs', 'filtered_labs_{}.csv'.format(batch)),
                           parse_dates=['inferred_specimen_datetime'],
                           pid=pid, eid=eid).drop(columns=[eid])

    creatinine['lab_result'] = creatinine['lab_result'].astype(float)

    creatinine = creatinine.merge(encounter, on=[pid], how='inner')
    con1 = creatinine['inferred_specimen_datetime'] <= creatinine['admit_datetime'] + timedelta(days=1)
    previous_creatinine = creatinine[con1]

    con2 = creatinine['inferred_specimen_datetime'] >= creatinine['admit_datetime'] - timedelta(days=365)
    one_year_prior_creatinine = creatinine[con1 & con2]

    # add previousCreatinine Flag
    encounter['PreviousCreatinineFlag'] = 0
    encounter['esrd_admin_flag'] = pd.to_numeric(encounter['esrd_admin_flag'], errors='coerce')
    con = (encounter[eid].isin(one_year_prior_creatinine[eid])) & (encounter['esrd_admin_flag'] != 1)
    encounter.loc[con, 'PreviousCreatinineFlag'] = 1

    # calculate egfr
    previous_creatinine.loc[:, 'sample_age'] = (previous_creatinine['inferred_specimen_datetime'].dt.date - previous_creatinine['birth_date'].dt.date).copy() / timedelta(days=365.2425)
    previous_creatinine.loc[:, 'row_egfr'] = previous_creatinine[['sample_age', 'sex', 'race', 'lab_result']]\
        .apply(lambda x: eGFR_fun(x[0], x[1], x[2], x[3], race_correction), axis=1).copy()

    # add uncertain ckd flag
    encounter['uncertain_ckd'] = 0
    con1 = (encounter['finalICD910'] == 1) & (encounter['esrd_admin_flag'] == 0) & (encounter['kidneyTransplant_admin_flag'] == 0) & (encounter['ckd_admin_flag'] == 0)
    con2 = (encounter['finalICD910'] == 0) & (encounter['PreviousCreatinineFlag'] == 1)
    encounter.loc[con1 | con2, 'uncertain_ckd'] = 1

    ####add two egfr information
    previous_creatinine['admit_date'] = previous_creatinine['admit_datetime'].dt.date
    previous_creatinine['specimen_date'] = previous_creatinine['inferred_specimen_datetime'].dt.date
    data = previous_creatinine[previous_creatinine['admit_date'] > previous_creatinine['specimen_date']]

    ####1. find those do not have insufficient information, that is do not have two egfr at least 90 days apart
    creatinine_max_date = data.groupby([eid], as_index=False)['specimen_date'].max()
    creatinine_max_date = creatinine_max_date.rename(columns = {'specimen_date' : 'max_specimen_date'})
    sufficient_creatinine = data.merge(creatinine_max_date, on = eid, how = 'left')
    con = sufficient_creatinine['max_specimen_date'] >= (sufficient_creatinine['specimen_date'] + timedelta(days=90))
    sufficient_creatinine = sufficient_creatinine[con]
    sufficient_creatinine = sufficient_creatinine.sort_values([eid, 'specimen_date'])
    sufficient_creatinine = sufficient_creatinine.drop_duplicates([eid], keep='last').reset_index(drop = True)
    encounter['insufficient_data_flag'] = np.nan
    con = (encounter[eid].isin(sufficient_creatinine[eid])) & (encounter['finalICD910'] == 0)
    encounter.loc[con, 'insufficient_data_flag'] = 0
    con = (encounter['finalICD910'] == 0) & (encounter['insufficient_data_flag'].isnull())
    encounter.loc[con, 'insufficient_data_flag'] = 1

    ###find those encounters have two egfr <= 60, and 90 days apart, not within 30 days before admission
    data_egfr = data[data['row_egfr'] <= 60]
    con = data_egfr['admit_date'] <= data_egfr['specimen_date'] + timedelta(days=30)
    data_30d = data_egfr[con]
    data_plus30d = data_egfr[~con]

    creatinine_max_date = data_plus30d.groupby([eid], as_index=False)['specimen_date'].max()
    creatinine_max_date = creatinine_max_date.rename(columns = {'specimen_date' : 'max_specimen_date'})
    two_egfr = data_plus30d.merge(creatinine_max_date, on = eid, how = 'left')
    con = two_egfr['max_specimen_date'] >= (two_egfr['specimen_date'] + timedelta(days=90))
    two_egfr = two_egfr[con]
    two_egfr = two_egfr.sort_values([eid, 'specimen_date'])
    two_egfr = two_egfr.drop_duplicates([eid], keep='last').reset_index(drop = True)
    encounter['egfr_90d_apart_p30d'] = np.nan
    con = (encounter['insufficient_data_flag'] == 0) | ((encounter['insufficient_data_flag'] != 1) & (encounter['esrd_admin_flag'] == 0) & (encounter['ckd_admin_flag'] == 0) & (encounter['kidneyTransplant_admin_flag'] == 0))
    con1 = encounter[eid].isin(two_egfr[eid])
    encounter.loc[con & con1, 'egfr_90d_apart_p30d'] = 1
    two_egfr['egfr_90d_apart_p30d_date'] = np.nan
    cond = (two_egfr['specimen_date'].notnull()) & (two_egfr['max_specimen_date'].notnull())
    two_egfr.loc[cond, 'egfr_90d_apart_p30d_date'] = two_egfr.loc[cond, ['specimen_date', 'max_specimen_date']].apply(lambda x: str(x[0]) + ', ' + str(x[1]), axis = 1)
    encounter = encounter.merge(two_egfr[[eid, 'egfr_90d_apart_p30d_date']], on = eid, how='left')
    encounter.loc[encounter['egfr_90d_apart_p30d'].isnull(), 'egfr_90d_apart_p30d_date'] = np.nan
    encounter.loc[con & (encounter['egfr_90d_apart_p30d'].isnull()), 'egfr_90d_apart_p30d'] = 0

    ###find those recent egfr within 30 days before admission
    creatinine_max_date = data_30d.groupby([eid], as_index=False)['specimen_date'].max()
    creatinine_max_date = creatinine_max_date.rename(columns = {'specimen_date' : 'egfr_30d_date'})
    encounter['egfr_30d'] = np.nan
    encounter = encounter.merge(creatinine_max_date[[eid, 'egfr_30d_date']], on = eid, how = 'left')
    encounter.loc[(encounter['egfr_90d_apart_p30d'] == 1) & (encounter['egfr_30d_date'].isnull()), 'egfr_30d'] = 0
    encounter.loc[(encounter['egfr_90d_apart_p30d'] == 1) & (encounter['egfr_30d_date'].notnull()), 'egfr_30d'] = 1
    encounter.loc[encounter['egfr_30d'] != 1, 'egfr_30d_date'] = np.nan

    if not os.path.exists(os.path.join(inmd_dir, 'encounter_egfr_flags')):
        os.makedirs(os.path.join(inmd_dir, 'encounter_egfr_flags'))

    encounter.to_csv(os.path.join(inmd_dir, 'encounter_egfr_flags', 'encounter_egfr_flags_{}.csv'.format(batch)), index = False)

    if not os.path.exists(os.path.join(inmd_dir, 'ckd_row_egfr')):
        os.makedirs(os.path.join(inmd_dir, 'ckd_row_egfr'))

    previous_creatinine.to_csv(os.path.join(inmd_dir, 'ckd_row_egfr', 'ckd_row_egfr_{}.csv'.format(batch)), index = False)


if __name__ == '__main__':
    # p03_add_prev_creatinine_and_egfr_flags(inmd_dir=inmd_dir, batch=batch, race_correction=race_correction, eid=eid, pid=pid)
    # inmd_dir= dir_dict.get('staging_dir')
    # batch=0

    pass
