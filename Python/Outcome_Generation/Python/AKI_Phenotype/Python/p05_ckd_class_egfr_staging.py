# -*- coding: utf-8 -*-
"""
Created on Tue Nov 24 14:49:32 2020

@author: renyuanfang
"""
import pandas as pd
import os
from datetime import timedelta
import numpy as np
from .utils import mdrd_fun, eGFR_fun
from .Utilities.FileHandling.io import check_load_df, save_data


CKD_dict = {'ESRD': 2, 'ESRD with Warning': 2,
            'AKD on Admission, CKD by Creatinine Criteria': 1, 'AKD on Admission, CKD by Medical History': 1,
            'CKD by Creatinine Criteria': 1, 'CKD by Medical History': 1, 'Recovered AKI on Admission, CKD by Medical History': 1,
            'CKD after kidney transplant by Medical History': 1, 'Recovered AKI on Admission, CKD by Creatinine Criteria': 1,
            'AKD on Admission, CKD after kidney transplant by Medical History': 1,
            'Recovered AKI on Admission, CKD after kidney transplant by Medical History': 1,
            'AKD on Admission, No CKD by Medical History': 0, 'AKD on Admission, No CKD by Medical History Or Creatinine Criteria': 0,
            'Insufficient Data': 0, 'No CKD by Medical History Or Creatinine Criteria': 0,
            'Recovered AKI on Admission, No CKD by Medical History': 0,
            'Recovered AKI on Admission, No CKD by Medical History Or Creatinine Criteria': 0}


def p05_find_subgroup(ckd_str: str, aki_flg, aki_date, adm_date, adm_cr, mdrd, warning: bool = False):
    """
    This function is a subfunction of p05_find_final_class which determines detailed ckd class.

    1. If warning is true and if admission_creatinine > 0 or if admission_creatinine / MDRD >= 1.5, return value as 'ESRD'. Else return as 'ESRD with Warning'.
    2. If aki_admin_flag is 1 and the difference between admission date and AKI date <= 90 days and if admission_creatinine > 0 or if admission_creatinine / MDRD >= 1.5, return value as 'AKD on Admission, ' + description of CKD. Else return as 'Recovered AKI on Admission, ' + description of CKD

    Parameters
    ----------
        ckd_str: str
            detailed ckd class, indicating if result is determined by medical history or creatinine criteria
        aki_flg: int
            indicator for aki by medical history
        aki_date: date
            most recent date of aki by medical history
        adm_date: date
            admission date
        adm_cr: float
            admission creatinine
        mdrd: float
            mdrd value
        warning: bool
            False

    Returns
    -------
    str
        detailed ckd class

    Notes
    -----
    """
    if warning:
        if (not (adm_cr > 0)) or float(adm_cr) / mdrd >= 1.5:
            return 'ESRD'
        else:
            return 'ESRD with Warning'
    if aki_flg == 1 and adm_date - pd.to_datetime(aki_date).date() <= timedelta(days=90):
        if (not (adm_cr > 0)) or float(adm_cr) / mdrd >= 1.5:
            return 'AKD on Admission, ' + ckd_str
        else:
            return 'Recovered AKI on Admission, ' + ckd_str

    return ckd_str


# This function finds the final class of CKD
def p05_find_final_class(row):
    """
    This function is a subfunction of p05_find_final_class which determines detailed ckd class.

    Parameters
    ----------
        row: Series
            A row of dataframe, must contain following rows:
                * final_code_flag
                * esrd_admin_flag
                * kidney_transplant_admin_flag
                * aki_admin_flag
                * ckd_admin_flag
                * kidney_date
                * esrd_date
                * admission_creatinine
                * mdrd
                * insufficient_data_flag
                * egfr_90d_apart_p30d

    Returns
    -------
    str
        detailed ckd class
    Notes
    -----
    MAY NEED FLOWCHART
    """
    if row['final_code_flag'] == 1:
        if row['esrd_admin_flag'] == 1:
            if row['kidney_transplant_admin_flag'] == 1 and row['kidney_date'] >= row['esrd_date']:
                return p05_find_subgroup('CKD after kidney transplant by Medical History', row['aki_admin_flag'], row['aki_date'], row['admit_date'], row['admission_creatinine'], row['mdrd'])
            else:
                return p05_find_subgroup('No CKD by Medical History', row['aki_admin_flag'], row['aki_date'], row['admit_date'], row['admission_creatinine'], row['mdrd'], warning=True)
        if row['kidney_transplant_admin_flag'] == 1:
            return p05_find_subgroup('CKD after kidney transplant by Medical History', row['aki_admin_flag'], row['aki_date'], row['admit_date'], row['admission_creatinine'], row['mdrd'])
        if row['ckd_admin_flag'] == 1:
            return p05_find_subgroup('CKD by Medical History', row['aki_admin_flag'], row['aki_date'], row['admit_date'], row['admission_creatinine'], row['mdrd'])
    if row['final_code_flag'] == 0 and row['insufficient_data_flag'] == 1:
        return 'Insufficient Data'
    if row["egfr_90d_apart_p30d"] == 0:
        return p05_find_subgroup('No CKD by Medical History Or Creatinine Criteria', row['aki_admin_flag'], row['aki_date'], row['admit_date'], row['admission_creatinine'], row['mdrd'])
    return p05_find_subgroup('CKD by Creatinine Criteria', row['aki_admin_flag'], row['aki_date'], row['admit_date'], row['admission_creatinine'], row['mdrd'])


def p05_find_ref_method(row, methods):
    '''
    This function is to find the method determining reference creatinine. We assign reference creatinine as minimum of [admission creatinine, minimum creatinine within 
    1-7 days before admission, medium creatinine within 8-365 days before admission, and mdrd (if do not have ckd)]. 
        
    Parameters
    ----------
        row: tuple
            A row of dataframe. This dataframe must contain following columns:
                * reference_creatinine
                * admission_creatinine
                * min_7_days
                * medium_8_365_days
                * mdrd
    
    Returns
    -------
    str
        method to be selected as reference creatinine
    
    Notes
    -----
    '''
    if pd.isna(row.reference_creatinine):
        return np.nan
    
    for method in methods:
        if row[method] == row.reference_creatinine:
            return method
    
    return np.nan


def p05_get_egfr_stage(egfr_value):
    """
    This function is to determine the G-stage of CKD class (['G1', 'G2', 'G3a', 'G3b', 'G4', 'G5', 'No staging can be done!'])

    1. If eGFR value is null, then return No staging can be done!
    2. If eGFR value is bigger and equal to than 90, return G1
    3. If eGFR value is between 60 and 90, return G2
    4. If eGFR value is between 45 and 60, return G3a
    5. If eGFR value is between 30 and 45, return G3b
    6. If eGFR value is between 15 and 30, return G4
    7. If eGFR value is smaller and equal to 15, return G5
    8. If none of the condition above satisfy, return null value

    Parameters
    ----------
        egfr_value: float
            egfr value

    Returns
    -------
    str
        G-stage of CKD

    Notes
    -----
    """
    if pd.isna(egfr_value):
        return 'No staging can be done!'
    if egfr_value >= 90:
        return 'G1'
    elif egfr_value >= 60:
        return 'G2'
    elif egfr_value >= 45:
        return 'G3a'
    elif egfr_value >= 30:
        return 'G3b'
    elif egfr_value >= 15:
        return 'G4'
    elif egfr_value < 15:
        return 'G5'
    else:
        return np.nan


def p05_get_CKD_class(detailed_ckd_class):
    """
    This function is used to generate overall ckd class (['0', '1', 'ESRD', 'Insufficient Data']). A ckd class with '1'/'0' indicates patients have/do not have
    CKD determined by medical history and creatinine. A ckd class with 'Insufficient Data' indicates patients do not have enough information to determine CKD.

    Parameters
    ----------
        detailed_ckd_class: str
            detailed ckd class belongs to one of following
             'ESRD','ESRD with Warning',
             'AKD on Admission, CKD by Creatinine Criteria','AKD on Admission, CKD by Medical History',
             'CKD by Creatinine Criteria','CKD by Medical History','Recovered AKI on Admission, CKD by Medical History',
             'CKD after kidney transplant by Medical History','Recovered AKI on Admission, CKD by Creatinine Criteria',
             'AKD on Admission, CKD after kidney transplant by Medical History',
             'Recovered AKI on Admission, CKD after kidney transplant by Medical History',
             'AKD on Admission, No CKD by Medical History','AKD on Admission, No CKD by Medical History Or Creatinine Criteria',
             'Insufficient Data','No CKD by Medical History Or Creatinine Criteria',
             'Recovered AKI on Admission, No CKD by Medical History',
             'Recovered AKI on Admission, No CKD by Medical History Or Creatinine Criteria'

    Returns
    -------
    str
        overall ckd class

    Notes
    -----
    """
    if 'No CKD' in detailed_ckd_class:
        return '0'
    elif 'CKD' in detailed_ckd_class:
        return '1'
    elif 'ESRD' in detailed_ckd_class:
        return 'ESRD'
    else:
        return 'Insufficient Data'


def p05_ckd_class_and_egfr_staging(inmd_dir: str, race_correction: bool, version: int, pid: str, eid: str, batch: int, **logging_kwargs):
    """
    Determine CKD classes and G-stage of CKD if having CKD.

    Action items:
        1. Read saved encounter file containing creatinine parameters
        2. Calculate mdrd value for each encounter
        3. Determine the most recent date of kidney transplant, esrd and aki before admission using recorded diagnosis and procedure codes
        4. Determine detailed ckd class using admission creatinine, mdrd, diagnosis code history etc.
        5. Determine reference creatinine using minimum of [admission creatinine, minimum creatinine within 1-7 days before admission, medium creatinine within 8-365 days before admission, and mdrd (if do not have ckd)]
        6. Calculate egfr for each encounter using reference creatinine
        7. Determine the G-stage of CKD for those patients with CKD
        8. Save resulting encounter file with newly generated columns. New columns include:
            * final_class
            * egfr
            * egfr_staging
            * reference_creatinine
            * method
            * ckd

    Parameters
    ----------
        inmd_dir: str
            intermediate file directory
        batch: int
            batch number, default value = 0
        race_correction: bool
            flag for using race agnostic for eGFR and mdrd calculation, default value = True

    Returns
    -------
    None

    Notes
    -----
    Can add flowchart for how do we determine determined CKD class, and G-stage
    """
    encounter = check_load_df(os.path.join(inmd_dir, 'encounter_creatinine_parameters', 'encounter_creatinine_parameters_{}.csv'.format(batch)),
                              desired_types={x: 'datetime' for x in ['admit_datetime', 'birth_date',
                                                                     'kidney_transplant_condition_code_date', 'kidney_transplant_procedure_code_date',
                                                                     'esrd_condition_code_date', 'esrd_procedure_code_date', 'aki_condition_code_date', 'aki_procedure_code_date']},
                              pid=pid, eid=eid, dtype=None, **logging_kwargs)
    
    encounter['admit_date'] = encounter['admit_datetime'].dt.date
    encounter['birth_date'] = encounter['birth_date'].dt.date
    encounter['age'] = (encounter['admit_date'] - encounter['birth_date']) / timedelta(days=365.2425)

    encounter['insufficient_data_flag'] = encounter['insufficient_data_flag'].fillna(0)
    encounter['mdrd'] = encounter[['age', 'sex', 'race']].apply(lambda x: mdrd_fun(x[0], x[1], x[2], race_correction, version), axis=1) if encounter.shape[0] > 0 else None

    for x in ['kidney_transplant_condition_code_date', 'kidney_transplant_procedure_code_date', 'esrd_condition_code_date', 'esrd_procedure_code_date', 'aki_condition_code_date', 'aki_procedure_code_date']:
        encounter[x] = encounter[x].dt.date
    encounter['kidney_date'] = encounter[['kidney_transplant_condition_code_date', 'kidney_transplant_procedure_code_date']].apply(lambda x: max(x), axis=1) if encounter.shape[0] > 0 else None
    encounter['esrd_date'] = encounter[['esrd_condition_code_date', 'esrd_procedure_code_date']].apply(lambda x: max(x), axis=1) if encounter.shape[0] > 0 else None
    encounter['aki_date'] = encounter[['aki_condition_code_date', 'aki_procedure_code_date']].apply(lambda x: max(x), axis=1) if encounter.shape[0] > 0 else None

    encounter['final_class'] = encounter.apply(p05_find_final_class, axis=1) if encounter.shape[0] > 0 else None
    encounter['egfr_staging'] = np.nan
    encounter['final_class_num'] = encounter['final_class'].apply(lambda x: CKD_dict[x])

    encounter['reference_creatinine'] = np.nan
    con = encounter['final_class_num'] == 0
    encounter.loc[con, 'reference_creatinine'] = encounter.loc[con, ['admission_creatinine', 'min_7_days', 'medium_8_365_days', 'mdrd']].apply(lambda x: np.min(x), axis=1) if encounter.shape[0] > 0 else None
    encounter.loc[~con, 'reference_creatinine'] = encounter.loc[~con, ['admission_creatinine', 'min_7_days', 'medium_8_365_days']].apply(lambda x: np.min(x), axis=1) if encounter.shape[0] > 0 else None
    encounter['method'] = encounter.apply(p05_find_ref_method, methods=['admission_creatinine', 'min_7_days', 'first_creatinine', 'medium_8_365_days', 'mdrd'], axis=1) if encounter.shape[0] > 0 else None

    encounter['egfr'] = np.nan
    con = (encounter['method'].notnull()) & (encounter['method'] != 'mdrd')
    encounter.loc[con, 'egfr'] = encounter.loc[con, ['age', 'sex', 'race', 'reference_creatinine']].apply(lambda x: eGFR_fun(x[0], x[1], x[2], x[3], race_correction, version), axis=1) if encounter.shape[0] > 0 else None
    encounter['egfr_staging'] = np.nan
    con = encounter['final_class_num'] == 1
    encounter.loc[con, 'egfr_staging'] = encounter.loc[con, 'egfr'].apply(lambda x: p05_get_egfr_stage(x)) if encounter.shape[0] > 0 else None
    encounter['ckd'] = encounter['final_class'].apply(p05_get_CKD_class) if encounter.shape[0] > 0 else None

    encounter = encounter.drop(columns=['admit_date', 'kidney_date', 'esrd_date', 'aki_date', 'final_class_num'])
    encounter_no_esrd = encounter[encounter['ckd'] != 'ESRD']

    save_data(df=encounter,
              out_path=os.path.join(inmd_dir, 'encounter_ckd', 'encounter_ckd_{}.csv'.format(batch)),
              index=False, **logging_kwargs)
    save_data(df=encounter_no_esrd,
              out_path=os.path.join(inmd_dir, 'encounter_ckd', 'encounter_ckd_noesrd_{}.csv'.format(batch)),
              index=False, **logging_kwargs)
