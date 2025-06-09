# -*- coding: utf-8 -*-
import pandas as pd
from .AKI_Phenotype.Python.utils import eGFR_fun
# from Utils.log_messages import log_print_email_message as logm


def CKD_classification(CKD):
    '''
    Function belongs to:-CKD_result.py
    Module to reclassify CKD results for IDEALIST

    Parameters
    ----------
        CKD : String

    Returns
    -------
    String
        New CKD classification

    Notes
    -----
    Flow chart for this function:
        .. image:: flow_charts/statics/CKD_result/CKD_classification.png
    '''
    if pd.isnull(CKD):
        return
    elif "NO CKD" in CKD.upper():
        return "No CKD"
    elif "CKD STATUS NEEDS CLARIFICATION" in CKD.upper():
        return "No CKD"
    elif "ESRD" in CKD.upper():
        return "ESRD"
    elif "INSUFFICIENT" in CKD.upper():
        return "No CKD"
    elif "CKD" in CKD.upper():
        return "CKD"
    else:
        return "No CKD"


def generate_cdk_results(df: pd.DataFrame, ckd_df: pd.DataFrame, pid: str, time_index_col: str, reference_dt_col: str, unique_index_col: str, **logging_kwargs):
    """
    Convert CKD outcome to standard for varialbe generation.

    Belongs to: -CKD_result.py

    Parameters
    ----------
    ckd_df : pd.DataFrame
        Input dataframe must contain following columns:
                * eid
                * Final Class
                * eGFR
                * reference_creatinine
    eid : str
        encounter id column.

    Returns
    -------
    log : str
        event log.
    ckd_df : TYPE
        dataframe with updated CKD information.

    Notes
    -----
    Flow chart for this function:
        .. image:: flow_charts/statics/CKD_result/generate_cdk_results.png

    """
    if 'egfr' not in ckd_df.columns:
        # this is designed to work with final_aki file from aki phenotyping
        ckd_df['egfr'] = ckd_df.apply(lambda row: eGFR_fun(age=float(row.age), sex=row.sex, race=row.race, row_creatinine=float(row.lab_result), race_correction=False, version=2), axis=1)
        
    df = pd.merge_asof(left=df.sort_values(reference_dt_col, ascending=True),
                         right=ckd_df[[pid, time_index_col, "final_class", "egfr", "reference_creatinine"]].sort_values(time_index_col, ascending=True),
                         left_on=reference_dt_col,
                         right_on=time_index_col,
                         by='person_id',
                         allow_exact_matches=False,
                         direction='backward')


    df["CKD"] = df["final_class"].apply(CKD_classification)
    df["eGFR"] = df["egfr"].fillna("not available")

    return df.drop(columns=["final_class", "egfr", time_index_col])\
        .rename(columns={"reference_creatinine": "reference creatinine"})
