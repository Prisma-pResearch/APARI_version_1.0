# -*- coding: utf-8 -*-
"""
Created on Mon Sep 28 09:46:49 2020.

@author: ruppert20
"""
import pandas as pd
import os
from ..Utilities.FileHandling.io import check_load_df
from ..AKI_Phenotype.Python.main import main_run_AKI_CKD_Phenotyping


def aki_outcome(source_df: pd.DataFrame,
                aki_final_fp: str,
                aki_trajectory_fp: str,
                aki_summary_fp: str,
                eid: str,
                pid: str,
                batch_id: str,
                dir_dict: dict,
                project_name: str,
                visit_detail_end_col: str,
                visit_start_col: str,
                visit_detail_type: str,
                time_intervals: list = ['3D', '7D'],
                directory: str = None,
                patterns: list = None,
                **logging_kwargs) -> pd.DataFrame:
    """
    Generate AKI Outcomes.

    Parameters
    ----------
    source_df : pd.DataFrame
        DESCRIPTION.
    aki_final_fp : str
        DESCRIPTION.
    aki_trajectory_fp : str
        DESCRIPTION.
    aki_summary_fp : str
        DESCRIPTION.
    eid : str
        DESCRIPTION.
    visit_detail_end_col : str
        DESCRIPTION.
    visit_detail_type : str
        DESCRIPTION.
    time_intervals : list, optional
        DESCRIPTION. The default is ['3D', '7D'].
    directory : str, optional
        DESCRIPTION. The default is None.
    patterns : list, optional
        DESCRIPTION. The default is None.
    **logging_kwargs : TYPE
        DESCRIPTION.

    Returns
    -------
    TYPE
        DESCRIPTION.

    """
    interval_types: list = [f'aki_{visit_detail_type}', 'aki_admit']

    race_correction: str = 'with_race_correction' if 'with_race_correction' in aki_final_fp else 'without_race_correction'

    # load aki_final
    aki_final_df = check_load_df(input_v=aki_final_fp,
                                 usecols=[eid, 'inferred_specimen_datetime', 'specimen_date', 'aki_flag', 'aki_stage'],
                                 parse_dates=['inferred_specimen_datetime', 'specimen_date'],
                                 directory=directory,
                                 patterns=patterns,
                                 desired_types={'visit_occurrence_id': 'sparse_int'},
                                 ds_type='pandas', **logging_kwargs)\
        .rename(columns={'inferred_specimen_datetime': 'specimen_taken_date_time',
                         'specimen_date': 'specimen_taken_date',
                         'aki_stage': f'aki_{race_correction}_staging'})\
        .drop_duplicates()

    if not isinstance(aki_final_df, pd.DataFrame):
        # run aki phenotype
        main_run_AKI_CKD_Phenotyping(dir_dict=dir_dict,
                                     race_corrections=[False],
                                     eids=[eid],
                                     pid=pid,
                                     independent_sub_batch=True,
                                     success_fp=os.path.join(dir_dict.get('status_files'), f'AKI_Phenotype_success_{batch_id}_' if isinstance(batch_id, str) else 'AKI_Phenotype_success_'),
                                     version=2,
                                     project_name=project_name,
                                     regex=True,
                                     batches=[batch_id] if isinstance(batch_id, str) else None,
                                     serial=True,
                                     max_workers=1)

    # load aki_trajectory
    aki_trajectory_df = check_load_df(input_v=aki_trajectory_fp,
                                      usecols=[eid, 'overall_type', 'aki_recovery', 'paki'],
                                      ds_type='pandas',
                                      directory=directory,
                                      desired_types={'visit_occurrence_id': 'sparse_int'},
                                      patterns=patterns, **logging_kwargs)\
        .rename(columns={'overall_type': f'overall_aki_{race_correction}_type',
                         'aki_recovery': f'aki_{race_correction}_recovery',
                         'paki': f'paki_{race_correction}'})\
        .drop_duplicates()

    aki_summary_df = check_load_df(input_v=aki_summary_fp,
                                   usecols=[eid, 'worst_aki_staging', 'discharge_aki_status'],
                                   ds_type='pandas',
                                   directory=directory,
                                   desired_types={'visit_occurrence_id': 'sparse_int'},
                                   patterns=patterns, **logging_kwargs)\
        .rename(columns={'worst_aki_staging': f'max_aki_{race_correction}_stage',
                         'discharge_aki_status': f'discharge_aki_{race_correction}_status'})

    # fill information one row at a time
    for i, row in source_df.copy().iterrows():

        temp_final_aki = aki_final_df[aki_final_df[eid] == row[eid]]

        for interval_type in interval_types:
            for interval in time_intervals:
                fill_aki_outcome(source_df=source_df,
                                 i=i,
                                 row=row,
                                 race_correction=race_correction,
                                 temp_final_aki=temp_final_aki,
                                 interval_label=interval_type,
                                 inteval_duration=interval,
                                 interval_reference_col=visit_detail_end_col if interval_type == f'aki_{visit_detail_type}' else visit_start_col)

        source_df.loc[i, f'aki_adm_disch_{race_correction}'] = 1 if temp_final_aki.aki_flag.isin(['1', '1.0', 1]).any() else 0

        source_df.loc[i, f'aki_{visit_detail_type}_disch_{race_correction}'] = None if pd.isnull(row[visit_detail_end_col]) else 1 if temp_final_aki.loc[(
            temp_final_aki.specimen_taken_date_time >= row[visit_detail_end_col]), 'aki_flag'].isin(['1', '1.0', 1]).any() else 0

    # append the overal aki_type, stage at discharge, and max stage
    return check_load_df(source_df, desired_types={'visit_occurrence_id': 'sparse_int'})\
        .merge(aki_trajectory_df, how='left', on=[eid])\
        .merge(aki_summary_df, how='left', on=eid)


def fill_aki_outcome(row: pd.Series,
                     i: int,
                     source_df: pd.DataFrame,
                     temp_final_aki: pd.DataFrame,
                     interval_label: str,
                     inteval_duration: str,
                     race_correction: str,
                     interval_reference_col: str):
    """
    Fill boolean AKI status for a given interval in both 24hr days and calendar days.

    Parameters
    ----------
    source_df : pd.Dataframe
        DESCRIPTION.
    temp_aki_final : pd.DataFrame
        DESCRIPTION.
    interval_label : str
        DESCRIPTION.
    inteval_duration : str
        DESCRIPTION.
    interval_reference_col : str
        DESCRIPTION.

    Returns
    -------
    None.

    """
    # calculate 24 hr interval
    if pd.isnull(row[interval_reference_col]):
        source_df.loc[i, f'{interval_label}_{inteval_duration.lower()}_{race_correction}'] = None
        source_df.loc[i, f'{interval_label}_{inteval_duration.lower()}_cal_{race_correction}'] = None
    else:
        # check for observations in hour based interval
        hourly_interval_mask = ((temp_final_aki.specimen_taken_date_time >= row[interval_reference_col])
                                & (temp_final_aki.specimen_taken_date_time <= (row[interval_reference_col] + pd.to_timedelta(inteval_duration))))

        if any(hourly_interval_mask):
            # check for any aki events within interval
            aki_mask = (hourly_interval_mask & (temp_final_aki.aki_flag.isin(['1', '1.0', 1])))

            # fill appropriate value
            source_df.loc[i, f'{interval_label}_{inteval_duration.lower()}_{race_correction}'] = 1 if aki_mask.any() else 0
        else:
            # fill appropriate value
            source_df.loc[i, f'{interval_label}_{inteval_duration.lower()}_{race_correction}'] = None

        # check for observations in calendar day interval
        cal_interval_mask = ((temp_final_aki.specimen_taken_date_time >= row[interval_reference_col].normalize())
                             & (temp_final_aki.specimen_taken_date <= (row[interval_reference_col].normalize() + pd.to_timedelta(inteval_duration))))

        if any(cal_interval_mask):

            # check for any aki events within calendar day interval
            cal_aki_mask = (cal_interval_mask & (temp_final_aki.aki_flag.isin(['1', '1.0', 1])))

            # fill appropriate value
            source_df.loc[i, f'{interval_label}_{inteval_duration.lower()}_cal_{race_correction}'] = 1 if cal_aki_mask.any() else 0
        else:
            # fill appropriate value
            source_df.loc[i, f'{interval_label}_{inteval_duration.lower()}_cal_{race_correction}'] = None


if __name__ == "__main__":
    pass
