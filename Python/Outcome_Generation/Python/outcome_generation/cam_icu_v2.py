# -*- coding: utf-8 -*-
"""
Created on Thu Oct 15 13:14:47 2020.

@author: ruppert20
"""
import pandas as pd
import re


def cam_icu_outcome(cam_df: pd.DataFrame,
                    base_df: pd.DataFrame,
                    unique_index_col: str,
                    eid: str,
                    visit_start_col: str,
                    visit_end_col: str,
                    visit_detail_end_col: str,
                    visit_detail_type: str = 'surg',
                    time_intervals: list = ['1D', '3D', '7D', 'disch'],
                    **logging_kwargs) -> pd.DataFrame:
    """
    Generate CAM ICU Delirium Outcomes.

    Parameters
    ----------
    directory : str
        directory where the input file is located.
    cam_name : str
        name of the file to be loaded.
    base_df : pd.DataFrame
        DataFrame containing atleast the following columns:
            *eid
            *visit_start_col
            *visit_end_col
            *visit_detail_end_col
            *unique_index_col.
    unique_index_col : str
        Unique index column. This will likely be the visit_detail_id.
    eid : str
        Encounter/Visit Occurrence ID.
    visit_start_col : str
        Column corresponding to visit_start_datetime or hospital admission datetime.
    visit_end_col : str
        Column corresponding to visit_end_datetime or hospital discharge datetime..
    visit_detail_end_col : str
        Column corresponding to visit_detail_end_datetime or surgery stop datetime.
    patterns : list
        List of patterns used to load the cam file.
    regex : bool
        Whether a regular expression is used in the patterns.
    visit_detail_type : str, optional
        Whether the visit details in quesiton are surgical (surg) or icu stays (icu). The default is 'surg'.
    time_intervals : list, optional
        The time intervals from the visit_start_col and visit_detail_end_col to be used to calculate outcomes. The default is ['1D', '3D', '7D', 'disch'].
    **logging_kwargs : TYPE
        kwargs to be passed to the logging function.

    Returns
    -------
    pd.DataFrame
        Cam outcome dataframe.

    """

    # filter for relevant types of observations
    if 'value_as_concept_id' in cam_df.columns:
        cam_df.query('variable_name == "cam"', inplace=True)
        cam_df.query("value_as_concept_id.isin(['9191', '9189', '45877572'])", engine='python', inplace=True)
        cam_df.value_as_concept_id.replace({'9189': 'NEGATIVE',
                                            '9191': 'POSITIVE',
                                            '45877572': 'Unable to Assess'}, inplace=True)
        cam_df: pd.DataFrame = cam_df[[eid, 'measurement_datetime', 'value_as_concept_id']].copy(deep=True)\
            .rename(columns={'measurement_datetime': 'recorded_time', 'value_as_concept_id': 'meas_value'})
    else:
        cam_df = cam_df.loc[cam_df.disp_name.isin(['CAM Screening Results', '*RETIRED* CALCULATING POSITIVE OR NEGATIVE FOR DELIRIUM',
                                                   'CALCULATING POSITIVE OR NEGATIVE FOR DELIRIUM']),
                            [eid, 'recorded_time', 'meas_value']]

    # group simultaneous observations
    cam_sum = cam_df\
        .groupby([eid, 'recorded_time'])\
        .agg({'meas_value': take_highest_priority_cam_measure})\
        .rename(columns={'meas_value': 'cam_delirium_indicator'})\
        .reset_index()

    base_df = base_df[['subject_id', eid, unique_index_col, visit_start_col, visit_end_col, visit_detail_end_col]]

    for i, row in base_df.copy().iterrows():

        for reference_int, start_col in {visit_detail_type: visit_detail_end_col, 'adm': visit_start_col}.items():

            for time_interval in time_intervals:

                # 24 hr based intereval
                base_df.loc[i,
                            f"delirium_cam_{reference_int}_{time_interval.lower()}"] = None if pd.isna(row[start_col])\
                    else 1 if cam_sum.loc[((cam_sum[eid] == row[eid])
                                           & (cam_sum.recorded_time >= row[start_col])
                                           & (cam_sum.recorded_time <= (row[visit_end_col] if time_interval == 'disch'
                                                                        else (row[start_col] + pd.to_timedelta(time_interval))))),
                                          'cam_delirium_indicator'].dropna().sum() > 0 else 0

                # calendar day based intereval
                base_df.loc[i,
                            f"delirium_cam_{reference_int}_{time_interval.lower()}_cal"] = None if pd.isna(row[start_col])\
                    else 1 if cam_sum.loc[((cam_sum[eid] == row[eid])
                                           & (cam_sum.recorded_time >= row[start_col])
                                           & (cam_sum.recorded_time <= (row[visit_end_col] if time_interval == 'disch'
                                                                        else (row[start_col].normalize() + pd.to_timedelta(time_interval))))),
                                          'cam_delirium_indicator'].dropna().sum() > 0 else 0

    # calcualte for entire encounter
    return base_df.drop(columns=[visit_start_col, visit_end_col, visit_detail_end_col])


def take_highest_priority_cam_measure(x: pd.Series) -> int:
    """
    Code for delirium as 1 or 0 based on series of measurements.

    Parameters
    ----------
    x : pd.Series
        DESCRIPTION.

    Returns
    -------
    int
        DESCRIPTION.

    """
    # return postive if any of the observations were positive
    if any(x.astype(str).str.contains('positive', flags=re.IGNORECASE)):
        return 1
    # return negative if not positive
    return 0
