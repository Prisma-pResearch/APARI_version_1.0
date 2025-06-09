# -*- coding: utf-8 -*-
"""
Created on Fri Sep  4 09:26:29 2020.

@author: ruppert20
"""
import pandas as pd
from ..Utilities.PreProcessing.time_intervals import condense_overlapping_segments


def prepare_stations_for_icu_outcomes(source_df: pd.DataFrame,
                                      eid: str,
                                      pid: str,
                                      visit_detail_start_col: str,
                                      visit_detail_end_col: str,
                                      df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare internal stations file for ICU outcome generation.

    Actions:
        1.Filter ICU rows from internal stations file
        2. Merge any ICU stays with less than 24 hours in between

    Parameters
    ----------
    source_df : pd.DataFrame
        DESCRIPTION.
    eid : str
        DESCRIPTION.
    pid : str
        DESCRIPTION.
    df : pd.DataFrame
        DESCRIPTION.

    Returns
    -------
    TYPE
        DESCRIPTION.

    """
    # filter ICU stations
    if 'variable_name' in df.columns:
        df.rename(columns={'variable_name': 'location_type',
                           'visit_detail_start_datetime': visit_detail_start_col,
                           'visit_detail_end_datetime': visit_detail_end_col}, inplace=True)
        
    icu_df = df.loc[((df.location_type.str.lower() == 'icu') & df[pid].isin(source_df[pid].unique())),
                    [pid, eid, visit_detail_start_col, visit_detail_end_col]]

    # merge ICU stays that have less than 24 hours in between
    icu_df = icu_df.groupby(eid, group_keys=False).apply(condense_overlapping_segments,
                                       grouping_columns=[eid],
                                       start_col=visit_detail_start_col,
                                       end_col=visit_detail_end_col,
                                       eid=eid,
                                       gap_tolerance_hours=24)\
        .reset_index(drop=(icu_df.shape[0] == 0)).drop(columns=['level_1'], errors='ignore')

    return icu_df.rename(columns={visit_detail_start_col: 'start_datetime', visit_detail_end_col: 'end_datetime'})
