# -*- coding: utf-8 -*-
"""
Created on Wed Aug 26 09:31:03 2020.

@author: Ruppert20
"""
import pandas as pd


def prepare_resp_for_mv_outcomes(source_df: pd.DataFrame,
                                 eid: str,
                                 pid: str,
                                 visit_detail_start_col: str,
                                 visit_detail_end_col: str,
                                 df: pd.DataFrame) -> pd.DataFrame:
    """
    Format respiratory dataframe for duration outcome generation.

    Actions:
        1. remove OR and procedure suite rows
        2. determine device changes and only keep rows with a device change
        3. stack times to create start and stop times for each device
        4. keep only ventilator rows

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
    resp_df : TYPE
        DESCRIPTION.

    """
    if 'variable_name' in df.columns:
        df.rename(columns={'variable_name': 'device_type',
                           'device_exposure_start_datetime': 'start_datetime',
                           'device_exposure_end_datetime': 'end_datetime'}, inplace=True)
        df.loc[:, 'device_type'] = 'ventilator'

        return df.sort_values([eid, 'start_datetime'])
    # format respiratory df to have start and stop time for devices
    # filter columns and sort chronologically
    resp_df = df.loc[(df[pid].isin(source_df[pid].unique())
                      & (~df.station_type.isin(['OR', 'Procedure suite']))),
                     [pid, eid, 'respiratory_datetime', 'device_type']]\
        .sort_values([eid, 'respiratory_datetime'])

    # determine when the device changes
    resp_df['device_delta'] = resp_df.groupby(eid)['device_type'].apply(lambda device: (device != device.shift()).astype(int))

    # filter for rows with a device change
    resp_df = resp_df[resp_df.device_delta == 1]

    # shift times up one row
    resp_df.loc[:, 'end_datetime'] = resp_df.groupby(eid)['respiratory_datetime'].shift(-1)

    # rename respiratory_datetime to start datetime
    resp_df.rename(columns={'respiratory_datetime': 'start_datetime'}, inplace=True)

    # remove non ventilator rows
    resp_df = resp_df[resp_df.device_type == 'ventilator']

    # fill the discharge datetime as the end datetime if it is still missing
    missing_end_mask = (resp_df.end_datetime.isna())

    if any(missing_end_mask):

        eids = resp_df.loc[missing_end_mask, eid].unique().tolist()

        end_dict = source_df.loc[source_df[eid].isin(eids), [eid, 'dischg_datetime']].drop_duplicates().set_index(eid).dischg_datetime.to_dict()

        resp_df.loc[missing_end_mask, 'end_datetime'] = resp_df.loc[missing_end_mask, eid].apply(lambda x: end_dict.get(x, None))

        # cleanup
        del eids, end_dict

    # subtract one second to avoid simultaneous devices
    resp_df.loc[:, 'end_datetime'] = resp_df.loc[:, 'end_datetime'].apply(lambda x: x - pd.to_timedelta('1s'))

    return resp_df


if __name__ == '__main__':

    pass
