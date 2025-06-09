# -*- coding: utf-8 -*-
"""
Module to Resample medications to the minute.

Created on Wed Nov  2 14:10:40 2022

@author: ruppert20
"""
from typing import Union
import pandas as pd
from ..FileHandling.io import check_load_df, save_data


def _resample_meds(med_df: pd.DataFrame, dose_col: str) -> pd.DataFrame:

    return pd.concat([pd.DataFrame(pd.Series(index=[med_df.iloc[0, med_df.columns.get_loc('start')], med_df.iloc[0, med_df.columns.get_loc('end')]],
                                             data=[0, 0]).resample('1min').ffill()).T,

                      med_df[['taken_datetime', 'end_datetime', dose_col]].copy()
                      .apply(lambda row: pd.Series(index=[row.taken_datetime, row.end_datetime + pd.to_timedelta('1 second')],
                                                   data=[row[dose_col], row[dose_col]]).resample('1min').ffill(), axis=1)],
                     axis=0, sort=False).sum().reset_index(drop=True)


def extract_and_resample_introp_fluids(clean_meds: Union[str, pd.DataFrame],
                                       clean_or_cases: Union[str, pd.DataFrame],
                                       eid: str = 'merged_enc_id',
                                       pid: str = 'patient_deiden_id',
                                       unique_row_col: str = 'or_case_num',
                                       result_fp: str = None,
                                       **logging_kwargs) -> pd.DataFrame:
    """
    Extract and Resample intraop fluids to the minute.

    Parameters
    ----------
    clean_meds : Union[str, pd.DataFrame]
        Either a pandas dataframe or path to meds table.
    clean_or_cases : Union[str, pd.DataFrame]
        Either a pandas dataframe or path to or_case_table.
    eid : str, optional
        Encounter ID column. The default is 'merged_enc_id'.
    pid : str, optional
        Pateint ID Column. The default is 'patient_deiden_id'.
    unique_row_col : str, optional
        Unique row index for the or_case_schedule file. The default is 'or_case_num'.
    result_fp : str, optional
        Output filepath where the result will be saved. The default is None, which will not save the result to file.
    **logging_kwargs : TYPE
        kwargs to pass to the log_print_email_message function from the logging module.

    Returns
    -------
    pd.DataFrame
        Resampled medication dataframe with all input iv fluids during a surgery down to the minute.
        The columns are [unique_row_col, 'minute', 'fluid_intake', 'fluid_intake_unit']

    """
    clean_meds: pd.DataFrame = check_load_df(clean_meds, desired_types={**{x: 'datetime' for x in ['taken_datetime', 'end_datetime']},
                                                                        **{x: 'float' for x in ['total_dose_character', 'fluid_volume', 'med_dosing_weight']}},
                                             usecols=[pid, eid, 'taken_datetime', 'end_datetime', 'total_dose_character', 'fluid_volume',
                                                      'fluid_volume_unit', 'med_dosing_weight', 'med_order_route', 'med_order_desc'],
                                             **logging_kwargs)

    start_stop_df = check_load_df(clean_or_cases,
                                  usecols=[pid, eid, unique_row_col, 'surgery_start_datetime', 'surgery_stop_datetime'],
                                  parse_dates=['surgery_start_datetime', 'surgery_stop_datetime'],
                                  **logging_kwargs)\
        .rename(columns={'surgery_start_datetime': 'start',
                         'surgery_stop_datetime': 'end'})\
        .dropna()

    # remove meds that have 0 dose or are rates without an end_datetime or have a missing route
    tmp = clean_meds[((clean_meds.total_dose_character != 0)
                      & (clean_meds.fluid_volume < 1E4)
                      & ~(clean_meds.fluid_volume_unit.str.contains('/hr', case=False, na=False, regex=False) & clean_meds.end_datetime.isnull())
                      & ~(clean_meds.med_order_route.isnull() & clean_meds.med_order_desc.str.contains('soak', case=False, regex=False, na=False)))].copy()

    # add end_time if missing
    tmp.loc[tmp.end_datetime.isnull(), 'end_datetime'] = tmp.loc[tmp.end_datetime.isnull(), 'taken_datetime'].values + pd.to_timedelta('1 minute')

    tmp.dropna(subset=['fluid_volume', 'end_datetime', 'taken_datetime'], inplace=True)

    rate_mask = tmp.fluid_volume_unit.str.contains('/hr', regex=False, na=False, case=False)

    # convert to minute
    tmp.loc[rate_mask, 'fluid_volume'] = tmp.loc[rate_mask, 'fluid_volume'] / 60

    tmp.loc[rate_mask, 'fluid_volume_unit'] = tmp.loc[rate_mask, 'fluid_volume_unit'].str.replace('/hr', '/min', regex=False)

    # convert fixed doses to rates/min based on start and end_datetime
    non_rate_mask = ~tmp.fluid_volume_unit.str.contains('/min', regex=False, na=False, case=False)

    tmp.loc[non_rate_mask, 'fluid_volume'] = tmp.loc[non_rate_mask, 'fluid_volume'] / ((tmp.loc[non_rate_mask, 'end_datetime'] - tmp.loc[non_rate_mask, 'taken_datetime']).dt.total_seconds() / 60)

    tmp.loc[non_rate_mask, 'fluid_volume_unit'] = tmp.loc[non_rate_mask, 'fluid_volume_unit'] + '/min'

    # normalize dose per kg
    kg_mask = ~tmp.fluid_volume_unit.str.contains('/kg', regex=False, na=False, case=False)

    # multiply by weight, if missing using 70 kgs for the weight
    tmp.loc[kg_mask, 'fluid_volume'] = (tmp.loc[kg_mask, 'fluid_volume'] / tmp.loc[kg_mask, 'med_dosing_weight'].fillna(70)).values

    tmp.loc[kg_mask, 'fluid_volume_unit'] = tmp.loc[kg_mask, 'fluid_volume_unit'] + '/kg'

    tmp = tmp.merge(start_stop_df, on=eid, how='left')

    intersecting_rows = (tmp.taken_datetime <= tmp.end) & (tmp.end_datetime >= tmp.start)

    intraop_meds = tmp[intersecting_rows].copy()

    intraop_meds.loc[:, 'taken_datetime'] = intraop_meds.loc[:, ['taken_datetime', 'start']].apply(max, axis=1).values
    intraop_meds.loc[:, 'end_datetime'] = intraop_meds.loc[:, ['end_datetime', 'end']].apply(min, axis=1).values

    output: pd.DataFrame = intraop_meds\
        .groupby(unique_row_col, group_keys=False)\
        .apply(_resample_meds,
               dose_col='fluid_volume')\
        .reset_index()\
        .rename(columns={'level_1': 'minute', 0: 'fluid_intake'})
    output['fluid_intake_unit'] = 'ml/min/kg'

    if isinstance(result_fp, str):
        save_data(df=output,
                  out_path=result_fp,
                  **logging_kwargs)

        return 'done'

    return output


if __name__ == '__main__':
    clean_meds: str = "file_path"
    clean_or_cases: str = "file_path"
    eid: str = 'merged_enc_id'
    pid: str = 'patient_deiden_id'
    unique_row_col: str = 'or_case_num'
    result_fp: str = None
    logging_kwargs: dict = {'display': True}
