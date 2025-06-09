# -*- coding: utf-8 -*-
"""
Created on Tue Nov 24 11:56:02 2020.

@author: renyuanfang
"""

import pandas as pd
import os
from datetime import timedelta
from .Utilities.FileHandling.io import check_load_df, save_data


def p04_create_creatinine_parameters(inmd_dir: str, eid: str, batch: int, pid: str, **logging_kwargs):
    """
    Create various creatinine parameters for each unique patient encounter.

    1. Load encounter and creatinine files in the intermediate directory as dataframe
    2. For creatinine dataframe, clean columns to correct data types and keep rows with only lab_result <= 30
    3. Use the cleaned creatinine dataframe to create the following dataframes
        * admission_cr: keep rows where specimen_date = admit_date, this represents all the creatinine value collected on patient admission date
        * previous_7_cr: keep rows where admit_date - specimen_date <= 7, this represents all the creatinine value collected 7 days prior to patient admission date
        * previous_8_365_cr: keep rows where 7 < admit_date - specimen_date <= 365, this represents all the creatinine value collected 8 days to 1 year prior to patient admission date
    4. Aggregate lab_result column for admission_cr, previous_7_cr and previous_8_365_cr
        * For admission_cr, previous_7_cr, take the minimum creatinine value as lab_result for each distinct encounter id rename as admission_creatinine, min_7_days and record corresponding dates
        * For previous_8_365_cr, take the median creatinine value as lab_result for each distinct encounter id rename as medium_8_365_days. Using 'gap' column by taking the absolute difference between orignal lab_result and median creatinine value and record the date with the least gap.
    5. Left join encounter dataframe with 3 updated creatinine dataframes sequentially on encounter id.
    6. Save the updated encounter dataframe to intermediate directory as csv file. New columns added:
        * admission_creatinine
        * min_7_days
        * min_7_days_date
        * medium_8_365_days
        * medium_8_365_days_date

    Parameters
    ----------
        inmd_dir: str
            intermediate file directory
        batch: int
            batch number, default value = 0
        eid: str
            encouter id column name, default value = 'merged_enc_id'

    Returns
    -------
    None
    """
    encounter = check_load_df(os.path.join(inmd_dir, 'encounter_egfr_flags', 'encounter_egfr_flags_{}.csv'.format(batch)),
                              pid=pid, eid=eid, dtype=None, **logging_kwargs)
    creatinine = check_load_df(os.path.join(inmd_dir, 'ckd_row_egfr', 'ckd_row_egfr_{}.csv'.format(batch)),
                               desired_types={'admit_datetime': 'datetime', 'inferred_specimen_datetime': 'datetime'},
                               pid=pid, eid=eid, dtype=None, **logging_kwargs)

    creatinine['lab_result'] = pd.to_numeric(creatinine['lab_result'], errors='coerce')
    creatinine = creatinine[creatinine['lab_result'] <= 30]
    creatinine['specimen_date'] = creatinine['inferred_specimen_datetime'].dt.date
    creatinine['admit_date'] = creatinine['admit_datetime'].dt.date

    admission_cr = creatinine[creatinine['specimen_date'] == creatinine['admit_date']]
    previous_7_cr = creatinine[((creatinine['specimen_date'] < creatinine['admit_date'])
                                & (creatinine['specimen_date'] >= (creatinine['admit_date'] - timedelta(days=7))))]
    previous_8_365_cr = creatinine[(creatinine['specimen_date'] < (creatinine['admit_date'] - timedelta(days=7))) & (creatinine['specimen_date'] >= (creatinine['admit_date'] - timedelta(days=365)))]

    min_admission_cr = admission_cr.groupby([eid], as_index=False)['lab_result'].min().rename(columns={'lab_result': 'admission_creatinine'})
    min_previous_7_cr = previous_7_cr.groupby([eid], as_index=False)['lab_result'].min().rename(columns={'lab_result': 'min_7_days'})
    medium_previous_8_365_cr = previous_8_365_cr.groupby([eid], as_index=False)['lab_result'].median().rename(columns={'lab_result': 'medium_8_365_days'})

    previous_7_cr = previous_7_cr.merge(min_previous_7_cr, on=eid, how='left')
    previous_8_365_cr = previous_8_365_cr.merge(medium_previous_8_365_cr, on=eid, how='left')

    previous_7_cr = previous_7_cr[previous_7_cr['lab_result'] == previous_7_cr['min_7_days']]
    previous_7_cr = previous_7_cr.sort_values([eid, 'specimen_date']).drop_duplicates([eid], keep='last')

    if previous_8_365_cr.shape[0] == 0:
        previous_8_365_cr = pd.DataFrame(columns=previous_8_365_cr.columns.tolist() + ['gap'])
    else:
        previous_8_365_cr['gap'] = previous_8_365_cr[['lab_result', 'medium_8_365_days']].apply(lambda x: (-1) * abs(x[0] - x[1]), axis=1)
    previous_8_365_cr = previous_8_365_cr.sort_values([eid, 'gap', 'specimen_date'])
    previous_8_365_cr = previous_8_365_cr.drop_duplicates([eid], keep='last')

    encounter = encounter.merge(min_admission_cr, on=eid, how='left')
    encounter = encounter.merge(previous_7_cr[[eid, 'min_7_days', 'specimen_date']], on=eid, how='left').rename(columns={'specimen_date': 'min_7_days_date'})
    encounter = encounter.merge(previous_8_365_cr[[eid, 'medium_8_365_days', 'specimen_date']], on=eid, how='left').rename(columns={'specimen_date': 'medium_8_365_days_date'})

    save_data(df=encounter,
              out_path=os.path.join(inmd_dir,
                                    'encounter_creatinine_parameters',
                                    'encounter_creatinine_parameters_{}.csv'.format(batch)),
              index=False, **logging_kwargs)
