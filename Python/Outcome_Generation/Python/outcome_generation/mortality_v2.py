# -*- coding: utf-8 -*-
"""
Created on Mon Sep 14 10:33:48 2020.

@author: ruppert20
"""
import pandas as pd
import re
from typing import Dict, List
from ..Utilities.FileHandling.io import check_load_df


def generate_mortality_outcomes_v2(source_df: pd.DataFrame,
                                ssdi_df: pd.DataFrame,
                                hsp_df: pd.DataFrame,
                                eid: str,
                                pid: str,
                                visit_occurrence_start: str,
                                visit_occurrence_end: str,
                                visit_detail_end: str,
                                visit_detail_start: str,
                                visit_detail_type: str,
                                time_intervals: Dict[str, List[str]],
                                **logging_kwargs) -> pd.DataFrame:
    """
    Generate mortality outcomes.

    Actions:
        1. Create clean death date using the following fill order:
            1. hospital death date
            2. ssdi death date
            3. discharge date (if discharge disposiiton is expired)
        2. Flag hospital mortality by the following conditions
            a. death during admission
            b. dischage dispositon as expired
            c. discharge disposition is hospice and death date is within 7 days of dicharge date

    Parameters
    ----------
    source_df : pd.DataFrame
        DESCRIPTION.
    ssdi_df : pd.DataFrame
        DESCRIPTION.
    hsp_df : pd.DataFrame
        DESCRIPTION.
    eid : str
        DESCRIPTION.
    pid : str
        DESCRIPTION.
    post_op_intervals : list, optional
        DESCRIPTION. The default is ['3D', '7D'].

    Returns
    -------
    TYPE
        DESCRIPTION.

    """
    if 'death_type_concept_id' in ssdi_df.columns:
        ssdi_df.loc[:, 'death_type_concept_id'] = ssdi_df.death_type_concept_id.apply(lambda x: {'32817': 'death_date', '32885': 'ssdi_death_date'}.get(x, 'death_date')).values

        if ssdi_df.shape[0] == 0:
            ssdi_df = pd.DataFrame(columns=[pid, 'ssdi_death_date', 'death_date'])
        else:
            ssdi_df = ssdi_df.groupby([pid, 'death_type_concept_id'])['death_date'].min().reset_index(drop=False)\
                .pivot(index=pid, columns='death_type_concept_id')['death_date']\
                .reset_index(drop=False)

        death_df = check_load_df(ssdi_df, desired_types={'death_date': 'datetime', 'ssdi_death_date': 'datetime'},
                                 ensure_cols=[pid, 'death_date', 'ssdi_death_date'])\
            .groupby(pid).agg({'death_date': 'min', 'ssdi_death_date': 'min'})\
            .reset_index(drop=False)
    else:
        death_df = check_load_df(ssdi_df, desired_types={'ssdi_death_date': 'datetime'},
                                 usecols=[pid, 'ssdi_death_date'])\
            .groupby(pid).min()\
            .merge(check_load_df(hsp_df, desired_types={'death_date': 'datetime'},
                                 usecols=[pid, 'death_date'])
                   .groupby(pid).min(),
                   left_index=True,
                   right_index=True,
                   how='outer')\
            .reset_index()

    fillable_idx: pd.Series = death_df.death_date.isnull() & death_df.ssdi_death_date.notnull()
    if fillable_idx.any():
        death_df.loc[fillable_idx, 'death_date'] = death_df.loc[fillable_idx, 'ssdi_death_date'].values

    # merge the death date information to the source df
    source_df = source_df\
        .merge(death_df[[pid, 'death_date']].rename(columns={'death_date': 'clean_death_date'}),
               on=pid, how='left')\
        .reset_index(drop=True)\
        .reset_index()\
        .rename(columns={'index': 'temp_index'})

    if 'discharged_to_concept_id' in source_df.columns:
        source_df['dischg_disposition'] = source_df.discharged_to_concept_id.replace({'8863': 'TO SKILLED NURSING',
                                                                                      '4139502': 'TO HOME',
                                                                                      '4111199': 'TO ANOTHER HOSPITAL',
                                                                                      '8920': 'TO REHAB',
                                                                                      '4306655': 'EXPIRED NO AUT',
                                                                                      '8546': 'TO HOSPICE HOME',
                                                                                      '8971': 'TO OTHER PSYCHIATRIC FACILITY',
                                                                                      '38003619': 'TO COURT OR LAW ENFORCEMENT',
                                                                                      '1333151': 'TO INTERMEDIATE CARE FACILITY',
                                                                                      '40482021': 'AMA',
                                                                                      '37117117': 'LWBS',
                                                                                      '8827': 'TO CUSTODIAL CARE'}).copy()

    # add encounter death flag
    admit_death = (((source_df.clean_death_date >= source_df[visit_occurrence_start]) & (source_df.clean_death_date <= source_df[visit_occurrence_end]))
                   | source_df.dischg_disposition.astype(str).str.contains('expired', flags=re.IGNORECASE, na=False)
                   | (source_df.dischg_disposition.astype(str).str.contains('hospice', flags=re.IGNORECASE, na=False)
                       & (source_df.clean_death_date <= source_df[visit_occurrence_end] + pd.to_timedelta('7 days'))))

    source_df['hospital_mortality'] = 0

    if any(admit_death):
        source_df.loc[admit_death, 'hospital_mortality'] = 1

        update_death_index = (admit_death & (source_df.clean_death_date > source_df[visit_occurrence_end]))

        source_df.loc[update_death_index, 'clean_death_date'] = source_df.loc[update_death_index, visit_occurrence_end].dt.date

    if len(time_intervals) > 0:
        for k, intervals in time_intervals.items():
            
            reference_point: str = locals().get(k)
            base_label: str = visit_detail_type if 'visit_detail' in k else 'visit_occurrence'
            base_label: str = f'{base_label}_{"start" if "start" in k else "end"}'
            assert isinstance(reference_point, str), f'Unable to locate the corresponding reference time point for the key {k}. Pleasse choose from one of the following: ["visit_occurrence_start", "visit_occurrence_end", "visit_detail_start", "visit_detail_end"]'
            
            for _, row in source_df.copy(deep=True).iterrows():

                for interval in intervals:
                    # calculate 24 hr interval
                    if pd.isnull(row[reference_point]):  # or pd.isnull(row.clean_death_date):
                        source_df.loc[(source_df.temp_index == row.temp_index), f'{base_label}_death_{interval.lower()}'] = None
                    elif ((row.clean_death_date >= row[reference_point].normalize()) & (row.clean_death_date <= (row[reference_point] + pd.to_timedelta(interval)))):
                        source_df.loc[(source_df.temp_index == row.temp_index), f'{base_label}_death_{interval.lower()}'] = 1
                    else:
                        source_df.loc[(source_df.temp_index == row.temp_index), f'{base_label}_death_{interval.lower()}'] = 0

                    # calculate calendar day interval
                    if pd.isnull(row[reference_point]):  # or pd.isnull(row.clean_death_date):
                        source_df.loc[(source_df.temp_index == row.temp_index), f'{base_label}_death_{interval.lower()}_cal'] = None
                    elif ((row.clean_death_date >= row[reference_point].normalize()) & (row.clean_death_date <= (row[reference_point].normalize() + pd.to_timedelta(interval)))):
                        source_df.loc[(source_df.temp_index == row.temp_index), f'{base_label}_death_{interval.lower()}_cal'] = 1
                    else:
                        source_df.loc[(source_df.temp_index == row.temp_index), f'{base_label}_death_{interval.lower()}_cal'] = 0

    return check_load_df(source_df.reset_index(drop=True),
                         desire_types={'clean_death_date': 'date'})\
        .drop(columns=['temp_index'], errors='ignore')


if __name__ == "__main__":

    pass
