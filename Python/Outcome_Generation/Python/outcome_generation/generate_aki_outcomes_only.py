# -*- coding: utf-8 -*-
"""
Re-Run AKI outcome to fix previous bug.

Created on Sat Feb  5 12:10:21 2022

@author: ruppert20
"""
import pandas as pd
import os
from functools import reduce
from ..Utilities.FileHandling.io import check_load_df, save_data
from .aki_outcome_v2 import aki_outcome
from ..Utilities.Logging.log_messages import log_print_email_message as logm


def generate_aki_only_outcomes_v2(clean_dir: str,
                                  encounter_name: str,
                                  or_case_name: str,
                                  aki_final_fp: str,
                                  aki_trajectory_fp: str,
                                  aki_summary_fp: str,
                                  ckd_summary_fp: str,
                                  eid: str,
                                  pid: str,
                                  out_dir: str,
                                  patterns: list,
                                  outcome_success_file_path: str = None,
                                  regex=True,
                                  outcome_file_name: str = 'original_ids_outcome_final',
                                  postop_intervals: list = ['1D', '3D', '7D'],
                                  time_intervals: list = ['surg_disch', 'admit_disch'],
                                  **logging_kwargs):
    """
    Generate outcomes final.

    Actions:
        1. Generate base dataframe which has one row for each completed OR Case and at least one row per inpatient/observation hospital encounter
        2. generate mortality outcomes from hospital and social security death index files
        3. Generate ICU LOS outcomes from internal stations file
        4. Generate ICD outcomes from diagnoses file
        5. Gernate MV outcomes from respiratory file
        6. Generate AKI and CKD outcomes from final_aki, trajectory_aki, and ckd files
        7. Genrate delirum CAM outcomes from cam file
        8. Combine aforementioned outcomes into one dataframe
        9. Adjust/add additional outcomes which are the union of the aforementioned outcomes produced
        10. Save result to file

    Parameters
    ----------
    encounter_name : str
        DESCRIPTION.
    or_case_name : str
        DESCRIPTION.
    internal_stations_name : str
        DESCRIPTION.
    ssdi_name : str
        DESCRIPTION.
    diagnoses_name : str
        DESCRIPTION.
    outcomes_map_fp : str
        DESCRIPTION.
    resp_df_name : str
        DESCRIPTION.
    aki_final_fp : str
        DESCRIPTION.
    aki_trajectory_fp : str
        DESCRIPTION.
    ckd_summary_fp : str
        DESCRIPTION.
    cam_name : str
        DESCRIPTION.
    eid : str
        DESCRIPTION.
    pid : str
        DESCRIPTION.
    outcome_file_name : str, optional
        DESCRIPTION. The default is 'original_ids_outcome_final.csv'.
    postop_intervals : list, optional
        DESCRIPTION. The default is ['1D', '3D', '7D'].
    time_intervals : list, optional
        DESCRIPTION. The default is ['surg_disch', 'admit_disch'].

    Returns
    -------
    log: str
        Log of status messages to be saved for debugging purposes.

    """
    if not isinstance(outcome_success_file_path, str):
        outcome_success_file_path: str = os.path.join(out_dir, 'outcome_success')

    if os.path.exists(outcome_success_file_path):
        return f'{outcome_file_name} alredy completed'

    logm(message='Creating base dataframe', **logging_kwargs)

    # load encounter df
    encounter_df = check_load_df(directory=clean_dir,
                                 input_v=encounter_name,
                                 patterns=patterns,
                                 ds_type='pandas',
                                 regex=regex,
                                 usecols=[pid, eid, 'merged_admit_datetime',
                                          'merged_dischg_datetime', 'encounter_type', 'patient_type',
                                          'dischg_disposition', 'death_date'],
                                 parse_dates=['merged_admit_datetime', 'merged_dischg_datetime', 'death_date'],
                                 **logging_kwargs)\
        .drop_duplicates()\
        .rename(columns={'merged_admit_datetime': 'admit_datetime',
                         'merged_dischg_datetime': 'dischg_datetime'})

    # load or case df
    or_case_df = check_load_df(directory=clean_dir,
                               input_v=or_case_name,
                               patterns=patterns,
                               ds_type='pandas',
                               regex=regex,
                               pid=pid, eid=eid,
                               usecols=[pid, eid, 'or_case_num',
                                        'sched_surgeon_dr_deiden_id', 'main_surgeon_dr_deiden_id',
                                        'main_anesthesiologist_dr_deiden_id', 'surgery_start_datetime',
                                        'surgery_stop_datetime', 'sched_location', 'cpt_1', 'cpt_1_description'],
                               parse_dates=['surgery_start_datetime', 'surgery_stop_datetime'],
                               **logging_kwargs)\
        .dropna(subset=[pid, eid, 'surgery_start_datetime', 'surgery_stop_datetime'])

    if 'or_case_num' not in or_case_df.columns:
        or_case_df.sort_values([pid, 'surgery_stop_datetime'], ascending=True, inplace=True)
        or_case_df.insert(loc=1, column='or_case_num', value=list(range(0, or_case_df.shape[0])), allow_duplicates=True)
    else:
        or_case_df = or_case_df.dropna(subset=['or_case_num']).drop_duplicates(subset=['or_case_num']).sort_values([pid, 'surgery_stop_datetime'], ascending=True)

    # add surgery order for each encounter
    or_case_df['surgery_order'] = or_case_df.groupby(eid)['or_case_num'].apply(lambda key: (key != key.shift()).astype(int).cumsum())

    # create base df
    base_df = encounter_df[[pid, eid, 'admit_datetime', 'dischg_datetime', 'dischg_disposition']]\
        .dropna(subset=[pid, eid, 'admit_datetime', 'dischg_datetime'])\
        .drop_duplicates(subset=[eid])

    # append or_case_info to base df
    base_df = base_df.merge(or_case_df,
                            on=[pid, eid],
                            how='left')

    # cols to strip
    cols_to_strip: list = [x for x in base_df.columns if x not in ['or_case_num', eid]] + ['clean_death_date']

    '''
    Calcuate AKI CKD outcomes
    '''

    logm(message='calculating AKI/CKD outcomes', **logging_kwargs)
    if isinstance(aki_final_fp, str):
        aki_outcomes = aki_outcome(source_df=base_df.copy(),
                                   pid=pid,
                                   eid=eid,
                                   aki_final_fp=aki_final_fp,
                                   aki_trajectory_fp=aki_trajectory_fp,
                                   aki_summary_fp=aki_summary_fp,
                                   time_intervals=['3D', '7D'],
                                   interval_types=['aki_surg', 'aki_admit'],
                                   **logging_kwargs)
    elif isinstance(aki_final_fp, list):
        aki_outcomes = reduce(lambda i, j: pd.merge(i[[x for x in i.columns if x not in cols_to_strip]],
                                                    j[[x for x in j.columns if x not in cols_to_strip]],
                                                    on=[eid, 'or_case_num'],
                                                    how='left'),
                              [aki_outcome(source_df=base_df.copy(),
                                           eid=eid,
                                           aki_final_fp=fin_fp,
                                           aki_trajectory_fp=traj_fp,
                                           aki_summary_fp=sum_fp,
                                           time_intervals=['3D', '7D'],
                                           interval_types=['aki_surg', 'aki_admit'],
                                           directory=None if os.path.exists(fin_fp) else clean_dir,
                                           patterns=patterns,
                                           **logging_kwargs)
                               for fin_fp, traj_fp, sum_fp in zip(aki_final_fp, aki_trajectory_fp, aki_summary_fp)])

    if isinstance(ckd_summary_fp, str):
        ckd_summary = check_load_df(input_v=ckd_summary_fp,
                                    usecols=[eid, 'final_class'],
                                    ds_type='pandas', **logging_kwargs)
    elif isinstance(ckd_summary_fp, list):
        ckd_summary = reduce(lambda x, y: pd.merge(x, y, how='outer', on=eid),
                             [check_load_df(input_v=fp,
                                            usecols=[eid, 'final_class'], ds_type='pandas',
                                            directory=clean_dir,
                                            patterns=patterns, **logging_kwargs)
                              .rename(columns={'final_class': f'final_class_{"with_race_correction" if "with_race_correction" in fp else "without_race_correction"}'})
                              .set_index(eid) for fp in ckd_summary_fp])
    else:
        logm(message='Unrecognized ckd summary file path format', error=True, raise_exception=True, **logging_kwargs)

    ckd_outcomes = base_df.copy().merge(ckd_summary, on=eid, how='left')

    del ckd_summary

    out = base_df\
        .merge(aki_outcomes[[x for x in aki_outcomes.columns if x not in cols_to_strip]], how='left', on=[eid, 'or_case_num'])\
        .merge(ckd_outcomes[[x for x in ckd_outcomes.columns if x not in cols_to_strip]], how='left', on=[eid, 'or_case_num'])

    # correct for ESRD in AKI final class is ESRD
    esrd_mask_1 = out.final_class_with_race_correction.astype(str).str.contains('ESRD', regex=False, case=False, na=False)

    if any(esrd_mask_1):
        for col in [x for x in out.columns if ('aki_' in x) and ('with_' in x)]:
            out.loc[esrd_mask_1, col] = 'ESRD'

    esrd_mask_2 = out.final_class_without_race_correction.astype(str).str.contains('ESRD', regex=False, case=False, na=False)

    if any(esrd_mask_2):
        for col in [x for x in out.columns if ('aki_' in x) and ('without_' in x)]:
            out.loc[esrd_mask_2, col] = 'ESRD'

    # save to file
    save_data(df=out,
              out_path=os.path.join(out_dir, f'{outcome_file_name}.csv'),
              index=False,
              **logging_kwargs)

    # create status file
    open(outcome_success_file_path, 'a').close()


if __name__ == "__main__":
    pass
