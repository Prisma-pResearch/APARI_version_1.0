# -*- coding: utf-8 -*-
"""
Created on Mon Dec  7 23:09:20 2020.

@author: renyuanfang
"""
import os
from .Utilities.FileHandling.io import check_load_df, save_data
from .utils import eGFR_fun


def p09_merge_outputfile(inmd_dir: str, out_dir: str, eid: str, pid: str, out_prefix: str, batch: str, pattern: str, **logging_kwargs):
    '''
    This function is used to merge all files stored in batch and output a single csv file to the destination folder.

    Parameters
    ----------
        inmd_dir:
            intermediate directory location
        out_dir: str
            output directory location
        eid: str
            encounter id column name, initial value = 'merged_enc_id'
        pid: str
            patient id column name

    Returns
    -------
    None
    '''
    success_fp: str = os.path.join(inmd_dir, f'aki_ckd_p09_complete_{batch}')

    if os.path.exists(success_fp):
        return

    ckd_summary = check_load_df(directory=os.path.join(inmd_dir, 'encounter_ckd'),
                                input_v='encounter_ckd',
                                patterns=pattern,
                                regex=True,
                                allow_empty_files=True,
                                eid=eid,
                                pid=pid,
                                **logging_kwargs)\
        .drop_duplicates(subset=[eid])

    ckd_no_esrd = check_load_df(directory=os.path.join(inmd_dir, 'encounter_ckd'),
                                input_v='encounter_ckd_noesrd',
                                patterns=pattern,
                                regex=True,
                                allow_empty_files=True,
                                eid=eid,
                                pid=pid,
                                **logging_kwargs)\
        .drop_duplicates([eid])

    # merge aki_summary file
    aki_summary = check_load_df(directory=os.path.join(inmd_dir, 'encounter_aki'),
                                input_v='encounter_aki_summary',
                                patterns=pattern,
                                regex=True,
                                pid=pid,
                                eid=eid,
                                allow_empty_files=True,
                                **logging_kwargs)\
        .drop_duplicates([eid])

    final_aki = check_load_df(directory=os.path.join(inmd_dir, 'encounter_aki'),
                              input_v='encounter_final_aki',
                              patterns=pattern,
                              allow_empty_files=True,
                              regex=True,
                              pid=pid,
                              eid=eid,
                              **logging_kwargs)

    final_aki['egfr'] = None if final_aki.shape[0] == 0 else final_aki.apply(lambda row: eGFR_fun(age=float(row.age), sex=row.sex, race=row.race,
                                                                             row_creatinine=float(row.lab_result), race_correction=False, version=2), axis=1)

    aki_daily = check_load_df(directory=os.path.join(inmd_dir, 'encounter_aki'),
                              input_v='encounter_aki_daily',
                              patterns=pattern,
                              allow_empty_files=True,
                              regex=True,
                              pid=pid,
                              eid=eid,
                              **logging_kwargs)

    aki_trajectory = check_load_df(directory=os.path.join(inmd_dir, 'aki_trajectory'),
                                   input_v='aki_trajectory',
                                   patterns=pattern,
                                   regex=True,
                                   allow_empty_files=True,
                                   pid=pid,
                                   eid=eid,
                                   **logging_kwargs)\
        .drop_duplicates([eid])

    encounter_dischg = check_load_df(directory=os.path.join(inmd_dir, 'aki_trajectory'),
                                     input_v='encounter_dischg_place',
                                     patterns=pattern,
                                     regex=True,
                                     allow_empty_files=True,
                                     eid=eid,
                                     pid=pid,
                                     **logging_kwargs)\
        .drop_duplicates([eid])

    # merge rrt summary file
    rrt_summary = check_load_df(directory=os.path.join(inmd_dir, 'dialysis_time'),
                                input_v='dialysis_summary',
                                patterns=pattern,
                                regex=True,
                                allow_empty_files=True,
                                pid=pid,
                                eid=eid,
                                **logging_kwargs)\
        .drop_duplicates([eid])

    # load patient encounter map
    pid_enc_map = check_load_df(directory=os.path.join(inmd_dir, 'filtered_encounters'),
                                input_v='enc_id_map_file',
                                patterns=pattern,
                                regex=True,
                                allow_empty_files=True,
                                pid=pid, eid=eid,
                                **logging_kwargs)

    # merge everything
    summary = pid_enc_map.merge(ckd_summary.copy(),
                                on=[pid, eid],
                                how='left')

    cols = []
    for x in aki_summary.columns.tolist():
        if x in ['admit_datetime', 'dischg_datetime', 'sex', 'race', 'age', 'final_class', 'egfr', 'mdrd']:
            continue
        cols.append(x)

    summary = summary.merge(aki_summary[cols], on=[pid, eid], how='left')
    summary = summary.merge(rrt_summary, on=[eid], how='left')
    summary = summary.merge(aki_trajectory[[eid, 'aki_recovery', 'paki', 'overall_type', 'akd_greater_than_7_days', 'aki_recovery_undetermined']], on=eid, how='left')
    summary = summary.merge(encounter_dischg, on=eid, how='left')

    save_data(df=ckd_summary, out_path=os.path.join(out_dir, f'{out_prefix}_ckd_summary_{batch}.csv'), index=False, **logging_kwargs)
    save_data(df=ckd_no_esrd, out_path=os.path.join(out_dir, f'{out_prefix}_ckd_noesrd_summary_{batch}.csv'), index=False, **logging_kwargs)
    save_data(df=aki_summary, out_path=os.path.join(out_dir, f'{out_prefix}_aki_summary_{batch}.csv'), index=False, **logging_kwargs)
    save_data(df=aki_trajectory.merge(pid_enc_map,
                                      how='left',
                                      on=eid),
              out_path=os.path.join(out_dir, f'{out_prefix}_aki_trajectory_{batch}.csv'), index=False, **logging_kwargs)
    save_data(df=final_aki.merge(pid_enc_map,
                                 how='left',
                                 on=eid),
              out_path=os.path.join(out_dir, f'{out_prefix}_final_aki_{batch}.csv'), index=False, **logging_kwargs)
    save_data(df=aki_daily.merge(pid_enc_map,
                                 how='left',
                                 on=eid),
              out_path=os.path.join(out_dir, f'{out_prefix}_aki_daily_{batch}.csv'), index=False, **logging_kwargs)
    save_data(df=rrt_summary.merge(pid_enc_map,
                                   how='left',
                                   on=eid),
              out_path=os.path.join(out_dir, f'{out_prefix}_rrt_summary_{batch}.csv'), index=False, **logging_kwargs)
    save_data(df=summary, out_path=os.path.join(out_dir, f'{out_prefix}_ckd_aki_master_{batch}.csv'), index=False, **logging_kwargs)
    save_data(df=encounter_dischg.merge(pid_enc_map,
                                        how='left',
                                        on=eid),
              out_path=os.path.join(out_dir, f'{out_prefix}_encounter_mort_dischg_place_{batch}.csv'), index=False, **logging_kwargs)

    open(success_fp, 'a').close()
