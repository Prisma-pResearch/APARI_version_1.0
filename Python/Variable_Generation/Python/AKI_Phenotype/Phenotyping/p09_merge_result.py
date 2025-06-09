# -*- coding: utf-8 -*-
"""
Created on Mon Dec  7 23:09:20 2020.

@author: renyuanfang
"""
import os
from Utils.file_operations import load_data


def p09_merge_outputfile(inmd_dir: str, out_dir: str, eid: str, pid: str, out_prefix: str):
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
    # merge ckd summary file
    pattern = r'_[0-9]+\.csv'

    success_fp: str = os.path.join(inmd_dir, 'aki_ckd_p09_complete')

    if os.path.exists(success_fp):
        return

    ckd_summary = load_data(directory=os.path.join(inmd_dir, 'encounter_ckd'),
                            file_path_query='encounter_ckd',
                            patterns=pattern,
                            regex=True,
                            return_log=False,
                            eid=eid,
                            pid=pid)\
        .drop_duplicates(subset=[eid])

    ckd_no_esrd = load_data(directory=os.path.join(inmd_dir, 'encounter_ckd'),
                            file_path_query='encounter_ckd_noesrd',
                            patterns=pattern,
                            regex=True,
                            return_log=False,
                            eid=eid,
                            pid=pid)\
        .drop_duplicates([eid])

    # merge aki_summary file
    aki_summary = load_data(directory=os.path.join(inmd_dir, 'encounter_aki'),
                            file_path_query='encounter_aki_summary',
                            patterns=pattern,
                            regex=True,
                            return_log=False,
                            pid=pid,
                            eid=eid)\
        .drop_duplicates([eid])

    final_aki = load_data(directory=os.path.join(inmd_dir, 'encounter_aki'),
                          file_path_query='encounter_final_aki',
                          patterns=pattern,
                          regex=True,
                          return_log=False,
                          pid=pid,
                          eid=eid)

    aki_daily = load_data(directory=os.path.join(inmd_dir, 'encounter_aki'),
                           file_path_query='encounter_aki_daily',
                           patterns=pattern,
                           regex=True,
                           return_log=False,
                           pid=pid,
                           eid=eid)

    aki_trajectory = load_data(directory=os.path.join(inmd_dir, 'aki_trajectory'),
                               file_path_query='aki_trajectory',
                               patterns=pattern,
                               regex=True,
                               return_log=False,
                               pid=pid,
                               eid=eid)\
        .drop_duplicates([eid])

    encounter_dischg = load_data(directory=os.path.join(inmd_dir, 'aki_trajectory'),
                                 file_path_query='encounter_dischg_place',
                                 patterns=pattern,
                                 regex=True,
                                 return_log=False,
                                 eid=eid,
                                 pid=pid)\
        .drop_duplicates([eid])

    # merge rrt summary file
    rrt_summary = load_data(directory=os.path.join(inmd_dir, 'dialysis_time'),
                            file_path_query='dialysis_summary',
                            patterns=pattern,
                            regex=True,
                            return_log=False,
                            pid=pid,
                            eid=eid)\
        .drop_duplicates([eid])

    # load patient encounter map
    pid_enc_map = load_data(directory=os.path.join(inmd_dir, 'filtered_encounters'),
                            file_path_query='enc_id_map_file',
                            patterns=pattern,
                            regex=True,
                            pid=pid, eid=eid)

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

    ckd_summary.to_csv(os.path.join(out_dir, f'{out_prefix}_ckd_summary.csv'), index=False)
    ckd_no_esrd.to_csv(os.path.join(out_dir, f'{out_prefix}_ckd_noesrd_summary.csv'), index=False)
    aki_summary.to_csv(os.path.join(out_dir, f'{out_prefix}_aki_summary.csv'), index=False)
    aki_trajectory.merge(pid_enc_map,
                      how='left',
                      on=eid)\
        .to_csv(os.path.join(out_dir, f'{out_prefix}_aki_trajectory.csv'), index=False)
    final_aki.merge(pid_enc_map,
                      how='left',
                      on=eid)\
        .to_csv(os.path.join(out_dir, f'{out_prefix}_final_aki.csv'), index=False)
    aki_daily.merge(pid_enc_map,
                      how='left',
                      on=eid)\
        .to_csv(os.path.join(out_dir, f'{out_prefix}_aki_daily.csv'), index=False)
    rrt_summary.merge(pid_enc_map,
                      how='left',
                      on=eid)\
        .to_csv(os.path.join(out_dir, f'{out_prefix}_rrt_summary.csv'), index=False)
    summary.to_csv(os.path.join(out_dir, f'{out_prefix}_ckd_aki_master.csv'), index=False)
    encounter_dischg.merge(pid_enc_map,
                      how='left',
                      on=eid)\
        .to_csv(os.path.join(out_dir, f'{out_prefix}_encounter_mort_dischg_place.csv'), index=False)

    open(success_fp, 'a').close()
