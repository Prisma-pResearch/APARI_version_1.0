# -*- coding: utf-8 -*-
"""
Created on Mon Nov 23 21:42:03 2020.

@author: renyuanfang
@Editor: ruppert20
"""
import os
from .p01_load_filter_files import p01_load_filter_file
from .p02_create_AdminFlag import p02_create_admin_flags
from .p03_create_prev_CreatinineFlag_row_egfr import p03_add_prev_creatinine_and_egfr_flags
from .p04_creatinine_parameters import p04_create_creatinine_parameters
from .p05_ckd_class_egfr_staging import p05_ckd_class_and_egfr_staging
from .p06_dialysis import p06_generate_rrt_summary
from .p07_aki import p07_generate_aki
from .p08_trajectory import p08_generateTrajectory
from .p09_merge_result import p09_merge_outputfile
from .Utilities.FileHandling.io import get_batches_from_directory
from .Utilities.ResourceManagement.parallelization_helper import run_function_in_parallel_v2
from .Utilities.ProjectManagement.setup_project import _ensure_folder_dict
from .Utilities.ProjectManagement.completion_monitoring import check_complete
# from .Utilities.General.func_utils import debug_inputs


def main_run_AKI_CKD_Phenotyping(dir_dict: dict,
                                 race_corrections: list,
                                 eids: list,
                                 pid: str,
                                 independent_sub_batch: bool,
                                 success_fp: str,
                                 source_data_key: str = 'source_data',
                                 out_data_key: str = 'generated_data',
                                 intermediate_data_key: str = 'intermediate_data',
                                 var_file_link_key: str = 'variable_file_link',
                                 version: int = 2,
                                 project_name: str = 'IDEALIST',
                                 patterns: list = [r'_[0-9]+\.csv', r'\.csv'],
                                 regex: bool = True,
                                 batches: list = None,
                                 serial: bool = False,
                                 max_workers: int = None):

    if os.path.exists(success_fp):
        return

    # calculate number of batches
    batches: list = get_batches_from_directory(directory=dir_dict.get(source_data_key),
                                               batches=None if batches in [[], ['']] else batches, file_name=r'^person_INNER_lookup_join',
                                               independent_sub_batches=independent_sub_batch)

    dirs_list: list = []

    kwargs_list: list = []

    for enc_id in eids:
        for race_correction in race_corrections:

            # create new intermediate directory
            stage_dir: str = os.path.join(dir_dict.get(intermediate_data_key), 'Phenotyping', enc_id, 'with_race_correction' if race_correction else 'without_race_correction_v{}'.format(version))

            _ensure_folder_dict(folder_dict={'filtered_encounters': None,
                                             'filtered_labs': None,
                                             'filtered_diagnosis': None,
                                             'filtered_procedure': None,
                                             'filtered_dialysis': None,
                                             'encounter_admin_flags': None,
                                             'encounter_egfr_flags': None,
                                             'ckd_row_egfr': None,
                                             'encounter_creatinine_parameters': None,
                                             'encounter_ckd': None,
                                             'dialysis_time': None,
                                             'encounter_aki': None,
                                             'aki_trajectory': None},
                                root_dir=stage_dir, non_unique_base_folders=[])

            dirs_list.append(stage_dir)

            for batch in batches:

                kwargs_list.append({'in_dir': dir_dict.get('source_data'),
                                    'inmd_dir': stage_dir,
                                    'out_dir': dir_dict.get(out_data_key),
                                    'race_correction': race_correction,
                                    'version': version,
                                    'eid': enc_id,
                                    'pid': pid,
                                    'out_begin': project_name,
                                    'var_file_linkage_fp': dir_dict.get(var_file_link_key),
                                    'independent_sub_batch': independent_sub_batch,
                                    'display': False,
                                    'log_name': f'AKI_CKD_{enc_id}_race_correction_{race_correction}_{batch}',
                                    'log_dir': None,
                                    'batch': batch})

    # run main AKI_CKD logic
    run_function_in_parallel_v2(generate_CKD_AKI,
                                kwargs_list=kwargs_list,
                                max_workers=max_workers,
                                update_interval=10,
                                disp_updates=True,
                                log_name='AKI_CKD_Phenotyping',
                                list_running_futures=True,
                                debug=serial)

    # merge results from batches
    p9_kwargs: list = []
    for directory in dirs_list:
        for b in batches:
            if not os.path.exists(os.path.join(directory, f'aki_ckd_p09_complete_{b}')):
                p9_kwargs.append({'inmd_dir': directory,
                                  'out_dir': dir_dict.get(out_data_key),
                                  'eid': os.path.basename(os.path.dirname(directory)),
                                  'pid': pid,
                                  'batch': b,
                                  'out_prefix': f'{os.path.basename(os.path.dirname(directory))}_{os.path.basename(directory)}',
                                  'pattern': [r'_{}\.csv'.format(b) if (independent_sub_batch or ('chunk' in b)) else r'_{}_[0-9]+\.csv'.format(b)],
                                  'log_name': f'AKI_CKD_{os.path.basename(os.path.dirname(directory))}_batch_{b}',
                                  'display': False})
    if len(p9_kwargs) > 0:
        run_function_in_parallel_v2(p09_merge_outputfile,
                                    kwargs_list=p9_kwargs,
                                    max_workers=max_workers,
                                    update_interval=10,
                                    disp_updates=True,
                                    log_name='AKI_CKD_Phenotyping',
                                    list_running_futures=True,
                                    debug=serial)

    check_complete(dir_dict=dir_dict.copy(),
                   batch_list=None if batches in [[], ['']] else batches,
                   search_clean=False, phenotyping_flag=True,
                   dest_dir_key=out_data_key,
                   split_into_n_batches=dir_dict.get('split_into_n_batches', None),
                   files_list=[f'{os.path.basename(os.path.dirname(x))}_{os.path.basename(x)}' for x in dirs_list],
                   raise_exception=True, source_dir_key='source_data',
                   pre_run_check=False, essential_file_name=r'^person_INNER_lookup_join')

    open(success_fp, 'a').close()


def generate_CKD_AKI(in_dir: str,
                     inmd_dir: str,
                     out_dir: str,
                     race_correction: bool,
                     version: int,
                     eid: str,
                     pid: str,
                     out_begin: str,
                     var_file_linkage_fp: str,
                     batch: int,
                     independent_sub_batch: bool,
                     **logging_kwargs):
    """
    Coordinate excution of the CKD and AKI phenotyping codes and output the CKD and AKI result for a patient batch.

    Parameters
    ----------
    in_dir : str
        Source Folder.
    inmd_dir : str
        Temporary intermediate folder.
    out_dir : str
        Output folder.
    race_correction : bool
        Whether race correction in AKI_EPI formulat should be applied.
    version : int
        Code version. Supports 1 or 2.
    eid : str
        Encounter id variable.
    pid : str
        patient id variable.
    out_begin : str
        prefix of output file.
    var_file_linkage_fp : str
        file path to variable specification document
    batch : int
        Batch ID.
    independent_sub_batch : bool
        Whether sub_batches contain independet or randomly assigned patients.
    **logging_kwargs : TYPE
        Kwargs for logging.

    Returns
    -------
    None.

    """
    # debug_inputs(function=generate_CKD_AKI, kwargs=locals(), dump_fp=f'AKI_Phenotyping_{batch}.p')
    # raise Exception('stop here')
    success_fp: str = os.path.join(inmd_dir, f'phenotyping_{batch}_success')

    p1_success_fp: str = os.path.join(inmd_dir, f'phenotyping_p1_{batch}_success')
    p2_success_fp: str = os.path.join(inmd_dir, f'phenotyping_p2_{batch}_success')
    p3_success_fp: str = os.path.join(inmd_dir, f'phenotyping_p3_{batch}_success')
    p4_success_fp: str = os.path.join(inmd_dir, f'phenotyping_p4_{batch}_success')
    p5_success_fp: str = os.path.join(inmd_dir, f'phenotyping_p5_{batch}_success')
    p6_success_fp: str = os.path.join(inmd_dir, f'phenotyping_p6_{batch}_success')
    p7_success_fp: str = os.path.join(inmd_dir, f'phenotyping_p7_{batch}_success')
    p8_success_fp: str = os.path.join(inmd_dir, f'phenotyping_p8_{batch}_success')

    if not os.path.exists(success_fp):

        patterns: list = [r'_{}\.csv'.format(batch), r'\.csv'] if independent_sub_batch else [r'_{}_[0-9]+\.csv'.format(batch), r'_{}\.csv'.format(batch), r'\.csv']

        if not os.path.exists(p1_success_fp):
            p01_load_filter_file(in_dir=in_dir, inmd_dir=inmd_dir, var_file_linkage_fp=var_file_linkage_fp, batch=batch, eid=eid, pid=pid,
                                 patterns=patterns, **logging_kwargs)
            open(p1_success_fp, 'a').close()

        if not os.path.exists(p2_success_fp):
            p02_create_admin_flags(inmd_dir=inmd_dir, batch=batch, eid=eid, pid=pid, **logging_kwargs)
            open(p2_success_fp, 'a').close()

        if not os.path.exists(p3_success_fp):
            p03_add_prev_creatinine_and_egfr_flags(inmd_dir=inmd_dir, batch=batch,
                                                   race_correction=race_correction, version=version,
                                                   eid=eid, pid=pid, **logging_kwargs)
            open(p3_success_fp, 'a').close()

        if not os.path.exists(p4_success_fp):
            p04_create_creatinine_parameters(inmd_dir=inmd_dir, batch=batch,
                                             eid=eid, pid=pid, **logging_kwargs)
            open(p4_success_fp, 'a').close()

        if not os.path.exists(p5_success_fp):
            p05_ckd_class_and_egfr_staging(inmd_dir=inmd_dir, batch=batch, race_correction=race_correction, version=version,
                                           pid=pid, eid=eid, **logging_kwargs)
            open(p5_success_fp, 'a').close()

        if not os.path.exists(p6_success_fp):
            p06_generate_rrt_summary(inmd_dir=inmd_dir, batch=batch, eid=eid,
                                     pid=pid, **logging_kwargs)
            open(p6_success_fp, 'a').close()

        if not os.path.exists(p7_success_fp):
            p07_generate_aki(inmd_dir=inmd_dir, batch=batch, race_correction=race_correction, version=version,
                             eid=eid, pid=pid, **logging_kwargs)
            open(p7_success_fp, 'a').close()

        if not os.path.exists(p8_success_fp):
            p08_generateTrajectory(inmd_dir=inmd_dir, in_dir=in_dir, batch=batch, eid=eid,
                                   pid=pid, var_file_linkage_fp=var_file_linkage_fp, patterns=patterns, **logging_kwargs)
            open(p8_success_fp, 'a').close()

        open(success_fp, 'a').close()


if __name__ == '__main__':
    pass
