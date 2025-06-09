# -*- coding: utf-8 -*-
"""
Created on Mon Nov 23 21:42:03 2020.

@author: renyuanfang
"""
import os
from datetime import datetime

import Phenotyping.p01_load_filter_files as p1
import Phenotyping.p02_create_AdminFlag as p2
import Phenotyping.p03_create_prev_CreatinineFlag_row_egfr as p3
import Phenotyping.p04_creatinine_parameters as p4
import Phenotyping.p05_ckd_class_egfr_staging as p5
import Phenotyping.p06_dialysis as p6
import Phenotyping.p07_aki as p7
import Phenotyping.p08_trajectory as p8
import Phenotyping.p09_merge_result as p9


def p00_generate_CKD_AKI(in_dir: str,
                         inmd_dir: str,
                         out_dir: str,
                         race_correction: bool,
                         eid: str,
                         pid: str,
                         out_begin: str,
                         enc_name: str,
                         labs_name: str,
                         diagnosis_name: str,
                         procedure_name: str,
                         crrt_name: str,
                         dialysis_name: str,
                         ssdi_name: str,
                         batch: int):
    """
    Execute the CKD and AKI phenotyping codes and output the CKD and AKI result for each patient batch by batch.

    1. Creates success file path for each step if not exists
    2. Execute code step by step

    Parameters
    ----------
    in_dir : str
        Source data directory.
    inmd_dir : str
        Intermediate data directory.
    out_dir : str
        Output files directory.
    race_correction : bool
        Indicator for applying race correction functions.
    eid : str
        Encounter deidentified id.
    pid : str
        Patient deidentified id.
    enc_name : str
        Encounter file name.
    labs_name : str
        Labs file name.
    diagnosis_name : str
        Diagnosis file name.
    procedure_name : str
        Procedure file name.
    crrt_name : str
        CRRT file name.
    dialysis_name : str
        Dialysis file name.
    ssdi_name : str
        Social security death index file name.
    batch : int
        Input batch number.

    Returns
    -------
    None.

    Notes
    -----
    Flow chart for this function:
        .. image:: flow_charts/AKI_CKD/generate_CKD_AKI_fun.png
    """
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

        if not os.path.exists(p1_success_fp):
            p1.p01_load_filter_file(in_dir=in_dir, inmd_dir=inmd_dir, enc_name=enc_name, labs_name=labs_name, diagnosis_name=diagnosis_name,
                                    procedure_name=procedure_name, crrt_name=crrt_name, dialysis_name=dialysis_name, batch=batch, eid=eid, pid=pid)
            open(p1_success_fp, 'a').close()

        if not os.path.exists(p2_success_fp):
            p2.p02_create_admin_flags(inmd_dir=inmd_dir, batch=batch, eid=eid, pid=pid)
            open(p2_success_fp, 'a').close()

        if not os.path.exists(p3_success_fp):
            p3.p03_add_prev_creatinine_and_egfr_flags(inmd_dir=inmd_dir, batch=batch,
                                                      race_correction=race_correction, eid=eid, pid=pid)
            open(p3_success_fp, 'a').close()

        if not os.path.exists(p4_success_fp):
            p4.p04_create_creatinine_parameters(inmd_dir=inmd_dir, batch=batch, eid=eid, pid=pid)
            open(p4_success_fp, 'a').close()

        if not os.path.exists(p5_success_fp):
            p5.p05_ckd_class_and_egfr_staging(inmd_dir=inmd_dir, batch=batch, race_correction=race_correction, pid=pid, eid=eid)
            open(p5_success_fp, 'a').close()

        if not os.path.exists(p6_success_fp):
            p6.p06_generate_rrt_summary(inmd_dir=inmd_dir, batch=batch, eid=eid, pid=pid)
            open(p6_success_fp, 'a').close()

        if not os.path.exists(p7_success_fp):
            p7.p07_generate_aki(inmd_dir=inmd_dir, batch=batch, race_correction=race_correction, eid=eid, pid=pid)
            open(p7_success_fp, 'a').close()

        if not os.path.exists(p8_success_fp):
            p8.p08_generateTrajectory(inmd_dir=inmd_dir, in_dir=in_dir, batch=batch, eid=eid, pid=pid, ssdi_name=ssdi_name)
            open(p8_success_fp, 'a').close()
        
    if os.path.exists(p8_success_fp): 
        p9.p09_merge_outputfile(inmd_dir=inmd_dir,
                                out_dir=out_dir,
                                eid=eid,
                                pid=pid,
                                out_prefix=out_begin,
                                batch=batch)

        open(success_fp, 'a').close()


if __name__ == '__main__':
    # Beginning of the final output name
    in_dir = r''
    inmd_dir = r''
    out_dir = r''
    race_correction = False
    eid = 'merged_enc_id'
    pid = 'patient_deiden_id'
    out_begin = 'AKI_EPIC_2'
    enc_name = ''
    labs_name = ''
    diagnosis_name = ''
    procedure_name = ''
    crrt_name = ''
    dialysis_name = ''
    ssdi_name = ''
    batch = 0

    p00_generate_CKD_AKI(in_dir=in_dir,
                         inmd_dir=inmd_dir,
                         out_dir=out_dir,
                         race_correction=race_correction,
                         eid=eid,
                         pid=pid,
                         out_begin=out_begin,
                         enc_name=enc_name,
                         labs_name=labs_name,
                         diagnosis_name=diagnosis_name,
                         procedure_name=procedure_name,
                         crrt_name=crrt_name,
                         dialysis_name=dialysis_name,
                         ssdi_name=ssdi_name,
                         batch=batch)
