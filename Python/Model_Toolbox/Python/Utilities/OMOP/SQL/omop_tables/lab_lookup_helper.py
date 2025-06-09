# -*- coding: utf-8 -*-
"""
Created on Tue Jun 13 15:29:07 2023

@author: ruppert20
"""
from typing import List, Union
import pandas as pd
from ...Database.connect_to_database import omop_engine_bundle
from ...Database.database_updates import log_database_update
from tqdm import tqdm


def lab_lookup_helper(lookup_df: pd.DataFrame, engine_bundle: omop_engine_bundle, lab_list: Union[List[str], None] = ['albumin_ur_24h_t',
                                                                                                                      'basophils_per',
                                                                                                                      'bilirubin_tot_ur',
                                                                                                                      'bilirubin_tot_ur_pres',
                                                                                                                      'bnp',
                                                                                                                      'bun_ur',
                                                                                                                      'bun_ur_24h',
                                                                                                                      'bun_ur_24h_t',
                                                                                                                      'cacr_r_ur',
                                                                                                                      'cacr_r_ur_24h',
                                                                                                                      'calcium_ionized',
                                                                                                                      'calcium_ionized_corr',
                                                                                                                      'calcium_ur',
                                                                                                                      'calcium_ur_24h',
                                                                                                                      'calcium_ur_24h_t',
                                                                                                                      'chloride',
                                                                                                                      'chloride_ur',
                                                                                                                      'chloride_ur_24h',
                                                                                                                      'chloride_ur_24h_t',
                                                                                                                      'chpd_ur_24h',
                                                                                                                      'creatinine_ur',
                                                                                                                      'creatinine_ur_24h',
                                                                                                                      'creatinine_ur_24h_t',
                                                                                                                      'creatinine_ur_mol',
                                                                                                                      'eosinophils_per',
                                                                                                                      'glucose_post',
                                                                                                                      'glucose_t',
                                                                                                                      'hgb_electrophoresis',
                                                                                                                      'microalbumin_24h',
                                                                                                                      'microalbumin_24h_t',
                                                                                                                      'monocytes_per',
                                                                                                                      'neutrophils_band',
                                                                                                                      'neutrophils_per',
                                                                                                                      'p_panel',
                                                                                                                      'p24',
                                                                                                                      'ph_ur',
                                                                                                                      'potassium_ur',
                                                                                                                      'potassium_ur_24h',
                                                                                                                      'potassium_ur_24h_mt',
                                                                                                                      'potassium_ur_24h_t',
                                                                                                                      'rbc_ur_pres',
                                                                                                                      'rbc_ur_v',
                                                                                                                      'scr_ur_24h',
                                                                                                                      'sg',
                                                                                                                      'sodium_u_24hr',
                                                                                                                      'sodium_ur',
                                                                                                                      'sodium_ur_24h',
                                                                                                                      'sodium_ur_24h_t',
                                                                                                                      'upcr',
                                                                                                                      'wbc_ur',
                                                                                                                      'wbc_ur_pres',
                                                                                                                      'wbc_ur_sedim',
                                                                                                                      'wbc_ur_sedim_l'
                                                                                                                      ]):

    lookup_df.lab_type = lookup_df.lab_type.str.strip().str.lower().str.replace(' ', '_')

    currently_available: pd.DataFrame = pd.read_sql(f"SELECT DISTINCT variable_name from {engine_bundle.lookup_schema}.{engine_bundle.lookup_table} WHERE concept_class_id = 'lab test';", con=engine_bundle.engine)

    for lab_test in tqdm(lookup_df.lab_type[lookup_df.lab_type.isin(lab_list) if isinstance(lab_list, list) else lookup_df.lab_type.notnull()].dropna().unique(), desc='updating lab lookup information'):
        if lab_test not in currently_available.variable_name:
            log_database_update(file_name='lab_lookup_utility',
                                batch='N/A',
                                dest_table=engine_bundle.lookup_table, note=f'Add {lab_test} to lookup table',
                                engine=engine_bundle,
                                github_release='N/A',
                                execute_query=True,
                                crud_query=f'''INSERT INTO {engine_bundle.lookup_schema}.{engine_bundle.lookup_table} SELECT 
                                                [concept_id],
                                                [concept_code],
                                                [concept_name],
                                                [vocabulary_id],
                                                concept_class_id,
                                                [domain_id],
                                                NULL [ancestor_concept_id],
                                                '{lab_test}' as variable_name,
                                                'Need to make more exaustive list, uses loinc code only' variable_desc
                                            FROM 
                                                VOCAB.CONCEPT C
                                            WHERE
                                                vocabulary_id = 'LOINC'
                                                AND
                                                concept_code IN ('{"','".join(lookup_df.loc[lookup_df.lab_type == lab_test, 'stamped_and_inferred_loinc_code'])}')''')
