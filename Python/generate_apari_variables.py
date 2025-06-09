# -*- coding: utf-8 -*-
"""
Created on Fri Jun 16 09:09:19 2023

@author: ruppert20
"""
import os
from typing import Dict, Union, List
import pandas as pd
import sqlalchemy
from sqlalchemy.engine.base import Engine
from .Outcome_Generation.Python.outcome_generation.outcome_generation_v3 import generate_outcomes_v3
from .SOFA.Python.omop_sofa import calculate_SOFA
from .Variable_Generation.Python.variable_generation_v2 import omop_variable_generation
from .Utilities.Database.connect_to_database import omop_engine_bundle
from .Utilities.FileHandling.variable_specification_utilities import load_variables_from_var_spec
from .Utilities.PreProcessing.time_intervals import resolve_overlaps, condense_overlapping_segments
from .Utilities.FileHandling.io import check_load_df, save_data
from .Utilities.Logging.log_messages import log_print_email_message as logm
from .Utilities.General.func_utils import debug_inputs
from .Utilities.PreProcessing.time_intervals import condense_in_parallel


def generate_APARI_variables_part1(dir_dict: Dict[str, any],
                                   project_name: str,
                                   subject_id_mode: str,
                                   engine_bundle: omop_engine_bundle,
                                   default_facility_zip: Union[str, int],
                                   cohort_id: Union[str, int],
                                   subset_id: Union[str, int, None] = None,
                                   pid: str = 'person_id',
                                   eid: str = 'visit_occurrence_id',
                                   source_data_key: str = 'source_data',
                                   gen_data_key: str = 'generated_data',
                                   intermediate_data_key: str = 'intermediate_data',
                                   var_file_link_key: str = 'variable_file_link',
                                   **logging_kwargs):

    batch_id: str = f'{cohort_id}_chunk_{subset_id}' if pd.notnull(subset_id) else cohort_id

    success_fp: str = os.path.join(dir_dict.get('status_files'), f'APARI_variable_generation_part_1{batch_id}_success_')

    if os.path.exists(success_fp):
        return

    if not isinstance(engine_bundle.engine, Engine):
        engine_bundle: omop_engine_bundle = omop_engine_bundle(engine=sqlalchemy.create_engine(engine_bundle.engine, fast_executemany=True, execution_options={"stream_results": True}),
                                                               database=engine_bundle.database,
                                                               vocab_schema=engine_bundle.vocab_schema,
                                                               data_schema=engine_bundle.data_schema,
                                                               lookup_schema=engine_bundle.lookup_schema,
                                                               results_schema=engine_bundle.results_schema,
                                                               operational_schema=engine_bundle.operational_schema,
                                                               database_update_table=engine_bundle.database_update_table,
                                                               lookup_table=engine_bundle.lookup_table,
                                                               drug_lookup_table=engine_bundle.drug_lookup_table)

    # run variable generation
    omop_variable_generation(dir_dict=dir_dict,
                             project_name=project_name,
                             data_source_key=source_data_key,
                             default_facility_zip=default_facility_zip,
                             aki_phenotype_data_key=gen_data_key,
                             var_file_linkage_fp=var_file_link_key,
                             batch_id=batch_id,
                             mode='surgery',
                             pid=pid,
                             eid=eid,
                             subject_id_type=subject_id_mode,
                             exact_subject_mode=True,
                             labs=['glucose_post', 'anion_gap', 'troponin_i', 'bun', 'base_excess', 'po2a', 'pco2a', 'bun_ur_24h', 'lymphocytes_per', 'cacr_r_ur_24h', 'upcr',
                                   'rbc_ur', 'apr_ur_24h', 'apr_ur', 'lactate', 'hco3', 'creatinine_ur_mol', 'hgb', 'microalbumin_24h_t', 'microalbumin_ur', 'umacr', 'glucose_ur',
                                   'alt', 'albumin', 'albumin_ur', 'albumin_ur_24h_t', 'hgb_a1c', 'calcium', 'calcium_ur', 'calcium_ionized', 'calcium_ur_24h', 'ast', 'pha', 'base_deficit',
                                   'bilirubin_dir', 'bilirubin_tot', 'crp', 'co2', 'carboxyhem', 'rbc_ur_v', 'uap_cat', 'o2sata', 'hct', 'hgb_electrophoresis', 'creatinine_ur_24h', 'chloride',
                                                    'chloride_ur', 'chloride_ur_24h_t', 'rdw', 'chloride_ur_24h', 'potassium_ur_24h', 'sodium_ur_24h', 'creatinine', 'creatinine_ur', 'creatinine_ur_24h_t',
                                                    'glucose', 'glucose_t', 'neutrophils_per', 'methem', 'eosinophils', 'eosinophils_per', 'rbc', 'wbc', 'lymphocytes', 'monocytes_per', 'neutrophils',
                                                    'neutrophils_band', 'neutrophils_per_band', 'plt', 'ph_ur', 'potassium', 'potassium_ur', 'potassium_ur_24h_t', 'p24', 'sodium', 'sodium_ur', 'sodium_ur_24h_t',
                                                    'sg', 'microalbumin_24h', 'basophils_per', 'esr', 'mcv', 'bnp', 'bun_ur', 'bun_ur_24h_t', 'uncr', 'mpv', 'rbc_ur_pres', 'wbc_ur_pres', 'scr_ur_24h', 'p_panel',
                                                    'inr', 'chpd_ur_24h', 'sodium_u_24hr', 'wbc_ur_sedim_l', 'wbc_ur_sedim', 'calcium_ionized_corr', 'bilirubin_tot_ur_pres', 'wbc_ur', 'potassium_ur_24h_mt',
                                                    'hgb_ur', 'troponin_t', 'calcium_ur_24h_t', 'bilirubin_tot_ur', 'basophils', 'monocytes', 'mch', 'mchc', 'uacr', 'cacr_r_ur'],
                             out_path=os.path.join(dir_dict.get(gen_data_key), f'all_surgical_variables_{batch_id}.csv'),
                             **logging_kwargs)

    # run outcome generation
    generate_outcomes_v3(dir_dict=dir_dict,
                         var_file_linkage_fp=var_file_link_key,
                         source_dir_key=source_data_key,
                         aki_phenotype_dir_key=gen_data_key,
                         dest_dir_key=gen_data_key,
                         visit_detail_type='surgery',
                         batch_id=batch_id,
                         **logging_kwargs)

    # run sofa score calculation
    calculate_SOFA(engine=engine_bundle,
                   cohort_id=cohort_id,
                   keep_zeros=True,
                   subset_id=subset_id,
                   sofa_start='visit_start_datetime',
                   sofa_stop='visit_detail_end_datetime',
                   local_mode=True,
                   dir_dict=dir_dict,
                   aki_phenotype_data_key=gen_data_key,
                   data_source_key=source_data_key,
                   var_file_linkage_fp=var_file_link_key,
                   save_folder_path=dir_dict.get(gen_data_key),
                   sofa_versions=['eSOFA'],
                   sofa_frequency='1 h',
                   **logging_kwargs)

    open(success_fp, 'a').close()


def generate_APARI_variables_part2(dir_dict: Dict[str, any],
                                   project_name: str,
                                   cohort_id: Union[str, int],
                                   subset_id: Union[str, int, None] = None,
                                   pid: str = 'person_id',
                                   eid: str = 'visit_occurrence_id',
                                   source_data_key: str = 'source_data',
                                   gen_data_key: str = 'generated_data',
                                   intermediate_data_key: str = 'intermediate_data',
                                   var_file_link_key: str = 'variable_file_link',
                                   **logging_kwargs):
    batch_id: str = f'{cohort_id}_chunk_{subset_id}' if pd.notnull(subset_id) else cohort_id

    success_fp: str = os.path.join(dir_dict.get('status_files'), f'APARI_variable_generation_part_2{batch_id}_success_')

    if os.path.exists(success_fp):
        return
    ### generate station variables ###
    patterns: List[str] = [r'_{}\.csv'.format(batch_id)] if isinstance(subset_id, (int, str)) else [r'_{}\.csv'.format(batch_id), r'_{}_chunk_[0-9]+\.csv'.format(batch_id), r'_{}_[0-9]+\.csv'.format(batch_id), r'_[0-9]+_chunk_[0-9]+\.csv', r'_[0-9]+_[0-9]+\.csv', r'_[0-9]+\.csv', r'\.csv']

    # load station data
    hospital_locs_df: pd.DataFrame = load_variables_from_var_spec(variables_columns=['visit_detail_start_datetime', 'visit_detail_end_datetime', 'icu', 'surgery', 'operating_room', 'ward',
                                                                                     'procedure_suite', 'emergency_department', 'or_holding_unit', 'post_anesthesia_care_unit'],
                                                                  var_spec_key=var_file_link_key,
                                                                  dir_dict=dir_dict,
                                                                  filter_variables=False,
                                                                  data_source_key=source_data_key,
                                                                  cdm_tables=['visit_detail'],
                                                                  paritition_filter='xxxisnullxxx',
                                                                  project='APARI',
                                                                  id_vars=['subject_id'],
                                                                  mute_duplicate_var_warnings=True,
                                                                  desired_types={**{x: 'datetime' for x in ['visit_detail_start_datetime', 'visit_detail_end_datetime']},
                                                                                 **{x: 'sparse_int' for x in [pid, eid, 'value_as_concept_id', 'subject_id']}},
                                                                  patterns=patterns,
                                                                  allow_empty_files=True, regex=True, dtype=None, ds_type='pandas', **logging_kwargs)\
        .drop(columns=['visit_detail_concept_id'])\
        .dropna(subset=['visit_detail_end_datetime'])

    # load surgery data
    surgery_data: pd.DataFrame = check_load_df('all_surgical_variables', directory=dir_dict.get(gen_data_key),
                                               patterns=patterns,
                                               usecols=['subject_id', 'surgery_start_datetime', 'surgery_end_datetime'],
                                               desired_types={**{x: 'sparse_int' for x in ['subject_id']},
                                                              **{x: 'datetime' for x in ['surgery_end_datetime', 'surgery_start_datetime']}})
    

    # #debug
    # print("=== Loaded surgery_data ===")
    # print(surgery_data.head(10))
    # print("Shape:", surgery_data.shape)
    # print(surgery_data['subject_id'].dtype)
    # print("Debug subject rows:")   
    # debug_subjects = ['77128963','77263983','77069091']
    # print(surgery_data[surgery_data['subject_id'].isin(debug_subjects)])

# set priority
    hospital_locs_df['priority'] = hospital_locs_df.variable_name.apply(lambda x: 1 if x in ['operating_room', 'surgery'] else
                                                                        2 if x in ['procedure_suite'] else
                                                                        3 if x in ['icu'] else
                                                                        4 if x in ['or_holding_unit', 'post_anesthesia_care_unit'] else
                                                                        5 if x in ['emergency_department'] else
                                                                        6)

#     logm('Verifying integretiy of visit_detail table and resolving any overlaps', **logging_kwargs)
    
#     # #debug
#     # print(hospital_locs_df['subject_id'].dtype)
#     # print("Raw hospital_locs_df before condense for debug subjects:")
#     # print(hospital_locs_df[hospital_locs_df['subject_id'].isin(debug_subjects)].sort_values(['subject_id', 'visit_detail_start_datetime']))

#     hospital_locs_df: pd.DataFrame = condense_in_parallel(df=hospital_locs_df.drop_duplicates().copy(deep=True),
#                                                           grouping_columns=['subject_id'],
#                                                           start_col='visit_detail_start_datetime',
#                                                           end_col='visit_detail_end_datetime',
#                                                           gap_tolerance_hours=0,
#                                                           custom_overlap_function=resolve_overlaps,
#                                                           priority_col='priority',
#                                                           granularity='1 min',
#                                                           max_len='30 days',
#                                                           chunk_size=50,
#                                                           max_workers=12,
#                                                           serial=False,
#                                                           **logging_kwargs)

    if 'subject_id' in hospital_locs_df.columns:
        hospital_locs_df.reset_index(drop=True, inplace=True)
    else:
        hospital_locs_df.reset_index(drop=False, inplace=True)
#         #debug
#     # print(hospital_locs_df['subject_id'].dtype)
#     # print("Condensed hospital_locs_df for debug subjects:")
#     # print(hospital_locs_df[hospital_locs_df['subject_id'].isin(debug_subjects)].sort_values(['subject_id', 'visit_detail_start_datetime']))



# # hospital_locs_df.variable_name.replace({x: 'ward' for x in ['emergency_department']}, inplace=True)
#     if surgery_data is None or surgery_data.empty:
#         logm(f"surgery_data is empty for batch {batch_id}. Saving an empty station_vars_{batch_id}.csv with correct columns.", **logging_kwargs)
    
#         # Define column names based on expected structure
#         expected_columns = ['subject_id', 'pre_op_station', 'post_op_station',
#                             'time_to_pacu_h', 'had_postop_pacu_admit', 
#                             'time_to_ward_h', 'had_postop_ward_admit',
#                             'time_to_icu_h', 'had_postop_icu_admit', 'icu_to_ward']
    
#         # Create an empty DataFrame with the expected column names
#         station_vars_df = pd.DataFrame(columns=expected_columns)
    
#         # Save the empty DataFrame
#         save_data(df=station_vars_df,
#                   out_path=os.path.join(dir_dict.get(gen_data_key), f'station_vars_{batch_id}.csv'))
  
#     else:   
    pre_op_station = pd.merge_asof(left=surgery_data[['subject_id', 'surgery_start_datetime']].sort_values('surgery_start_datetime', ascending=True),
                                   right=hospital_locs_df[['subject_id', 'visit_detail_start_datetime', 'variable_name']]
                                   .query("~variable_name.isin(['operating_room', 'surgery', 'or_holding_unit', 'post_anesthesia_care_unit', 'procedure_suite'])", engine='python')
                                   .sort_values('visit_detail_start_datetime', ascending=True),
                                   by='subject_id',
                                   left_on='surgery_start_datetime',
                                   right_on='visit_detail_start_datetime',
                                   allow_exact_matches=False,
                                   direction='backward')\
        .dropna(subset=['visit_detail_start_datetime'])\
        .drop(columns=['visit_detail_start_datetime', 'surgery_start_datetime'])\
        .rename(columns={'variable_name': 'pre_op_station'})

    pre_op_station.pre_op_station = pre_op_station.pre_op_station.apply(lambda x: 'unknown' if pd.isnull(x) else x if x in ['emergency_department', 'ward', 'icu'] else 'other')
    #debug
    # print(pre_op_station['subject_id'].dtype)
    # print("Pre-op station result for debug subjects:")
    # print(pre_op_station[pre_op_station['subject_id'].isin(debug_subjects)])


    post_op_station = pd.merge_asof(left=surgery_data[['subject_id', 'surgery_end_datetime']].sort_values('surgery_end_datetime', ascending=True),
                                    right=hospital_locs_df[['subject_id', 'visit_detail_start_datetime', 'variable_name']]
                                    .query("~variable_name.isin(['operating_room', 'surgery', 'or_holding_unit', 'procedure_suite'])", engine='python')
                                    .sort_values('visit_detail_start_datetime', ascending=True),
                                    by='subject_id',
                                    left_on='surgery_end_datetime',
                                    right_on='visit_detail_start_datetime',
                                    allow_exact_matches=False,
                                    direction='forward')\
        .dropna(subset=['visit_detail_start_datetime'])\
        .drop(columns=['visit_detail_start_datetime', 'surgery_end_datetime'])\
        .rename(columns={'variable_name': 'post_op_station'})
        
    first_post_op_pacu_station= pd.merge_asof(left=surgery_data[['subject_id', 'surgery_end_datetime']].sort_values('surgery_end_datetime', ascending=True),
                                               right=hospital_locs_df[['subject_id', 'visit_detail_start_datetime', 'variable_name']]
                                               .query("variable_name.isin(['post_anesthesia_care_unit'])", engine='python')
                                               .sort_values('visit_detail_start_datetime', ascending=True),
                                               by='subject_id',
                                               left_on='surgery_end_datetime',
                                               right_on='visit_detail_start_datetime',
                                               allow_exact_matches=False,
                                               direction='forward')\
        .dropna(subset=['visit_detail_start_datetime'])
    first_post_op_pacu_station['time_to_pacu_h'] = (first_post_op_pacu_station.visit_detail_start_datetime - first_post_op_pacu_station.surgery_end_datetime).astype('timedelta64[h]')
    first_post_op_pacu_station['had_postop_pacu_admit'] = '1' if first_post_op_pacu_station.shape[0] > 0 else '0'
    first_post_op_pacu_station.drop(columns=['variable_name', 'surgery_end_datetime', 'visit_detail_start_datetime'], inplace=True)

   

    first_post_op_ward_station = pd.merge_asof(left=surgery_data[['subject_id', 'surgery_end_datetime']].sort_values('surgery_end_datetime', ascending=True),
                                               right=hospital_locs_df[['subject_id', 'visit_detail_start_datetime', 'variable_name']]
                                               .query("variable_name.isin(['ward'])", engine='python')
                                               .sort_values('visit_detail_start_datetime', ascending=True),
                                               by='subject_id',
                                               left_on='surgery_end_datetime',
                                               right_on='visit_detail_start_datetime',
                                               allow_exact_matches=False,
                                               direction='forward')\
        .dropna(subset=['visit_detail_start_datetime'])

    first_post_op_ward_station['time_to_ward_h'] = (first_post_op_ward_station.visit_detail_start_datetime - first_post_op_ward_station.surgery_end_datetime).astype('timedelta64[h]')
    first_post_op_ward_station['had_postop_ward_admit'] = '1' if first_post_op_ward_station.shape[0] > 0 else '0'
    first_post_op_ward_station.drop(columns=['variable_name', 'surgery_end_datetime', 'visit_detail_start_datetime'], inplace=True)

    first_post_op_icu_station = pd.merge_asof(left=surgery_data[['subject_id', 'surgery_end_datetime']].sort_values('surgery_end_datetime', ascending=True),
                                              right=hospital_locs_df[['subject_id', 'visit_detail_start_datetime', 'variable_name']]
                                              .query("variable_name.isin(['icu'])", engine='python')
                                              .sort_values('visit_detail_start_datetime', ascending=True),
                                              by='subject_id',
                                              left_on='surgery_end_datetime',
                                              right_on='visit_detail_start_datetime',
                                              allow_exact_matches=False,
                                              direction='forward')\
        .dropna(subset=['visit_detail_start_datetime'])

    first_post_op_icu_station['time_to_icu_h'] = (first_post_op_icu_station.visit_detail_start_datetime - first_post_op_icu_station.surgery_end_datetime).astype('timedelta64[h]')
    first_post_op_icu_station['had_postop_icu_admit'] = '1' if first_post_op_icu_station.shape[0] > 0 else '0'
    first_post_op_icu_station.drop(columns=['variable_name', 'surgery_end_datetime', 'visit_detail_start_datetime'], inplace=True)

    station_vars_df: pd.DataFrame = post_op_station.merge(pre_op_station, how='outer', on='subject_id')\
        .merge(first_post_op_pacu_station, how='left', on='subject_id')\
        .merge(first_post_op_icu_station, how='left', on='subject_id')\
        .merge(first_post_op_ward_station, how='left', on='subject_id')
        
    station_vars_df['icu_to_ward'] = ((station_vars_df.post_op_station == 'icu') & (station_vars_df.had_postop_ward_admit == '1')).astype(int)

    save_data(df=station_vars_df,
              out_path=os.path.join(dir_dict.get(gen_data_key), f'station_vars_{batch_id}.csv'))

    # postop mv
    postop_mv: pd.DataFrame = pd.concat([load_variables_from_var_spec(variables_columns=['mechanical_ventilation', 'device_exposure_start_datetime', 'device_exposure_end_datetime'],
                                                                      var_spec_key=var_file_link_key,
                                                                      dir_dict=dir_dict,
                                                                      id_vars=['subject_id'],
                                                                      filter_variables=True,
                                                                      data_source_key=source_data_key,
                                                                      cdm_tables=['device_exposure'],
                                                                      project='APARI',
                                                                      mute_duplicate_var_warnings=True,
                                                                      desired_types={**{x: 'datetime' for x in ['device_exposure_start_datetime', 'device_exposure_end_datetime']},
                                                                                     **{x: 'sparse_int' for x in [pid, eid, 'value_as_concept_id', 'subject_id', 'visit_detail_id']}},
                                                                      patterns=patterns,
                                                                      allow_empty_files=True, regex=True, dtype=None, ds_type='pandas', **logging_kwargs)
                                         .drop(columns=['device_concept_id']),
                                         hospital_locs_df.query("variable_name.isin(['operating_room', 'surgery', 'post_anesthesia_care_unit', 'procedure_suite'])", engine='python')
                                         .copy(deep=True).rename(columns={x: x.replace('visit_detail', 'device_exposure') for x in ['visit_detail_start_datetime', 'visit_detail_end_datetime']})],
                                        axis=0, ignore_index=True)

    # assign mv as priority 999
    postop_mv.priority.fillna(999, inplace=True)
    
    if postop_mv.shape[0] > 0:
        postop_mv.loc[:, 'device_exposure_end_datetime'] = postop_mv[['device_exposure_end_datetime', 'device_exposure_start_datetime']]\
            .apply(lambda row: row.device_exposure_end_datetime if ((row.device_exposure_end_datetime - row.device_exposure_start_datetime) <= pd.to_timedelta('30 days'))
                   else (row.device_exposure_start_datetime + pd.to_timedelta('30 days')), axis=1).values

    # keep only instances of mv outside of operations, procedures, and post_procedure holding areas
    logm('Verifying integretiy of mechanical ventilation data and resolving any overlaps', **logging_kwargs)
    postop_mv = postop_mv\
        .drop_duplicates()\
        .groupby('subject_id')\
        .apply(condense_overlapping_segments,
               start_col='device_exposure_start_datetime',
               end_col='device_exposure_end_datetime',
               gap_tolerance_hours=0,
               custom_overlap_function=resolve_overlaps,
               priority_col='priority',
               grouping_columns=['subject_id'])\
        .query('variable_name.isin(["mechanical_ventilation"])', engine='python')\
        .drop(columns=['priority'])

    if 'subject_id' in postop_mv.columns:
        postop_mv.reset_index(drop=True, inplace=True)
    else:
        postop_mv.reset_index(drop=False, inplace=True)

    postop_mv['postop_mv_duration'] = (postop_mv.device_exposure_end_datetime - postop_mv.device_exposure_start_datetime)

    postop_mv['postop_mv'] = '1' if postop_mv.shape[0] > 0 else '0'

    save_data(df=postop_mv.groupby('subject_id').agg({'postop_mv': 'max',
                                                     'postop_mv_duration': 'sum'})
              .reset_index(drop=False),
              out_path=os.path.join(dir_dict.get(gen_data_key), f'postop_mv_vars_{batch_id}.csv'),
              **logging_kwargs)

    open(success_fp, 'a').close()
