# -*- coding: utf-8 -*-
"""
Make APARI Dataset.

Created on Fri Jun 16 09:37:44 2023

@author: ruppert20
"""
import os
import pandas as pd
from typing import Dict, List, Union
from tqdm import tqdm
from .Utilities.FileHandling.variable_specification_utilities import load_variables_from_var_spec
from .Utilities.FileHandling.io import check_load_df, save_data
from .Utilities.PreProcessing.standardization_functions_config_helper import process_df_with_pre_processing_instructions
from .Utilities.PreProcessing.aggregation_functions import _mean_only
from .Utilities.PreProcessing.Standardized_data import build_dataset
from .Utilities.Logging.log_messages import log_print_email_message as logm
from .Utilities.General.func_utils import debug_inputs


def make_APARI_dataset(dir_dict: Dict[str, any],
                       project_name: str,
                       subject_id_type: str,
                       serial: bool,
                       pid: str = 'person_id',
                       eid: str = 'visit_occurrence_id',
                       source_data_key: str = 'source_data',
                       gen_data_key: str = 'generated_data',
                       intermediate_data_key: str = 'intermediate_data',
                       var_file_link_key: str = 'variable_file_link',
                       combine_dev_and_test_for_kfold: bool = True,
                       dset_name: str = 'APARI_v1.0.h5',
                       force_regenerate_dataset: bool = False,
                       **logging_kwargs):
    
    # debug_inputs(function=make_APARI_dataset, kwargs=locals(), dump_fp='make_dataset.pkl')
    # raise Exception('stop here')

    logging_kwargs['log_name'] = logging_kwargs.pop('log_name', '') + '_make_APARI_dataset'

    success_fp: str = os.path.join(dir_dict.get('status_files'), f'APARI_{dset_name}_dataset_generation_success_')

    patterns: List[str] = [r'_[0-9]+_chunk_[0-9]+\.csv',
                           r'_[0-9]+_[0-9]+\.csv', r'_[0-9]+\.csv', r'\.csv']

    if (not force_regenerate_dataset) and os.path.exists(success_fp):
        return

    # load variables from variable_generation
    var_l: list = ['subject_id', pid, eid, 'age', 'emergent', 'poa_cci', 'smoking_status', 'emergent_procedure', 'gender_concept_id', 'race_concept_id', 'payer_concept_id', 'poa_cci_aids', 'poa_cci_diabwc',
                   'poa_diabetes', 'poa_cci_pud', 'poa_icancer', 'poa_imcancer', 'poa_icvd', 'poa_ichf', 'poa_cci_dementia', 'poa_imi', 'sched_post_op_location',
                   'poa_cci_msld', 'poa_cci_hp', 'surgery_type', 'surgery_provider_id', 'primary_procedure', 'seen_in_ed_yn', 'surgery_start_datetime', 'poa_icpd',
                   'surgery_end_datetime', 'visit_start_datetime', 'visit_end_datetime', 'zip_9', 'procedure_urgency', 'admit_priority', 'surgical_service'
                   ]
    var_gen_vars_df: pd.DataFrame = check_load_df('all_surgical_variables', directory=dir_dict.get(gen_data_key),
                                                  usecols=var_l,
                                                  use_col_intersection=True,
                                                  desired_types={**{x: 'sparse_int' for x in [pid, eid, 'subject_id', 'zip_9', 'poa_cci_aids', 'poa_cci_diabwc', 'payer_concept_id',
                                                                                              'poa_diabetes', 'poa_cci_pud', 'poa_icancer', 'poa_imcancer', 'poa_icvd', 'poa_ichf',
                                                                                              'poa_cci_dementia', 'poa_imi', 'poa_cci_msld', 'poa_cci_hp', 'poa_cci', 'gender_concept_id',
                                                                                              'surgical_service', 'surgery_provider_id', 'primary_procedure', 'seen_in_ed_yn', 'procedure_urgency',
                                                                                              'sched_post_op_location', 'poa_icpd']},
                                                                 **{x: 'datetime' for x in ['surgery_end_datetime', 'surgery_start_datetime', 'visit_start_datetime', 'visit_end_datetime']}},
                                                  **logging_kwargs).merge (check_load_df('care_site', directory=dir_dict.get(source_data_key), patterns=patterns, **logging_kwargs), how='left', on='subject_id')\
        .merge (check_load_df('30_day_readmission', directory=dir_dict.get(source_data_key), patterns=patterns, **logging_kwargs), how='left', on='subject_id')\
        .merge (check_load_df('dnr_order', directory=dir_dict.get(source_data_key), patterns=patterns, **logging_kwargs), how='left', on='subject_id')\
        .rename(columns={'emergent': 'emergent_admission'})\
        .replace({'missing': None})

    var_gen_vars_df['los_h'] = (var_gen_vars_df.visit_end_datetime - var_gen_vars_df.visit_start_datetime).astype('timedelta64[h]')
    var_gen_vars_df['los_d'] = var_gen_vars_df['los_h'] / 24
    var_gen_vars_df['length_of_surgery_min'] = (var_gen_vars_df.surgery_end_datetime - var_gen_vars_df.surgery_start_datetime).astype('timedelta64[m]')
    
      
    #load custom sql data (30_day_readmission,  dnr_order, postop_submodel data)
    
    # readmission = check_load_df('30_day_readmission', directory=dir_dict.get(source_data_key), patterns=patterns, **logging_kwargs)
    # dnr_order = check_load_df('dnr_order', directory=dir_dict.get(source_data_key), patterns=patterns, **logging_kwargs)
    # var_gen_vars_df=var_gen_vars_df.merge(readmission, how='left', on='subject_id')\
    #         .merge(dnr_order, how='left', on='subject_id')

    # filter out organ donations
    var_gen_vars_df.query('(sched_post_op_location != "4021524") | sched_post_op_location.isnull()', engine='python', inplace=True)

    if os.path.exists(os.path.join(dir_dict.get('static'), 'APARI_static_raw.p')) and os.path.exists(os.path.join(dir_dict.get('time_series'), 'APARI_time_series_raw.p')) and (not force_regenerate_dataset):
        outcomes = check_load_df(os.path.join(dir_dict.get('outcomes'), 'APARI_outcome_data.csv'), **logging_kwargs)

    else:

        # load median pre-op ASA score
        asa_df: pd.DataFrame = _generate_asa_variables(load_variables_from_var_spec(variables_columns=['asa_score', 'measurement_datetime', 'observation_datetime'],
                                                                                    var_spec_key=var_file_link_key,
                                                                                    dir_dict=dir_dict,
                                                                                    filter_variables=True,
                                                                                    use_dask=True,
                                                                                    max_workers=1,
                                                                                    data_source_key=source_data_key,
                                                                                    cdm_tables=['observation', 'measurement'],
                                                                                    project='APARI',
                                                                                    mute_duplicate_var_warnings=True,
                                                                                    coalesce_fields={'asa_datetime': ['measurement_datetime', 'observation_datetime']},
                                                                                    desired_types={**{x: 'datetime' for x in ['visit_start_datetime', 'visit_end_datetime']},
                                                                                                   **{x: 'sparse_int' for x in [pid, eid, 'value_as_concept_id', 'subject_id']}},
                                                                                    patterns=patterns,
                                                                                    allow_empty_files=True, regex=True, dtype={'visit_detail_id': 'Int64'}, ds_type='pandas', **logging_kwargs)
                                                       .dropna(subset=['value_as_number', 'value_as_concept_id'], how='all'))\
            .merge(var_gen_vars_df[['subject_id', 'surgery_start_datetime']], on='subject_id', how='inner')\
            .query('asa_datetime <= surgery_start_datetime')\
            .drop(columns=[pid, eid, 'unit_concept_id', 'asa_datetime', 'surgery_start_datetime','visit_detail_id'], errors='ignore')\
            .groupby('subject_id').median().reset_index(drop=False)

        # load transfusion information
        rbc_transfusion = load_variables_from_var_spec(variables_columns=['rbc_transfusion', 'device_exposure_start_datetime', 'unit_concept_id',
                                                                          'measurement_datetime', 'transfusion', 'value_as_number'],
                                                       var_spec_key=var_file_link_key,
                                                       dir_dict=dir_dict,
                                                       filter_variables=True,
                                                       use_dask=True,
                                                       id_vars=['subject_id'],
                                                       max_workers=1,
                                                       data_source_key=source_data_key,
                                                       cdm_tables=['device_exposure', 'measurement'],
                                                       project='APARI',
                                                       coalesce_fields={'timestamp': ['device_exposure_start_datetime', 'measurement_datetime']},
                                                       mute_duplicate_var_warnings=True,
                                                       desired_types={**{x: 'datetime' for x in ['device_exposure_start_datetime', 'measurement_datetime']},
                                                                      **{x: 'sparse_int' for x in [pid, eid, 'value_as_concept_id', 'subject_id', 'unit_concept_id']},
                                                                      **{x: 'float' for x in ['value_as_number']}},
                                                       patterns=patterns,
                                                       allow_empty_files=True, regex=True, dtype={'unit_concept_id': 'float64', 'transfusion': 'str'}, ds_type='pandas', **logging_kwargs)\
            .drop(columns=['variable_name'], errors='ignore')\
            .drop_duplicates()

        assert (rbc_transfusion.unit_concept_id.isnull() | rbc_transfusion.unit_concept_id.isin(['8587', '4122415', '8510'])).all(), f"The following invalid transfusion units were detected: {rbc_transfusion.unit_concept_id[~rbc_transfusion.unit_concept_id.isin(['8587', '4122415', '8510'])].unique().tolist()}"

        # assume ~250 ml per unit of pRBCs
        rbc_transfusion.value_as_number = rbc_transfusion.apply(lambda row: row.value_as_number * (1 if ((row.unit_concept_id in ['8587', '4122415']) or ((row.value_as_number > 10) and pd.isnull(row.unit_concept_id))) else 250), axis=1)

        rbc_transfusion.drop(columns=['unit_concept_id'], inplace=True)

        rbc_transfusion = _generate_rbc_variables(rbc_transfusion).reset_index(drop=False)

        # load preop-sofa score (admit and immediat pre-op)
        sofa_df: pd.DataFrame = check_load_df('', patterns=['*eSOFA*'], regex=False, directory=dir_dict.get('generated_data'),
                                              usecols=[pid, eid, 'SOFA_datetime',
                                                       'eSOFA_score', 'e_sofa_score'],
                                              desired_types={**{x: 'sparse_int' for x in [pid, eid]},
                                                             **{'e_sofa_score': 'float', 'sofa_datetime': 'datetime'}},
                                              use_col_intersection=True, **logging_kwargs).sort_values('sofa_datetime', ascending=True)                

        admit_sofa: pd.DataFrame = pd.merge_asof(left=var_gen_vars_df[['subject_id', pid, eid, 'visit_start_datetime']].sort_values('visit_start_datetime', ascending=True),
                                                 right=sofa_df,
                                                 direction='forward',
                                                 left_on='visit_start_datetime',
                                                 right_on='sofa_datetime',
                                                 by=[pid, eid],
                                                 allow_exact_matches=True)\
            .drop(columns=['sofa_datetime', 'visit_start_datetime', pid, eid])\
            .drop_duplicates(subset=['subject_id'])\
            .rename(columns={'e_sofa_score': 'admit_e_sofa'})

        admit_sofa = _generate_sofa_variables(df=admit_sofa, sofa_cols=['admit_e_sofa'])

        preop_sofa: pd.DataFrame = pd.merge_asof(left=var_gen_vars_df[['subject_id', pid, eid, 'surgery_start_datetime']].sort_values('surgery_start_datetime', ascending=True),
                                                 right=sofa_df,
                                                 direction='backward',
                                                 left_on='surgery_start_datetime',
                                                 right_on='sofa_datetime',
                                                 by=[pid, eid],
                                                 allow_exact_matches=False)\
            .drop(columns=['sofa_datetime', 'surgery_start_datetime', pid, eid])\
            .drop_duplicates(subset=['subject_id'])\
            .rename(columns={'e_sofa_score': 'preop_e_sofa'})

        preop_sofa = _generate_sofa_variables(df=preop_sofa, sofa_cols=['preop_e_sofa'])

        del sofa_df

        # load station variables
        station_variables = check_load_df('station_vars', directory=dir_dict.get(gen_data_key), patterns=patterns, **logging_kwargs)

        # load intraop vitals [dbp, etco2, heart_rate, pip, spo2, peep, respiratory_rate, sbp, tidal_volume, body_temperature]
        intraop_df: pd.DataFrame = _harmonize_vitals(load_variables_from_var_spec(variables_columns=['diastolic_blood_pressure', 'etco2', 'heart_rate', 'pip', 'spo2',
                                                                                                     'peep', 'respiratory_rate', 'systolic_blood_pressure', 'tidal_volume',
                                                                                                     'body_temperature', 'measurement_datetime'],
                                                                                  var_spec_key=var_file_link_key,
                                                                                  dir_dict=dir_dict,
                                                                                  id_vars=['subject_id'],
                                                                                  filter_variables=True,
                                                                                  aggregation_function_filter='xxxisnullxxx',
                                                                                  data_source_key=source_data_key,
                                                                                  cdm_tables=['measurement'],
                                                                                  project='APARI',
                                                                                  mute_duplicate_var_warnings=True,
                                                                                  desired_types={**{x: 'datetime' for x in ['measurement_datetime']},
                                                                                                 **{x: 'sparse_int' for x in [pid, eid, 'value_as_concept_id', 'subject_id']}},
                                                                                  patterns=patterns,
                                                                                  allow_empty_files=True, regex=True, dtype=None, ds_type='pandas', **logging_kwargs))\
            .drop(columns=['unit_concept_id'], errors='ignore')
        # load National/State ADI data
        adi_df = check_load_df(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'Resource_Files', 'adi_condensed.csv'),
                               usecols=['nine_digit_zip', 'adi_natrank', 'adi_staterank'],
                               dtype=str,
                               use_dask=True,
                               **logging_kwargs)
        adi_df = adi_df[adi_df.nine_digit_zip.isin(var_gen_vars_df.zip_9.unique().tolist())].compute()

        # load cost data
        cost_charge_df: pd.DataFrame = load_variables_from_var_spec(variables_columns=['total_charge', 'total_cost', 'inferred_total_cost', 'professional_service_charge', 'icu_charge'],
                                                                    var_spec_key=var_file_link_key,
                                                                    dir_dict=dir_dict,
                                                                    id_vars=['subject_id'],
                                                                    filter_variables=True,
                                                                    data_source_key=source_data_key,
                                                                    cdm_tables=['cost'],
                                                                    project='APARI',
                                                                    mute_duplicate_var_warnings=True,
                                                                    desired_types={**{x: 'datetime' for x in ['visit_start_datetime', 'visit_end_datetime']},
                                                                                   **{x: 'sparse_int' for x in [pid, eid, 'value_as_concept_id', 'subject_id']}},
                                                                    patterns=patterns,
                                                                    allow_empty_files=True, regex=True, dtype=None, ds_type='pandas', **logging_kwargs)

        cost_charge_df['total_cost_times_1k'] = None if (cost_charge_df.shape[0] == 0) else cost_charge_df['total_cost'] / 1000
        cost_charge_df['total_charges_times_1k'] = None if (cost_charge_df.shape[0] == 0) else cost_charge_df['total_charge'] / 1000
        # cost_charge_df['icu_charge_times_1k'] = cost_charge_df['icu_charge'] / 1000 TODO: test once icu charges are available
        cost_charge_df['prof_charges_times_1k'] = None if (cost_charge_df.shape[0] == 0) else cost_charge_df['professional_service_charge'] / 1000

        ### average postop vitals ###
        postop_vitals_list: List[str] = ['diastolic_blood_pressure', 'heart_rate', 'systolic_blood_pressure',
                                         'spo2', 'respiratory_rate', 'body_temperature', 'gcs_eye_score']
        postop_vitals: pd.DataFrame = load_variables_from_var_spec(variables_columns=['value_as_number_count'] + postop_vitals_list,
                                                                   var_spec_key=var_file_link_key,
                                                                   dir_dict=dir_dict,
                                                                   id_vars=['subject_id'],
                                                                   use_dask=True,
                                                                   max_workers=1,
                                                                   filter_variables=True,
                                                                   data_source_key=source_data_key,
                                                                   cdm_tables=['measurement'],
                                                                   aggregation_function_filter='AVG',
                                                                   project='APARI',
                                                                   mute_duplicate_var_warnings=True,
                                                                   desired_types={**{x: 'datetime' for x in ['visit_start_datetime', 'visit_end_datetime']},
                                                                                  **{x: 'sparse_int' for x in [pid, eid, 'value_as_concept_id', 'subject_id', 'unit_concept_id']}},
                                                                   patterns=patterns,
                                                                   allow_empty_files=True, regex=True, dtype={'unit_concept_id': 'float64'}, ds_type='pandas', **logging_kwargs)

        # standardize units and compute weighted average
        postop_vitals = _harmonize_vitals(postop_vitals)\
            .groupby(['subject_id', 'variable_name', 'unit_concept_id'])\
            .apply(_weighted_average, val_col='value_as_number', weight_col='value_as_number_count')\
            .rename('value_as_number')\
            .reset_index(drop=False)\
            .drop(columns=['level_3'], errors='ignore')

        try:
            postop_vitals = pd.pivot(postop_vitals,
                                     index='subject_id',
                                     columns='variable_name',
                                     values='value_as_number')
        except ValueError:
            problems = postop_vitals.groupby(['variable_name'])['unit_concept_id'].unique()
            raise Exception(f'There is one or more measuremets with multiple units that still need to be converted: Please check the following: {problems[problems.apply(len) > 1]}')

        for v in postop_vitals_list:
            if v not in postop_vitals.columns:
                postop_vitals[v] = None

        postop_vitals.rename(columns={x: f'{x}_4hr_postop_avg' for x in postop_vitals_list}, inplace=True)

        postop_vitals = _generate_gcs_variables(postop_vitals)

        # postop pressors (hour after surgery)
        postop_pressors: pd.DataFrame = load_variables_from_var_spec(variables_columns=['pressors_inotropes', 'pressor_flag'],
                                                                     var_spec_key=var_file_link_key,
                                                                     dir_dict=dir_dict,
                                                                     id_vars=['subject_id'],
                                                                     use_dask=True,
                                                                     max_workers=1,
                                                                     filter_variables=True,
                                                                     data_source_key=source_data_key,
                                                                     cdm_tables=['drug_exposure'],
                                                                     project='APARI',
                                                                     mute_duplicate_var_warnings=True,
                                                                     desired_types={**{x: 'datetime' for x in ['visit_start_datetime', 'visit_end_datetime']},
                                                                                    **{x: 'sparse_int' for x in [pid, eid, 'value_as_concept_id', 'subject_id']}},
                                                                     patterns=patterns,
                                                                     allow_empty_files=True, regex=True, dtype=None, ds_type='pandas', **logging_kwargs)\
            .drop(columns=['variable_name'], errors='ignore')

        # postop mv
        postop_mv_variables = check_load_df('postop_mv_vars', directory=dir_dict.get(gen_data_key), patterns=patterns, **logging_kwargs)

        # postop procedure variables
        postop_procs: List[str] = ['cpr', 'intubation', 'arterial_catheter', 'central_venous_catheter', 'pulmonary_artery_catheter',
                                   'bronchoscopy', 'cardioversion_electric', 'chest_tube']
        postop_procedures: pd.DataFrame = load_variables_from_var_spec(variables_columns=postop_procs,
                                                                       var_spec_key=var_file_link_key,
                                                                       dir_dict=dir_dict,
                                                                       id_vars=['subject_id'],
                                                                       filter_variables=True,
                                                                       use_dask=True,
                                                                       max_workers=1,
                                                                       data_source_key=source_data_key,
                                                                       cdm_tables=['procedure_occurrence'],
                                                                       project='APARI',
                                                                       mute_duplicate_var_warnings=True,
                                                                       desired_types={**{x: 'datetime' for x in ['procedure_datetime']},
                                                                                      **{x: 'sparse_int' for x in [pid, eid, 'value_as_concept_id', 'subject_id']}},
                                                                       patterns=patterns,
                                                                       allow_empty_files=True, regex=True, dtype=None, ds_type='pandas', **logging_kwargs)

        if postop_procedures.shape[0] > 0:
            postop_procedures['indicator'] = '1'
            postop_procedures = pd.pivot(postop_procedures.groupby(['subject_id', 'variable_name']).agg({'indicator': 'max'}).reset_index(drop=False),
                                         index='subject_id',
                                         columns='variable_name',
                                         values='indicator').reset_index(drop=False)
        else:
            postop_procedures = pd.DataFrame(columns=['subject_id'] + postop_procs)

        ### second OR case ###
        second_surgery: pd.DataFrame = load_variables_from_var_spec(variables_columns=['second_or_end_datetime', 'second_or_start_datetime'],
                                                                    var_spec_key=var_file_link_key,
                                                                    dir_dict=dir_dict,
                                                                    id_vars=['subject_id'],
                                                                    filter_variables=False,
                                                                    paritition_filter='2',
                                                                    data_source_key=source_data_key,
                                                                    cdm_tables=['visit_detail'],
                                                                    project='APARI',
                                                                    mute_duplicate_var_warnings=True,
                                                                    desired_types={**{x: 'datetime' for x in ['second_or_start_datetime', 'second_or_end_datetime']},
                                                                                   **{x: 'sparse_int' for x in [pid, eid, 'value_as_concept_id', 'subject_id']}},
                                                                    patterns=patterns,
                                                                    allow_empty_files=True, regex=True, dtype=None, ds_type='pandas', **logging_kwargs)\
            .merge(load_variables_from_var_spec(variables_columns=['second_or_procedure'],
                                                var_spec_key=var_file_link_key,
                                                dir_dict=dir_dict,
                                                id_vars=['subject_id'],
                                                filter_variables=False,
                                                paritition_filter='2',
                                                data_source_key=source_data_key,
                                                cdm_tables=['procedure_occurrence'],
                                                project='APARI',
                                                mute_duplicate_var_warnings=True,
                                                desired_types={**{x: 'datetime' for x in ['second_or_start_datetime', 'second_or_end_datetime']},
                                                               **{x: 'sparse_int' for x in [pid, eid, 'second_or_procedure', 'subject_id']}},
                                                patterns=patterns,
                                                allow_empty_files=True, regex=True, dtype=None, ds_type='pandas', **logging_kwargs)
                   .drop_duplicates(subset=['subject_id']).drop(columns=['variable_name']),
                   how='left',
                   on='subject_id')\
            .merge(load_variables_from_var_spec(variables_columns=['second_or_surgery_type'],
                                                var_spec_key=var_file_link_key,
                                                dir_dict=dir_dict,
                                                id_vars=['subject_id'],
                                                filter_variables=False,
                                                paritition_filter='2',
                                                data_source_key=source_data_key,
                                                cdm_tables=['observation'],
                                                project='APARI',
                                                mute_duplicate_var_warnings=True,
                                                desired_types={**{x: 'datetime' for x in ['second_or_start_datetime', 'second_or_end_datetime']},
                                                               **{x: 'sparse_int' for x in [pid, eid, 'second_or_surgery_type', 'subject_id']}},
                                                patterns=patterns,
                                                allow_empty_files=True, regex=True, dtype=None, ds_type='pandas', **logging_kwargs)
                   .drop_duplicates(subset=['subject_id']).drop(columns=['variable_name']),
                   how='left',
                   on='subject_id')\
            .merge(var_gen_vars_df[['subject_id', 'surgery_start_datetime']],
                   how='inner',
                   on='subject_id')\
            .query("0 < (second_or_start_datetime - surgery_start_datetime).astype('timedelta64[h]') < 720", engine='python')
           # .query("(second_or_start_datetime - surgery_start_datetime).astype('timedelta64[h]') < 720", engine='python')
           
        second_surgery['had_subsequent_or'] = '1' if second_surgery.shape[0] > 0 else'0'
        second_surgery['interval_first_or_to_second_or'] = (second_surgery.second_or_start_datetime - second_surgery.surgery_start_datetime).astype('timedelta64[h]')

        second_surgery.drop(columns=['surgery_start_datetime'], inplace=True)

        ### discharge disposition ###
        discharge_df = load_variables_from_var_spec(variables_columns=['discharged_to_concept_id', 'discharge_to_concept_id'],
                                                    var_spec_key=var_file_link_key,
                                                    dir_dict=dir_dict,
                                                    id_vars=['subject_id'],
                                                    data_source_key=source_data_key,
                                                    cdm_tables=['visit_occurrence'],
                                                    project='APARI',
                                                    mute_duplicate_var_warnings=True,
                                                    desired_types={**{x: 'datetime' for x in ['second_or_start_datetime', 'second_or_end_datetime']},
                                                                   **{x: 'sparse_int' for x in [pid, eid, 'discharged_to_concept_id', 'discharge_to_concept_id', 'subject_id']}},
                                                    patterns=patterns,
                                                    allow_empty_files=True, regex=True, dtype=None, ds_type='pandas', **logging_kwargs)\
            .rename(columns={'discharge_to_concept_id': 'discharged_to_concept_id'})

        stnd_dict: dict = check_load_df(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'Resource_Files', 'APARI_Variable_Specification.xlsx'), sheet_name='standardization_values', desired_types={'concept_id': 'sparse_int'})\
            .query('column_name == "discharged_to_concept_id"').set_index('concept_id').standardized_value.to_dict()

        discharge_df['dispo'] = discharge_df.discharged_to_concept_id.apply(lambda x: stnd_dict.get(x, 'other' if pd.notnull(x) else 'unknown'))

        discharge_df['dispo_non_home'] = discharge_df.dispo.isin(['home', 'home_care']).astype(int).astype(str)

        discharge_df.drop(columns=['discharged_to_concept_id'], inplace=True)

        # outcomes
        outcomes = check_load_df('surgery_omop_outcome_final', directory=dir_dict.get(gen_data_key), patterns=patterns,
                                 desired_types={**{x: 'sparse_int' for x in ['visit_detail_id', 'cv_cardiac_arrest_surgery_discharge_flag', 'subject_id']},
                                                **{'icu_surgery_disch': 'float'},
                                                **{x: 'datetime' for x in ['surgery_start_datetime', 'surgery_end_datetime']}},
                                 usecols=['aki_surgery_disch_without_race_correction', 'overall_aki_without_race_correction_type',
                                          'max_aki_without_race_correction_stage', 'icu_surgery_disch', 'icu_free_surgery_disch_cal',
                                          'cv_cardiac_arrest_surgery_discharge_flag', 'mv_surgery_disch_cal', 'mv_free_surgery_disch_cal',
                                          'subject_id', 'visit_detail_id', 'hospital_mortality', 'surgery_start_datetime', 'surgery_end_datetime'],
                                 **logging_kwargs)\
            .rename(columns={'aki_surgery_disch_without_race_correction': 'aki_without_race_correction',
                             'cv_cardiac_arrest_surgery_discharge_flag': 'cardiac_arrest_surg_disch',
                             'icu_surgery_disch': 'icu_los',
                             'icu_free_surgery_disch_cal': 'icu_free_days',
                             'mv_surgery_disch_cal': 'mv_days',
                             'mv_free_surgery_disch_cal': 'mv_free_days'})
            
            
        if subject_id_type == 'visit_detail_id':
            outcomes: pd.DataFrame = outcomes\
            .query('visit_detail_id == subject_id')\
            .drop(columns=['visit_detail_id', 'surgery_start_datetime', 'surgery_end_datetime'])
        else:
            outcomes: pd.DataFrame = var_gen_vars_df[['subject_id', 'surgery_start_datetime', 'surgery_end_datetime']].copy(deep=True)\
                .merge(outcomes,
                       how='inner',
                       on=['subject_id', 'surgery_start_datetime', 'surgery_end_datetime'])\
                    .drop(columns=['surgery_start_datetime', 'surgery_end_datetime'])

        for x in ['cardiac_arrest_surg_disch', 'hospital_mortality']:
            outcomes[x].fillna('0', inplace=True)

        outcomes.loc[:, 'prolonged_icu_stay'] = outcomes.loc[:, ['icu_los']].apply(lambda row: 1 if (row.icu_los >= 2) else 0, axis=1)

        save_data(outcomes, out_path=os.path.join(dir_dict.get('outcomes'), 'APARI_outcome_data.csv'), index=False)

        static_df: pd.DataFrame = var_gen_vars_df.merge(adi_df, how='left', left_on='zip_9', right_on='nine_digit_zip')\
            .drop(columns=['nine_digit_zip'])\
            .merge(preop_sofa, how='left', on='subject_id')\
            .merge(admit_sofa, how='left', on='subject_id')\
            .merge(rbc_transfusion, how='left', on='subject_id')\
            .merge(station_variables, how='left', on='subject_id')\
            .merge(asa_df, how='left', on='subject_id')\
            .merge(cost_charge_df, how='left', on='subject_id')\
            .merge(postop_vitals, how='left', on='subject_id')\
            .merge(postop_pressors, how='left', on='subject_id')\
            .merge(postop_mv_variables, how='left', on='subject_id')\
            .merge(postop_procedures, how='left', on='subject_id')\
            .merge(second_surgery, on='subject_id', how='left')\
            .merge(discharge_df, on='subject_id', how='left')\
            .merge(outcomes[['subject_id']], how='inner', on='subject_id')\
            .drop_duplicates()

        assert static_df.subject_id.duplicated().any() == False, 'There is some duplication in the dataset that needs to be resolved prior to continuing'

        save_data(static_df, out_path=os.path.join(dir_dict.get('static'), 'APARI_static_raw.csv'), index=False)

        save_data(static_df, out_path=os.path.join(dir_dict.get('static'), 'APARI_static_raw.p'))
        
        # ensure that only events with outcome are used in model training
        save_data(intraop_df.merge(outcomes[['subject_id']], how='inner', on='subject_id'), out_path=os.path.join(dir_dict.get('time_series'), 'APARI_time_series_raw.p'))

    cohort_df: pd.DataFrame = check_load_df(os.path.join(dir_dict.get(source_data_key), f'{project_name}_master_cohort_definition.csv'),
                                            usecols=[subject_id_type, 'cohort', 'facility_zip'],
                                            desired_types={subject_id_type: 'sparse_int', 'facility_zip': 'sparse_int'})\
        .rename(columns={subject_id_type: 'subject_id'})

    # for facility_zip in cohort_df.facility_zip.unique():
    #     print(facility_zip)
    #     _dataset_builder(cohort_df=cohort_df.query(f"facility_zip == '{facility_zip}'").reset_index(drop=True),
    #                      var_gen_vars_df=var_gen_vars_df,
    #                      outcomes=outcomes,
    #                      serial=serial,
    #                      dir_dict=dir_dict,
    #                      dset_name=dset_name,
    #                      facility_zip=facility_zip,
    #                      force_regenerate_dataset=force_regenerate_dataset,
    #                      **logging_kwargs)

    _dataset_builder(cohort_df=cohort_df,
                     var_gen_vars_df=var_gen_vars_df,
                     outcomes=outcomes,
                     serial=serial,
                     dir_dict=dir_dict,
                     dset_name=dset_name,
                     facility_zip=None,
                     force_regenerate_dataset=force_regenerate_dataset,
                     **logging_kwargs)

    open(success_fp, 'a').close()


def _generate_asa_variables(asa_df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize ASA score to number and add generated variables.

    Code:       Name:                                                            Vocabulary:
    4186042		American Society of Anesthesiologists physical status class 1    SNOMED
    4184967		American Society of Anesthesiologists physical status class 2    SNOMED
    4186043		American Society of Anesthesiologists physical status class 3    SNOMED
    4211334		American Society of Anesthesiologists physical status class 4    SNOMED
    4186044		American Society of Anesthesiologists physical status class 5    SNOMED
    4186045		American Society of Anesthesiologists physical status class 6    SNOMED
    1620670		ASA1 (no disturbance)                                            LOINC
    1620561		ASA2 (mild/moderate)                                             LOINC
    1620778		ASA3 (severe)                                                    LOINC
    1621010		ASA4 (life threatening)                                          LOINC
    1620711		ASA5 (moribund)                                                  LOINC


    Actions
    -------
    1. Convert ASA concept ID to number
    2. Check for invalid ASA scores
    3. Drop Unecessary columns
    4. Rename value_as_number to asa_score
    5. Add + variables
    6. Return modified dataframe

    Parameters
    ----------
    asa_df : pd.DataFrame
        required columns:
            *value_as_number
            *value_as_concept_id

    Returns
    -------
    asa_df : pd.DataFrame
        *asa_score

    """
    non_numeric_idx: pd.Series = asa_df.value_as_number.isnull() & asa_df.value_as_concept_id.notnull()

    if non_numeric_idx.any():
        asa_df.loc[non_numeric_idx, 'value_as_number'] = asa_df.loc[non_numeric_idx, 'value_as_concept_id'].copy(deep=True)\
            .astype(float).astype(int).replace({4186042: 1,
                                                4184967: 2,
                                                4186043: 3,
                                                4211334: 4,
                                                4186044: 5,
                                                4186045: 6,
                                                1620670: 1,#		ASA1 (no disturbance)  LOINC
                                                1620561: 2, #		ASA2 (mild/moderate)   LOINC
                                                1620778: 3, #		ASA3 (severe)  LOINC
                                                1621010: 4, #		ASA4 (life threatening) LOINC
                                                1620711: 5})
    invalid_scores = asa_df.value_as_number[(
        (asa_df.value_as_number > 6) | (asa_df.value_as_number < 1))].value_counts()

    assert len(
        invalid_scores) == 0, f'There were {len(invalid_scores)} Asa Scores. Please review the followng scores found and resovle them before continuing. {invalid_scores}'

    asa_df.drop(columns=['variable_name', 'value_as_concept_id'], inplace=True)
    asa_df.rename(columns={'value_as_number': 'asa_score'}, inplace=True)

    asa_df.loc[:, 'asa_1_plus'] = asa_df.loc[:, ['asa_score']].apply(
        lambda row: 1 if row.asa_score >= 1 else 0, axis=1)
    asa_df.loc[:, 'asa_2_plus'] = asa_df.loc[:, ['asa_score']].apply(
        lambda row: 1 if row.asa_score >= 2 else 0, axis=1)
    asa_df.loc[:, 'asa_3_plus'] = asa_df.loc[:, ['asa_score']].apply(
        lambda row: 1 if row.asa_score >= 3 else 0, axis=1)
    asa_df.loc[:, 'asa_4_plus'] = asa_df.loc[:, ['asa_score']].apply(
        lambda row: 1 if row.asa_score >= 4 else 0, axis=1)
    asa_df.loc[:, 'asa_5_plus'] = asa_df.loc[:, ['asa_score']].apply(
        lambda row: 1 if row.asa_score >= 5 else 0, axis=1)

    asa_df['asa_score_numeric'] = asa_df.asa_score.copy(deep=True)

    return check_load_df(asa_df, desired_types={'asa_score': 'sparse_int'})


def _generate_sofa_variables(df: pd.DataFrame, sofa_cols: List[str] = ['preop_e_sofa', 'admit_e_sofa']) -> pd.DataFrame:

    for sc in sofa_cols:
        df.loc[:, f'{sc}_1_plus'] = df.loc[:, sc].apply(lambda x: 1 if x >= 1 else 0)
        df.loc[:, f'{sc}_2_plus'] = df.loc[:, sc].apply(lambda x: 1 if x >= 2 else 0)
        df.loc[:, f'{sc}_3_plus'] = df.loc[:, sc].apply(lambda x: 1 if x >= 3 else 0)
        df.loc[:, f'{sc}_4_plus'] = df.loc[:, sc].apply(lambda x: 1 if x >= 4 else 0)

    return df


def _generate_gcs_variables(gcs_df: pd.DataFrame) -> pd.DataFrame:
    """
    Check for invalid gcs_eye scores and add gcs_eye < 4.

    Parameters
    ----------
    gcs_df : pd.DataFrame
        DESCRIPTION.

    Returns
    -------
    gcs_df : pd.DataFrame

    """
    val_col: str = 'gcs_eye_score_4hr_postop_avg' if 'gcs_eye_score_4hr_postop_avg' in gcs_df.columns else 'gcs_eye_score' if gcs_df.columns else 'value_as_number'
    name_col: str = val_col if val_col != 'value_as_number' else 'variable_name'
    output_name: str = 'gcs_e' if val_col == 'value_as_number' else val_col

    if name_col == 'variable_name':
        gcs_df.query('variable_name == "gcs_eye_score"', inplace=True)

    invalid_scores = gcs_df[val_col][((gcs_df[val_col] > 4) | (gcs_df[val_col] < 1)) & gcs_df[val_col].notnull()].value_counts()

    assert len(invalid_scores) == 0, f'There were {len(invalid_scores)} Glasgow Coma Eye Scores. Please review the followng scores found and resovle them before continuing. {invalid_scores}'

    gcs_df.drop(columns=['variable_name', 'value_as_concept_id'], inplace=True, errors='ignore')
    gcs_df.rename(columns={'value_as_number': output_name}, inplace=True)
    gcs_df.loc[:, 'gcs_e_less_than_4'] = gcs_df.loc[:, [output_name]].apply(lambda row: None if pd.isnull(row[output_name]) else 1 if (row[output_name] < 4) else 0, axis=1)

    return gcs_df


def _generate_rbc_variables(rbc_df: pd.DataFrame) -> pd.DataFrame:

    intervals: List[str] = ['preop', 'intraop', 'postop']

    # get counts and amounts of tranfusions in each period
    rbc_df = pd.pivot(rbc_df.groupby(['subject_id', 'transfusion'])['value_as_number'].count().reset_index(drop=False),
                      index='subject_id', values='value_as_number', columns='transfusion')\
        .rename(columns={f'{x}_rbc_volume': f'{x}_rbc_count' for x in intervals})\
        .merge(pd.pivot(rbc_df.groupby(['subject_id', 'transfusion'])['value_as_number'].sum().reset_index(drop=False),
                        index='subject_id', values='value_as_number', columns='transfusion'),
               how='inner',
               on='subject_id')

    # ensure all time bins are represented
    for col in [f'{x}_rbc_count' for x in intervals] + [f'{x}_rbc_volume' for x in intervals]:
        if col not in rbc_df.columns:
            rbc_df[col] = 0 if rbc_df.shape[0] > 0 else None
        else:
            rbc_df[col].fillna(0, inplace=True)

    # calculate total for admission
    rbc_df['total_rbc_volume'] = rbc_df[[f'{x}_rbc_volume' for x in intervals]].sum(axis=1)

    # add indicator variables
    rbc_df.loc[:, 'any_preop_rbc'] = rbc_df.loc[:, ['preop_rbc_volume']].apply(lambda row: 0 if (row.preop_rbc_volume < 1) else 1, axis=1)
    rbc_df.loc[:, 'any_intraop_rbc'] = rbc_df.loc[:, ['intraop_rbc_volume']].apply(lambda row: 0 if (row.intraop_rbc_volume < 1) else 1, axis=1)
    rbc_df.loc[:, 'any_postop_rbc'] = rbc_df.loc[:, ['postop_rbc_volume']].apply(lambda row: 0 if (row.postop_rbc_volume < 1) else 1, axis=1)
    rbc_df.loc[:, 'any_rbc_during_admit'] = rbc_df.loc[:, ['total_rbc_volume']].apply(lambda row: 0 if (row.total_rbc_volume < 1) else 1, axis=1)

    return rbc_df


def _harmonize_vitals(df: pd.DataFrame) -> pd.DataFrame:

    ### handle heart_rate/resp rate ###
    df.unit_concept_id.replace({'4118124': '8541',  # beats/min -> /min
                                '4117833': '8541'},  # breaths/min -> /min
                               inplace=True)

    fillable_idx: pd.Series = df.unit_concept_id.isnull() & df.variable_name.isin(['heart_rate', 'respiratory_rate'])

    if fillable_idx.any():
        df.loc[fillable_idx, 'unit_concept_id'] = '8541'

    ### handle temperature ###
    df.unit_concept_id.replace({'3192389': '586323',  # degrees Celsius
                                '4122393': '586323',  # degrees C
                                '8653': '586323',  # degrees Celsius
                                '3191526': '9289',  # degrees fahrenheit
                                '8517': '9289',  # bad map of unit code F as farad instead of fahrenheit
                                '4119675': '9289'}, inplace=True)  # degrees F
    fahrenheit_temp: pd.Series = df.variable_name.str.contains('temperature', regex=False, na=False) & df.unit_concept_id.isin(['9289'])

    if fahrenheit_temp.any():
        df.loc[fahrenheit_temp, 'value_as_number'] = (df.loc[fahrenheit_temp, 'value_as_number'] - 32) * 5 / 9
        df.loc[fahrenheit_temp, 'unit_concept_id'] = '586323'

    # note there is no assumption about unit for temperature

    ### handle blood pressure units ###
    df.unit_concept_id.replace({'4118323': '8876'}, inplace=True)  # mmHg

    # fill mm[Hg]
    fillable_idx: pd.Series = df.unit_concept_id.isnull() & df.variable_name.str.contains('blood_pressure', regex=False, na=False)

    if fillable_idx.any():
        df.loc[fillable_idx, 'unit_concept_id'] = '8876'

    ### Handl SpO2 ###
    spo2_idx: pd.Series = df.variable_name == 'spo2'

    if spo2_idx.any():
        df.loc[spo2_idx, 'unit_concept_id'] = df.loc[spo2_idx, 'unit_concept_id'].replace({'8554': '8728', '9230': '8728'})  # percent -> % sat, '% O2 -> % sat

    missing_spo2_idx: pd.Series = spo2_idx & df.unit_concept_id.isnull()

    if missing_spo2_idx.any():
        df.loc[missing_spo2_idx, 'unit_concept_id'] = '8728'
        
        
    ### Handle missing unit_concept_id for gcs_eye_score
    gcs_idx = df.variable_name == 'gcs_eye_score'
    
    if gcs_idx.any():
        df.loc[gcs_idx & df.unit_concept_id.isnull(), 'unit_concept_id'] = '9999'  # Assign dummy ID


    return df


def _weighted_average(dfg: pd.DataFrame, val_col: str, weight_col: str) -> float:
    return (dfg[val_col] * dfg[weight_col]).sum() / dfg[weight_col].sum()


def _dataset_builder(cohort_df: pd.DataFrame,
                     var_gen_vars_df: pd.DataFrame,
                     outcomes: pd.DataFrame,
                     serial: bool,
                     dir_dict: Dict[str, any],
                     dset_name: str,
                     facility_zip: Union[str, None],
                     force_regenerate_dataset: bool = False,
                     
                     **logging_kwargs):
    facility_zip: str = facility_zip or 'all'

    out_fp: str = os.path.join(dir_dict.get('dataset'), f'{facility_zip}_{dset_name}')

    if os.path.exists(out_fp) and not force_regenerate_dataset:
        return

    ds_dict: dict = {}
    for t in ['time_series', 'static']:
        pfp: str = os.path.join(dir_dict.get(t), f'APARI_{facility_zip}_{t}_data.h5')

        if (force_regenerate_dataset or (not os.path.exists(pfp))):
            t_df = check_load_df(os.path.join(dir_dict.get(t), f'APARI_{t}_raw.p'), **logging_kwargs)\
                .merge(cohort_df[['subject_id']], on='subject_id', how='inner')
        else:
            t_df = None

        enc_dir: str = os.path.join(dir_dict.get(f'{t}_standardization'), facility_zip)
        if not os.path.exists(enc_dir):
            os.mkdir(enc_dir)

        ds_dict[t] = {**{'helper_instruct_fp': os.path.join(os.path.dirname(os.path.dirname(__file__)), 'Resource_Files', 'APARI_Variable_Specification.xlsx'),
                         'raw_df': t_df,
                         'processed_df': None if (force_regenerate_dataset or (not os.path.exists(pfp))) else pfp,
                         'processed_loading_kwargs': None if (force_regenerate_dataset or (not os.path.exists(pfp))) else ({'dataset': 'ts' if t == 'time_series' else 'static'}),
                         'run_standardization': (force_regenerate_dataset or (not os.path.exists(pfp))),
                         'save_fp': pfp if (force_regenerate_dataset or (not os.path.exists(pfp))) else None,
                         'instruction_fp': os.path.join(dir_dict.get(f'{t}_standardization'), f'APARI_{facility_zip}_{t}_standardization_instructions.xlsx'),
                         'training_run': True,
                         'encoder_dir': enc_dir,
                         #'train_ids': cohort_df.subject_id[cohort_df.cohort.str.contains(r'TRAIN|DEVELOPMENT', regex=True, case=False, na=False)].tolist(), #exract ids from train/validation/test cohort to get seperate instruction files.
                         'train_ids': cohort_df.subject_id[cohort_df.cohort.str.contains(r'VALIDATION', regex=True, case=False, na=False)].tolist(),
                         'id_index': 'subject_id',
                         'use_existing_instructions': False,
                         'time_index_col': 'measurement_datetime' if t == 'time_series' else None,
                         'default_na_values': ['missing', 'unavailable', 'not available', 'unknown',
                                               'abnormal_value', 'no egfr', 'no reference creatinine'],
                         'default_missing_value_numeric': 'xxxmedianxxx',
                         'default_missing_value_binary': 0,   
                         'default_other_value_binary': None,
                         'default_missing_value_cat': 'unknown',
                         'default_other_value_cat': 'other',
                         'default_case_standardization': 'lower',
                         'default_min_num_cat_levels': 5,
                         'default_one_hot_embedding_threshold': 5,
                         'default_missingness_threshold': 0.5,
                         'default_lower_limit_percentile': 0.01,
                         'default_scale_values': True,
                         'default_upper_limit_percentile': 0.99,
                         'default_fill_lower_upper_bound_percentile': 0.15,
                         'default_fill_upper_lower_bound_percentile': 0.85,
                         'default_ensure_col': True,
                         'debug_column': None,
                         'seperate_by_type': False,
                         'stacked_meas_name': 'variable_name',
                         'stacked_meas_value': 'value_as_number',
                         'start': var_gen_vars_df.loc[var_gen_vars_df.subject_id.isin(cohort_df.subject_id), ['subject_id', 'surgery_start_datetime']].rename(columns={'surgery_start_datetime': 'start'}) if t == 'time_series' else None,
                         'end': var_gen_vars_df.loc[var_gen_vars_df.subject_id.isin(cohort_df.subject_id), ['subject_id', 'surgery_end_datetime']].rename(columns={'surgery_end_datetime': 'end'}) if t == 'time_series' else None,
                         'time_bin': '1 min',
                         'interpolation_method': 'linear',
                         'interpolation_limit_direction': 'both',
                         'interpolation_limit': None,
                         'interpolation_limit_area': None,
                         'resample_origin': 'start',
                         'resample_label': 'left',
                         'resample_fillna_val': None,
                         'resample_agg_func': _mean_only,
                         'default_dtype': None,
                         'serial': serial,
                         'pre_resample_default_dtype': None,
                         'random_seed': 42},
                      **logging_kwargs}

    build_dataset(datasets=ds_dict,
                  cohort_df=cohort_df,
                  subject_id_col='procedure_occurrence_id',   # wrong call before, should be 'procedure_occurrence_id',subject_id_col='visit_detail_id',
                  out_fp=out_fp,
                  y=check_load_df(outcomes.loc[outcomes.subject_id.isin(cohort_df.subject_id), ['subject_id', 'hospital_mortality', 'prolonged_icu_stay']], desired_types={x: 'int' for x in ['hospital_mortality', 'prolonged_icu_stay']}),
                  **logging_kwargs)

if __name__ == '__main__':
    import os
    import pandas as pd
    from typing import Dict, List, Union
    from tqdm import tqdm
    import sys
    sys.path.append(r'P:\GitHub\APARI_Federated_Learning')
    from Python.Utilities.FileHandling.variable_specification_utilities import load_variables_from_var_spec
    from Python.Utilities.FileHandling.io import check_load_df, save_data
    from Python.Utilities.PreProcessing.standardization_functions_config_helper import process_df_with_pre_processing_instructions
    from Python.Utilities.PreProcessing.aggregation_functions import _mean_only
    from Python.Utilities.PreProcessing.Standardized_data import build_dataset
    from Python.Utilities.Logging.log_messages import log_print_email_message as logm
    from Python.Utilities.General.func_utils import debug_inputs
    from Python.make_apari_dataset import _dataset_builder, _weighted_average, _harmonize_vitals, _generate_asa_variables, _generate_gcs_variables, _generate_rbc_variables, _generate_sofa_variables
    import pickle
    locals().update(pickle.load(open(r"Z:\GitHub\APARI_Federated_Learning\make_dataset.pkl", 'rb')))
    
    __file__ = r"Z:\GitHub\APARI_Federated_Learning\Python\make_apari_dataset.py"
