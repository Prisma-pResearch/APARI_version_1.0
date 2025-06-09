# -*- coding: utf-8 -*-
"""
Created on Thu Jun  8 17:51:25 2023

@author: ruppert20
"""
import pandas as pd
import os
from typing import Dict, Union, List
from .Utilities.FileHandling.variable_specification_utilities import load_variables_from_var_spec
from .Utilities.PreProcessing.data_format_and_manipulation import coalesce
from .Utilities.FileHandling.io import check_load_df, save_data
from .Utilities.Logging.log_messages import log_print_email_message as logm
from .AKI_Phenotype.Python.main import main_run_AKI_CKD_Phenotyping
from .variable_standardization_v2 import standardize_variables
from .Residency import generate_residency_variables
from .meds_v2 import meds_generation_v2
from .comorbidites import calculate_charlson_elixhauser_comorbidity_indicies_v2
from .labs import extract_laboratory_variables
from .ckd_result import generate_cdk_results
from .Utilities.General.func_utils import debug_inputs


def omop_variable_generation(dir_dict: Dict[str, str],
                             project_name: str,
                             data_source_key: str,
                             default_facility_zip: str,
                             aki_phenotype_data_key: str,
                             subject_id_type: str,
                             var_file_linkage_fp: str = 'variable_file_link',
                             batch_id: Union[str, None] = None,
                             mode: str = 'surgery',
                             pid: str = 'person_id',
                             eid: str = 'visit_occurrence_id',
                             exact_subject_mode: bool = True,
                             labs: List[str] = ['glucose_post', 'anion_gap', 'troponin_i', 'bun', 'base_excess', 'po2a', 'pco2a', 'bun_ur_24h', 'lymphocytes_per', 'cacr_r_ur_24h', 'upcr',
                                                'rbc_ur', 'apr_ur_24h', 'apr_ur', 'lactate', 'hco3', 'creatinine_ur_mol', 'hgb', 'microalbumin_24h_t', 'microalbumin_ur', 'umacr', 'glucose_ur',
                                                'alt', 'albumin', 'albumin_ur', 'albumin_ur_24h_t', 'hgb_a1c', 'calcium', 'calcium_ur', 'calcium_ionized', 'calcium_ur_24h', 'ast', 'pha', 'base_deficit',
                                                'bilirubin_dir', 'bilirubin_tot', 'crp', 'co2', 'carboxyhem', 'rbc_ur_v', 'uap_cat', 'o2sata', 'hct', 'hgb_electrophoresis', 'creatinine_ur_24h', 'chloride',
                                                'chloride_ur', 'chloride_ur_24h_t', 'rdw', 'chloride_ur_24h', 'potassium_ur_24h', 'sodium_ur_24h', 'creatinine', 'creatinine_ur', 'creatinine_ur_24h_t',
                                                'glucose', 'glucose_t', 'neutrophils_per', 'methem', 'eosinophils', 'eosinophils_per', 'rbc', 'wbc', 'lymphocytes', 'monocytes_per', 'neutrophils',
                                                'neutrophils_band', 'neutrophils_per_band', 'plt', 'ph_ur', 'potassium', 'potassium_ur', 'potassium_ur_24h_t', 'p24', 'sodium', 'sodium_ur', 'sodium_ur_24h_t',
                                                'sg', 'microalbumin_24h', 'basophils_per', 'esr', 'mcv', 'bnp', 'bun_ur', 'bun_ur_24h_t', 'uncr', 'mpv', 'rbc_ur_pres', 'wbc_ur_pres', 'scr_ur_24h', 'p_panel',
                                                'inr', 'chpd_ur_24h', 'sodium_u_24hr', 'wbc_ur_sedim_l', 'wbc_ur_sedim', 'calcium_ionized_corr', 'bilirubin_tot_ur_pres', 'wbc_ur', 'potassium_ur_24h_mt',
                                                'hgb_ur', 'troponin_t', 'calcium_ur_24h_t', 'bilirubin_tot_ur', 'basophils', 'monocytes', 'mch', 'mchc', 'uacr', 'cacr_r_ur'],
                             out_path: Union[str, None] = None,
                             **logging_kwargs):
    """
                                 Generate Pre-admission and pre-surgery/icu stay variables

                                 Parameters
                                 ----------
                                 default_facility_zip : str
                                     zip code to fill if facility zip code is null.
                                 aki_phenotype_data_key : str
                                     key in the dir_dict to find the directory containing the aki_phenotype information. Note: The script will run the AKI phenotype if the data cannot be found.
                                 var_file_linkage_fp : str, optional
                                     The key in the dir_dict where the linkage between the source_data and queries can be found. The default is 'variable_file_link'.
                                 batch_id : Union[str, None], optional
                                     Batch id used in the patterns in order to load the proper batch. The default is None.
                                 mode : str, optional
                                     Whether the variables should be generated based on the admission, surgery, or icu stay. The default is 'surgery'.
                                     ***NOTE***: The mode selected must correspond to the subject id, otherwise there may be some issures downstream.***
                                 pid : str, optional
                                     patient id column. The default is 'person_id'.
                                 eid : str, optional
                                     encounter id column. The default is 'visit_occurrence_id'.
                                 exact_subject_mode : bool, optional
                                     Whether dataframes should be truncated to match the subject ids in the cohort definition table. The default is True.
                                 labs : List[str], optional
                                     list of labs to run lab variable generation on. The default is ['glucose_post', 'anion_gap', 'troponin_i', 'bun', 'base_excess', 'po2a', 'pco2a', 'bun_ur_24h', 'lymphocytes_per', 'cacr_r_ur_24h', 'upcr',
                                                                  'rbc_ur', 'apr_ur_24h', 'apr_ur', 'lactate', 'hco3', 'creatinine_ur_mol', 'hgb', 'microalbumin_24h_t', 'microalbumin_ur', 'umacr', 'glucose_ur',
                                                                  'alt', 'albumin', 'albumin_ur', 'albumin_ur_24h_t', 'hgb_a1c', 'calcium', 'calcium_ur', 'calcium_ionized', 'calcium_ur_24h', 'ast', 'pha', 'base_deficit',
                                                                  'bilirubin_dir', 'bilirubin_tot', 'crp', 'co2', 'carboxyhem', 'rbc_ur_v', 'uap_cat', 'o2sata', 'hct', 'hgb_electrophoresis', 'creatinine_ur_24h', 'chloride',
                                                                  'chloride_ur', 'chloride_ur_24h_t', 'rdw', 'chloride_ur_24h', 'potassium_ur_24h', 'sodium_ur_24h', 'creatinine', 'creatinine_ur', 'creatinine_ur_24h_t',
                                                                  'glucose', 'glucose_t', 'neutrophils_per', 'methem', 'eosinophils', 'eosinophils_per', 'rbc', 'wbc', 'lymphocytes', 'monocytes_per', 'neutrophils', 
                                                                  'neutrophils_band', 'neutrophils_per_band', 'plt', 'ph_ur', 'potassium', 'potassium_ur', 'potassium_ur_24h_t', 'p24', 'sodium', 'sodium_ur', 'sodium_ur_24h_t',
                                                                  'sg', 'microalbumin_24h', 'basophils_per', 'esr', 'mcv', 'bnp', 'bun_ur', 'bun_ur_24h_t', 'uncr', 'mpv', 'rbc_ur_pres', 'wbc_ur_pres', 'scr_ur_24h', 'p_panel',
                                                                  'inr', 'chpd_ur_24h', 'sodium_u_24hr', 'wbc_ur_sedim_l', 'wbc_ur_sedim', 'calcium_ionized_corr', 'bilirubin_tot_ur_pres', 'wbc_ur', 'potassium_ur_24h_mt',
                                                                  'hgb_ur', 'troponin_t', 'calcium_ur_24h_t', 'bilirubin_tot_ur', 'basophils', 'monocytes', 'mch', 'mchc', 'uacr', 'cacr_r_ur'].
                                 out_path : Union[str, None], optional
                                     File path to save the resultant product. The default is None.
                                 **logging_kwargs : TYPE
                                     kwargs for the logging program.

                                 Returns
                                 -------
                                 None.

                                 """
    # debug_inputs(function=omop_variable_generation, kwargs=locals(), dump_fp='vargen_kwargs.pkl')
    # raise Exception('stop here')
    modes: List[str] = ['admission', 'icu', 'surgery']

    assert mode in modes, f'Unsupported mode: {mode}. Please select from one of the following: {modes}'

    success_fp: str = os.path.join(dir_dict.get('status_files'), f'{project_name}_variable_generation_{batch_id}_success_')

    if os.path.exists(success_fp):
        return

    if isinstance(out_path, str):
        assert os.path.isdir(os.path.dirname(out_path))

    logm(message='Creating base dataframe', **logging_kwargs)

    patterns: List[str] = [r'_{}\.csv'.format(batch_id), r'_{}_chunk_[0-9]+\.csv'.format(batch_id), r'_{}_[0-9]+\.csv'.format(batch_id)] if isinstance(batch_id, str) else [r'_[0-9]+_chunk_[0-9]+\.csv', r'_[0-9]+_[0-9]+\.csv', r'_[0-9]+\.csv', r'\.csv']

    lookup_table_df: pd.DataFrame = check_load_df(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'Resource_Files', 'variable_generation_lookup.xlsx'), sheet_name='omop_var_gen_final',
                                                  desired_types={'concept_id': 'sparse_int'})

    # load basic encounter information from the visit occurrence table
    base_df: pd.DataFrame = load_variables_from_var_spec(variables_columns=['visit_start_datetime', 'visit_end_datetime', 'admitted_from_concept_id', 'visit_concept_id', 'provider_id', 'visit_start_date'],
                                                         var_spec_key=var_file_linkage_fp,
                                                         dir_dict=dir_dict,
                                                         filter_variables=False,
                                                         # max_workers=1,
                                                         append_subject_id_type_if_missing=subject_id_type if subject_id_type == eid else None,
                                                         data_source_key=data_source_key,
                                                         cdm_tables=['visit_occurrence'],
                                                         project='Variable_Generation',
                                                         mute_duplicate_var_warnings=True,
                                                         desired_types={**{x: 'datetime' for x in ['visit_start_datetime', 'visit_end_datetime']},
                                                                        **{x: 'sparse_int' for x in [pid, eid, 'admitted_from_concept_id', 'visit_concept_id', 'provider_id', 'subject_id']}},
                                                         patterns=patterns,
                                                         allow_empty_files=True, regex=True, dtype=None, ds_type='pandas', **logging_kwargs).drop_duplicates(subset=['subject_id'])\
        .rename(columns={'provider_id': 'visit_provider_id', 'visit_start_date': 'encounter_effective_date'})\
        .merge(load_variables_from_var_spec(variables_columns=['seen_in_ed_yn'],
                                                             var_spec_key=var_file_linkage_fp,
                                                             dir_dict=dir_dict,
                                                             filter_variables=False,
                                                             # max_workers=1,
                                                             id_vars=['subject_id'],
                                                             append_subject_id_type_if_missing=subject_id_type if subject_id_type == eid else None,
                                                             data_source_key=data_source_key,
                                                             cdm_tables=['visit_occurrence'],
                                                             project='Variable_Generation',
                                                             mute_duplicate_var_warnings=True,
                                                             desired_types={**{x: 'datetime' for x in ['visit_start_datetime', 'visit_end_datetime']},
                                                                            **{x: 'sparse_int' for x in [pid, eid, 'admitted_from_concept_id', 'visit_concept_id', 'provider_id', 'subject_id']}},
                                                             patterns=patterns,
                                                             allow_empty_files=True, regex=True, dtype=None, ds_type='pandas', **logging_kwargs)\
            .drop_duplicates(subset=['subject_id']).drop(columns=['variable_name']),
            on='subject_id', how='left')
   

    if mode in ['icu', 'surgery']:
        if subject_id_type == 'procedure_occurrence_id':
            assert mode == 'surgery'
            base_df: pd.DataFrame = base_df.merge(load_variables_from_var_spec(variables_columns=['primary_procedure', 'procedure_datetime',	'procedure_end_datetime',
                                                                'intraop_rvu', 'intraop_rvu_adjusted',
                                                                'provider_id', 'specialty_concept_id'],
                                                                               # 'procedure_concept_id',
                                                                               dir_dict=dir_dict,
                                                                               # max_workers=1,
                                                                               var_spec_key=var_file_linkage_fp,
                                                                               data_source_key=data_source_key,
                                                                               filter_variables=True,
                                                                               id_vars=['subject_id'],
                                                                               project='Variable_Generation',
                                                                               cdm_tables=['procedure_occurrence'],
                                                                               mute_duplicate_var_warnings=True,
                                                                               desired_types={**{x: 'datetime' for x in ['procedure_datetime',	'procedure_end_datetime']},
                                                                                              **{x: 'sparse_int' for x in [pid, eid, 'visit_detail_id', 'provider_id', 'subject_id']}},
                                                                               patterns=patterns,
                                                                               allow_empty_files=True,
                                                                               regex=True, dtype=None,
                                                                               ds_type='pandas',
                                                                               **logging_kwargs)\
                .drop(columns=['variable_name']).rename(columns={'provider_id': f'{mode}_provider_id',
                                                                 'procedure_datetime': f'{mode}_start_datetime',
                                                                 'procedure_end_datetime': f'{mode}_end_datetime'}),
                                                             
                on='subject_id', how='inner')
            base_df['visit_detail_type'] = mode
            
            
        else:
            # load basic surgery/icu information from visit detail table
            base_df: pd.DataFrame = base_df.merge(load_variables_from_var_spec(variables_columns=[mode, 'visit_detail_start_datetime', 'visit_detail_end_datetime', 'variable_name', 'provider_id', 'visit_detail_id'],
                                                                               dir_dict=dir_dict,
                                                                               # max_workers=1,
                                                                               var_spec_key=var_file_linkage_fp,
                                                                               data_source_key=data_source_key,
                                                                               filter_variables=False,
                                                                               project='Variable_Generation',
                                                                               cdm_tables=['visit_detail'],
                                                                               mute_duplicate_var_warnings=True,
                                                                               desired_types={**{x: 'datetime' for x in ['visit_detail_start_datetime', 'visit_detail_end_datetime']},
                                                                                              **{x: 'sparse_int' for x in [pid, eid, 'visit_detail_id', 'provider_id', 'subject_id']}},
                                                                               patterns=patterns,
                                                                               allow_empty_files=True,
                                                                               regex=True, dtype=None,
                                                                               ds_type='pandas',
                                                                               **logging_kwargs)
                                                  .query('subject_id == visit_detail_id' if exact_subject_mode else f'variable_name == {mode}')
                                                  .dropna(subset=[pid, eid, 'visit_detail_start_datetime', 'visit_detail_end_datetime', 'visit_detail_id'], how='any')
                                                  .rename(columns={'provider_id': f'{mode}_provider_id',
                                                                   'visit_detail_start_datetime': f'{mode}_start_datetime',
                                                                   'visit_detail_end_datetime': f'{mode}_end_datetime',
                                                                   'variable_name': 'visit_detail_type'})
                                                  .drop_duplicates(subset=['subject_id']),
                                                  how='inner',
                                                  on=['subject_id', pid, eid])

        # load visit_detail related information from the observation table
        base_df: pd.DataFrame = base_df.merge(_coalesce_convert__aggregrate_and_pivot(load_variables_from_var_spec(variables_columns=['surgical_service', 'scheduled_surgical_service', 'value_as_string', 'procedure_urgency',
                                                                                                                                      'sched_start_datetime', 'scheduled_anesthesia_type', 'sched_post_op_location', 'surgery_type'],
                                                                                                                   dir_dict=dir_dict,
                                                                                                                   # max_workers=1,
                                                                                                                   var_spec_key=var_file_linkage_fp,
                                                                                                                   data_source_key=data_source_key,
                                                                                                                   project='Variable_Generation',
                                                                                                                   cdm_tables=['observation'],
                                                                                                                   mute_duplicate_var_warnings=True,
                                                                                                                   desired_types={**{x: 'datetime' for x in ['visit_detail_start_datetime', 'visit_detail_end_datetime']},
                                                                                                                                  **{x: 'sparse_int' for x in [pid, eid, 'visit_detail_id', 'value_as_concept_id', 'subject_id']}},
                                                                                                                   patterns=patterns,
                                                                                                                   allow_empty_files=True,
                                                                                                                   regex=True, dtype=None,
                                                                                                                   ds_type='pandas',
                                                                                                                   **logging_kwargs),
                                                                                      index_cols=['subject_id'],
                                                                                      value_cols={**{'sched_start_datetime': ['value_as_concept_id', 'value_as_string']},
                                                                                                  **{x: ['value_as_concept_id'] for x in ['surgical_service', 'scheduled_surgical_service', 'procedure_urgency',
                                                                                                                                          'scheduled_anesthesia_type', 'sched_post_op_location', 'surgery_type']}},
                                                                                      id_col='variable_name')
                                              .drop_duplicates(subset=['subject_id']),
                                              how='left',
                                              on=['subject_id'])

        # load primary procedure and anesthesia type from the payer procedure table
        base_df: pd.DataFrame = base_df.merge(_coalesce_convert__aggregrate_and_pivot(load_variables_from_var_spec(variables_columns=['primary_procedure', 'anesthesia_type'],
                                                                                                                   dir_dict=dir_dict,
                                                                                                                   # max_workers=1,
                                                                                                                   var_spec_key=var_file_linkage_fp,
                                                                                                                   data_source_key=data_source_key,
                                                                                                                   project='Variable_Generation',
                                                                                                                   cdm_tables=['procedure_occurrence'],
                                                                                                                   mute_duplicate_var_warnings=True,
                                                                                                                   desired_types={x: 'sparse_int' for x in [pid, eid, 'visit_detail_id', 'procedure_concept_id', 'subject_id', 'procedure_source_concept_id']},
                                                                                                                   patterns=patterns,
                                                                                                                   allow_empty_files=True,
                                                                                                                   regex=True, dtype=None,
                                                                                                                   ds_type='pandas',
                                                                                                                   **logging_kwargs)
                                                                                      .merge(lookup_table_df.query('var_gen_name == "anesthesia_type"')[['concept_id', 'var_gen_value']].rename(columns={'concept_id': 'procedure_concept_id'}).drop_duplicates().dropna(),
                                                                                             on='procedure_concept_id',
                                                                                             how='left'),
                                                                                      index_cols=['subject_id'],
                                                                                      id_col='variable_name',
                                                                                      aggregation_priority={'anesthesia_type': ['GENERAL', 'REGIONAL/LOCAL']},
                                                                                      value_cols={'primary_procedure': ['procedure_concept_id' if subject_id_type == 'procedure_occurrence_id' else 'procedure_source_concept_id'],
                                                                                                  'anesthesia_type': ['var_gen_value', 'procedure_concept_id']})
                                              .drop_duplicates(subset=['subject_id']),
                                              how='left',
                                              on=['subject_id'])
        
        if (mode == 'surgery') and (subject_id_type != 'procedure_occurrence_id'):
            # load provider specialty from the provider table
            base_df: pd.DataFrame = base_df.merge(load_variables_from_var_spec(variables_columns=['specialty_concept_id'],
                                                                               dir_dict=dir_dict,
                                                                               var_spec_key=var_file_linkage_fp,
                                                                               data_source_key=data_source_key,
                                                                               project='Variable_Generation',
                                                                               cdm_tables=['provider'],
                                                                               mute_duplicate_var_warnings=True,
                                                                               desired_types={x: 'sparse_int' for x in [pid, eid, 'visit_detail_id', 'specialty_concept_id', 'subject_id']},
                                                                               patterns=patterns,
                                                                               allow_empty_files=True,
                                                                               regex=True, dtype=None,
                                                                               ds_type='pandas',
                                                                               **logging_kwargs)
                                                  .drop_duplicates(subset=['subject_id']),
                                                  how='left',
                                                  on=['subject_id'])

    elif (mode in ['admission']) and exact_subject_mode:

        base_df.query('visit_occurrence_id == subject_id', inplace=True)

    # load encounter information from the observation table
    base_df: pd.DataFrame = base_df.merge(pd.pivot(load_variables_from_var_spec(variables_columns=['admit_priority', 'admitting_service', 'language', 'marital_status', 'smoking_status', 'admit_Priority'],
                                                                                dir_dict=dir_dict,
                                                                                var_spec_key=var_file_linkage_fp,
                                                                                data_source_key=data_source_key,
                                                                                project='Variable_Generation',
                                                                                cdm_tables=['observation'],
                                                                                filter_variables=True,
                                                                                # max_workers=1,
                                                                                mute_duplicate_var_warnings=True,
                                                                                desired_types={**{x: 'datetime' for x in ['visit_detail_start_datetime', 'visit_detail_end_datetime']},
                                                                                               **{x: 'sparse_int' for x in [pid, eid, 'visit_detail_id', 'provider_id', 'subject_id', 'value_as_concept_id']}},
                                                                                patterns=patterns,
                                                                                allow_empty_files=True,
                                                                                regex=True, dtype=None,
                                                                                ds_type='pandas',
                                                                                **logging_kwargs),
                                                   index=['subject_id'],
                                                   columns='variable_name',
                                                   values='value_as_concept_id').reset_index(drop=False)
                                          .drop_duplicates(subset=['subject_id']),
                                          how='left',
                                          on=['subject_id'])

    base_df.columns = [x.lower() for x in base_df.columns.tolist()]

    # load patient and facilty geodata from location table
    base_df: pd.DataFrame = base_df.merge(load_variables_from_var_spec(variables_columns=['patient_zip', 'facility_zip', 'county'],
                                                                       dir_dict=dir_dict,
                                                                       var_spec_key=var_file_linkage_fp,
                                                                       data_source_key=data_source_key,
                                                                       project='Variable_Generation',
                                                                       cdm_tables=['location'],
                                                                       # max_workers=1,
                                                                       mute_duplicate_var_warnings=True,
                                                                       desired_types={x: 'sparse_int' for x in [pid, eid, 'visit_detail_id', 'patient_zip', 'subject_id', 'facility_zip']},
                                                                       patterns=patterns,
                                                                       allow_empty_files=True,
                                                                       regex=True, dtype=None,
                                                                       ds_type='pandas',
                                                                       **logging_kwargs)\
                                          .drop(columns=[eid], errors='ignore')\
                                          .groupby('subject_id')\
                                          .first()\
                                          .reset_index(drop=False),
                                          how='left',
                                          on=['subject_id', pid])

    # load patient data from the person table
    base_df: pd.DataFrame = base_df.merge(load_variables_from_var_spec(variables_columns=['gender_concept_id', 'race_concept_id', 'ethnicity_concept_id', 'birth_date'],
                                                                       dir_dict=dir_dict,
                                                                       var_spec_key=var_file_linkage_fp,
                                                                       data_source_key=data_source_key,
                                                                       project='Variable_Generation',
                                                                       # max_workers=1,
                                                                       cdm_tables=['person'],
                                                                       mute_duplicate_var_warnings=True,
                                                                       desired_types={x: 'sparse_int' for x in [pid, eid, 'visit_detail_id', 'patient_zip', 'subject_id', 'facility_zip']},
                                                                       patterns=patterns,
                                                                       allow_empty_files=True,
                                                                       regex=True, dtype=None,
                                                                       ds_type='pandas',
                                                                       **logging_kwargs)
                                          .drop_duplicates(subset=['subject_id']),
                                          how='left',
                                          on=['subject_id', pid])

    

    # load payer from the payer plan period table
    base_df: pd.DataFrame = base_df.merge(load_variables_from_var_spec(variables_columns=['payer_concept_id', 'payer'],
                                                                       dir_dict=dir_dict,
                                                                       # max_workers=1,
                                                                       id_vars=['subject_id'],
                                                                       var_spec_key=var_file_linkage_fp,
                                                                       data_source_key=data_source_key,
                                                                       project='Variable_Generation',
                                                                       cdm_tables=['payer_plan_period'],
                                                                       append_subject_id_type_if_missing=subject_id_type if subject_id_type == eid else None,
                                                                       mute_duplicate_var_warnings=True,
                                                                       desired_types={x: 'sparse_int' for x in [pid, eid, 'visit_detail_id', 'specialty_concept_id', 'subject_id', 'payer_concept_id', 'payer']},
                                                                       patterns=patterns,
                                                                       allow_empty_files=True,
                                                                       regex=True, dtype=None,
                                                                       ds_type='pandas',
                                                                       **logging_kwargs)
                                          .drop_duplicates(subset=['subject_id']),
                                          how='left',
                                          on=['subject_id'])\
        .rename(columns={'payer': 'payer_concept_id'})

    # load most recent height and weight from
    base_df: pd.DataFrame = base_df.merge(_coalesce_convert__aggregrate_and_pivot(load_variables_from_var_spec(variables_columns=['height', 'weight'],
                                                                                                               dir_dict=dir_dict,
                                                                                                               # max_workers=1,
                                                                                                               id_vars=['subject_id'],
                                                                                                               var_spec_key=var_file_linkage_fp,
                                                                                                               data_source_key=data_source_key,
                                                                                                               project='Variable_Generation',
                                                                                                               cdm_tables=['measurement'],
                                                                                                               mute_duplicate_var_warnings=True,
                                                                                                               desired_types={**{x: 'sparse_int' for x in [pid, eid, 'visit_detail_id', 'subject_id', 'unit_concept_id']},
                                                                                                                              **{'value_as_number': 'float'}},
                                                                                                               patterns=patterns,
                                                                                                               allow_empty_files=True,
                                                                                                               regex=True, dtype=None,
                                                                                                               ds_type='pandas',
                                                                                                               **logging_kwargs),
                                                                                  value_cols=['value_as_number'],
                                                                                  index_cols='subject_id',
                                                                                  id_col='variable_name',
                                                                                  conversion_dict={'weight': {'standard_unit': '9529', '8739': 0.453592, '9373': (1/35.274)}, # conversion for pounds and ounces, respectively, to kilograms
                                                                                                   'height': {'standard_unit': '8582', '9330': 2.54}})
                                          .rename(columns={'height': 'height_cm', 'weight': 'weight_kg'})
                                          .drop_duplicates(subset=['subject_id']),
                                          how='left',
                                          on=['subject_id'])

    # assign facilty zip if missing
    base_df.facility_zip.fillna(default_facility_zip, inplace=True)

    # loads labs
    labs_df = load_variables_from_var_spec(variables_columns=['measurement_datetime', 'value_source_value'] + labs,
                                           dir_dict=dir_dict,
                                           # max_workers=1,
                                           var_spec_key=var_file_linkage_fp,
                                           data_source_key=data_source_key,
                                           project='Variable_Generation',
                                           cdm_tables=['measurement'],
                                           mute_duplicate_var_warnings=True,
                                           desired_types={**{x: 'sparse_int' for x in [pid, eid, 'visit_detail_id', 'specialty_concept_id', 'subject_id',]},
                                                          **{'measurement_datetime': 'datetime', 'value_as_number': 'float'}},
                                           patterns=patterns,
                                           allow_empty_files=True,
                                           regex=True, dtype=None,
                                           ds_type='pandas',
                                           allow_empty_returns=True,
                                           **logging_kwargs)
    if isinstance(labs_df, pd.DataFrame):
        labs_df = labs_df[labs_df.subject_id.isin(base_df.subject_id.unique())].reset_index(drop=True)

    # load pre-admission medications
    meds_df = load_variables_from_var_spec(variables_columns=['drug_exposure_start_datetime',
                                                              'drug_exposure_end_datetime',
                                                              'asprin',
                                                              'statins',
                                                              'aminoglycosides',
                                                              'aceis_arbs',
                                                              'diuretics',
                                                              'nsaids',
                                                              'pressors_inotropes',
                                                              'opioids',
                                                              'vancomycin',
                                                              'beta_blockers',
                                                              'antiemetics',
                                                              'bicarbonates'],
                                           dir_dict=dir_dict,
                                           # max_workers=1,
                                           var_spec_key=var_file_linkage_fp,
                                           data_source_key=data_source_key,
                                           project='Variable_Generation',
                                           cdm_tables=['drug_exposure'],  # TODO: Consider switching to drug era in future versions which will be more effecient
                                           mute_duplicate_var_warnings=True,
                                           desired_types={**{x: 'sparse_int' for x in [pid, eid, 'visit_detail_id', 'specialty_concept_id', 'subject_id',]},
                                                          **{'measurement_datetime': 'datetime', 'value_as_number': 'float'}},
                                           patterns=patterns,
                                           allow_empty_files=True,
                                           regex=True, dtype=None,
                                           ds_type='pandas',
                                           **logging_kwargs)
    meds_df = meds_df[meds_df.subject_id.isin(base_df.subject_id.unique())].reset_index(drop=True)

    # load pre-admission diagnoses
    diagnoses_df = load_variables_from_var_spec(variables_columns=['charlson_comorbidity_mi',
                                                                   'cv_hf',
                                                                   'pvd',
                                                                   'cerebrovascular_disease',
                                                                   'dementia',
                                                                   'chronic_pulmonary_disease',
                                                                   'rheumatologic_disease',
                                                                   'pud',
                                                                   'mild_liver_disease',
                                                                   'diabetes_mild_to_moderate',
                                                                   'diabetes_with_chronic_complications',
                                                                   'hemoplegia_or_paralegia',
                                                                   'renal_disease',
                                                                   'malignant_solid_tumor_without_metastisis',
                                                                   'moderate_to_severe_liver_disease',
                                                                   'metastatic_solid_tumor',
                                                                   'aids',
                                                                   'alcohol_abuse',
                                                                   'anemia_iron_deficiency_chronic_blood_loss',
                                                                   'cardiac_arrhythmia',
                                                                   'coagulopathy',
                                                                   'anemia_deficiency',
                                                                   'depression',
                                                                   'drug_abuse',
                                                                   'lytes',
                                                                   'complicated_hypertension',
                                                                   'hypothyroidism',
                                                                   'essential_hypertension',
                                                                   'lymphoma',
                                                                   'obesity',
                                                                   'other_neurological_disorder',
                                                                   'pulmonary_circulation_disease',
                                                                   'psychotic_disorder',
                                                                   'heart_valve_disease',
                                                                   'malnutrition_macronutrients_weightloss',
                                                                   'poa', 'condition_start_date'],
                                                dir_dict=dir_dict,
                                                var_spec_key=var_file_linkage_fp,
                                                # max_workers=1,
                                                data_source_key=data_source_key,
                                                project='Variable_Generation',
                                                cdm_tables=['condition_occurrence'],  # TODO: Consider switching to condition era in future versions which will be more effecient
                                                mute_duplicate_var_warnings=True,
                                                desired_types={**{x: 'sparse_int' for x in [pid, eid, 'visit_detail_id', 'specialty_concept_id', 'subject_id']},
                                                               **{'condition_start_date': 'date'}},
                                                patterns=patterns,
                                                allow_empty_files=True,
                                                regex=True, dtype=None,
                                                ds_type='pandas',
                                                **logging_kwargs)
    diagnoses_df = diagnoses_df[diagnoses_df.subject_id.isin(base_df.subject_id.unique())].reset_index(drop=True)

    aki_phenotype_df: pd.DataFrame = check_load_df(f'{eid}_without_race_correction_v2_final_aki',
                                                   directory=dir_dict.get(aki_phenotype_data_key),
                                                   patterns=patterns,
                                                   desired_types={'aki_datetime': 'datetime',
                                                                  'inferred_specimen_datetime': 'datetime'},
                                                   ds_type='pandas',
                                                   **logging_kwargs)
    if not isinstance(aki_phenotype_df, pd.DataFrame):
        # run aki phenotype
        main_run_AKI_CKD_Phenotyping(dir_dict=dir_dict,
                                     race_corrections=[False],
                                     eids=[eid],
                                     pid=pid,
                                     append_subject_id_type_if_missing=subject_id_type if subject_id_type == eid else None,
                                     independent_sub_batch=True,
                                     success_fp=os.path.join(dir_dict.get('status_files'), f'AKI_Phenotype_success_{batch_id}_' if isinstance(batch_id, str) else 'AKI_Phenotype_success_'),
                                     version=2,
                                     project_name=project_name,
                                     regex=True,
                                     batches=[batch_id] if isinstance(batch_id, str) else None,
                                     serial=True,
                                     max_workers=1)

        aki_phenotype_df: pd.DataFrame = check_load_df(f'{eid}_without_race_correction_v2_final_aki',
                                                       directory=dir_dict.get(aki_phenotype_data_key),
                                                       patterns=patterns,
                                                       allow_empty_files=True,
                                                       desired_types={'aki_datetime': 'datetime',
                                                                      'inferred_specimen_datetime': 'datetime',
                                                                      pid: 'sparse_int'},
                                                       ds_type='pandas',
                                                       **logging_kwargs)

        aki_phenotype_df = aki_phenotype_df.merge(base_df[['subject_id', pid]], on=pid, how='inner').reset_index(drop=True)

    df = _check_run_display(df=base_df, msg='Stardizing Input Variables', func=standardize_variables,
                            lookup_table_df=lookup_table_df,
                            mode=mode,
                            **logging_kwargs).copy(deep=True)

    df = _check_run_display(df=check_load_df(input_v=df.rename(columns={'patient_zip': 'zip'}),
                                             desired_types={'facility_zip': 'format_sparse_int'}),
                            msg='Gemerating Residency Variables',
                            func=generate_residency_variables,
                            zcta_df=check_load_df(input_v=os.path.join(os.path.dirname(os.path.dirname(__file__)), 'Resource_Files', 'ZCTA.csv'),
                                                  desired_types={'zip': 'sparse_int'},
                                                  ds_type='pandas', **logging_kwargs).drop_duplicates(subset=['zip'], keep='first')
                            .rename(columns={'z_c_t_a': 'zcta'}),
                            zipcoord=check_load_df(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'Resource_Files', 'US.txt'),
                                                   desired_types={'postal_code': 'sparse_int', 'latitude': 'float', 'longitude': 'float'},
                                                   usecols=['postal_code', 'latitude', 'longitude'],
                                                   ds_type='pandas', **logging_kwargs),
                            **logging_kwargs).copy(deep=True)

    df = _check_run_display(df=df, msg='Generating Medication Variables', func=meds_generation_v2,
                            meds_df=meds_df,
                            intervals={'pre_admission': 'visit_start_datetime', f'pre_{mode}': f'{mode}_start_datetime'} if mode in ['icu', 'surgery'] else {'pre_admission': 'visit_start_datetime'},
                            meds=['asprin', 'statins', 'AMINOGLYCOSIDES', 'ACEIs_ARBs', 'diuretics', 'nsaids', 'pressors_inotropes',
                                  'OPIOIDS', 'vancomycin', 'beta_blockers', 'antiemetics', 'bicarbonates']).copy(deep=True)

    if df.shape[0] > 0:

        df = _check_run_display(df=df, msg='Generating Charlson and Elixaur Comorbidities', func=calculate_charlson_elixhauser_comorbidity_indicies_v2,
                                condition_df=diagnoses_df,
                                scoring_df=check_load_df(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'Resource_Files', 'variable_generation_lookup.xlsx'),
                                                         preserve_case=True,
                                                         sheet_name='comorbidity_indicies', ds_type='pandas', **logging_kwargs),
                                reference_points={'poa': 'visit_start_datetime'},
                                **logging_kwargs).copy(deep=True)

        df = _check_run_display(df=df, msg='Generating labs Variables', func=extract_laboratory_variables,
                                labs_df=labs_df,
                                reference_date_col=f'{mode}_start_datetime' if mode in ['icu', 'surgery'] else 'visit_start_datetime',
                                unique_index_col='subject_id',
                                pid=pid,
                                **logging_kwargs).copy(deep=True)

    df = _check_run_display(df=df.copy(), msg='Generating CKD Variables', func=generate_cdk_results, **logging_kwargs, pid=pid, ckd_df=aki_phenotype_df,
                            time_index_col='inferred_specimen_datetime', reference_dt_col=f'{mode}_start_datetime' if mode in ['icu', 'surgery'] else 'visit_start_datetime', unique_index_col='subject_id')

    # cleanup
    df = df.dropna(subset=[eid]).drop_duplicates().fillna('missing')

    # df.drop(columns=['distance_from_shands', 'admitted_from_concept_id', 'admitting_service', 'admit_priority', 'ethnicity_concept_id', 'sched_post_op_location',
    #                  'race_concept_id', 'gender_concept_id', 'surgical_service'], inplace=True, errors='ignore')

    if isinstance(out_path, str):
        save_data(df=df,
                  out_path=out_path,
                  **logging_kwargs)

    if isinstance(success_fp, str):
        open(success_fp, 'a').close()



def _coalesce_convert__aggregrate_and_pivot(df: pd.DataFrame, value_cols: Union[List[str], Dict[str, Union[List[str], str]]], index_cols: List[str], id_col: str,
                                            aggregation_priority: Union[Dict[str, List[str]], None] = None,
                                            conversion_dict: Union[Dict[str, Dict[str, any]], None] = None) -> pd.DataFrame:
    if df.shape[0] == 0:
        if len(value_cols) > 0:
            v_cols: List[str] = list(value_cols.keys()) if isinstance(value_cols, dict) else value_cols
            df[v_cols] = None
        else:
            v_cols: List[str] = [] 

        return df[[id_col] + index_cols + v_cols ]

    # Ensure aggregation_priority is a dictionary
    if isinstance(value_cols, list):
        df['xxxValuexxx'] = df.apply(lambda row: coalesce(*[row[x] for x in value_cols]), axis=1)

    elif isinstance(value_cols, dict):
        df['xxxValuexxx'] = df.apply(lambda row: coalesce(*[row[x] for x in ([value_cols.get(row[id_col])] if isinstance(value_cols.get(row[id_col]), str) else value_cols.get(row[id_col]))]), axis=1)
 
    if isinstance(conversion_dict, dict):
        df = df.apply(_convert_units, var_key=id_col, unit_key='unit_concept_id', value_key='xxxValuexxx', conversion_dict=conversion_dict, axis=1)
 
    try:
        return pd.pivot(df, index=index_cols, columns=id_col, values='xxxValuexxx').reset_index(drop=False)

    except ValueError:
        tl: List[pd.DataFrame] = []
        for v in df[id_col].unique():
            tl.append(df.query(f'{id_col} == "{v}"').groupby(index_cols + [id_col])['xxxValuexxx'].apply(_return_first_match, (aggregation_priority if isinstance(aggregation_priority, dict) else {}).get(v, [])).reset_index(drop=False))
        return pd.pivot(pd.concat(tl, axis=0, sort=False, ignore_index=True), index=index_cols, columns=id_col, values='xxxValuexxx').reset_index(drop=False)


def _return_first_match(s: pd.Series, rank_list: List[any]) -> any:
    """
    Return first value matching list. If no matches found, Return first notnull value.

    Parameters
    ----------
    s : pd.Series
        DESCRIPTION.
    rank_list : List[any]
        DESCRIPTION.

    Returns
    -------
    any
        DESCRIPTION.

    """
    for v in rank_list:
        if v in s:
            return v

    out = s.dropna()

    if len(out) > 0:
        return out.iloc[0]


def _convert_units(row: pd.Series, var_key: str, unit_key: str, value_key: str, conversion_dict: Dict[str, Dict[str, any]]) -> pd.Series:
    """
    Convert units to standard.

    Parameters
    ----------
    row : pd.Series
        Input row.
    var_key : str
        variable name key in the row.
    unit_key : str
        unit key in the row.
    value_key : str
        value key in the row.
    conversion_dict : Dict[str, Dict[str, any]]
        Dictionary of conversion factors. The format is as follows:
            {"variable_1": {"standard_unit": std_unit, "alternate_unit": alternate_conversion_factor},
             "variable_2": {"standard_unit": std_unit, "alternate_unit": alternate_conversion_factor}}

        e.g.
        {'weight': {'standard_unit': '9529', '8739': 0.453592},
         'height': {'standard_unit': '8582', '9330': 2.54}}

    Returns
    -------
    updated row with conversions.

    """
    std_unit: str = conversion_dict.get(row[var_key], {}).get('standard_unit')
    if (row[unit_key] == std_unit) or pd.isnull(row[unit_key]):
        return row

    conversion_factor: float = conversion_dict.get(row[var_key], {}).get(row[unit_key])
    assert pd.notnull(conversion_factor), f'There was no conversion factor found in order to convert unit: {row[unit_key]} to {std_unit}'

    row[value_key] = row[value_key] * conversion_factor
    row[unit_key] = std_unit

    return row


def _check_run_display(df, msg: str, func: callable, **kwargs):

    bfs: int = df.shape[0]
    logm(message=msg, display=kwargs.get('display', True),
         log_name=kwargs.get('log_name', 'IDEALIST_Variable_Generation'),
         log_dir=kwargs.get('log_dir'))
    df = func(df, **kwargs)
    afs: int = df.shape[0]

    if afs != bfs:
        logm(f'Merging Error! Expected: {bfs} rows, Found: {afs}', display=True, raise_exception=True, error=True,
             log_name=kwargs.get('log_name', 'IDEALIST_Variable_Generation'),
             log_dir=kwargs.get('log_dir'))

    return df

if __name__ == '__main__':
    pass
    # import pandas as pd
    # import os
    # from typing import Dict, Union, List
    # from Python.Utilities.FileHandling.variable_specification_utilities import load_variables_from_var_spec
    # from Python.Utilities.PreProcessing.data_format_and_manipulation import coalesce
    # from Python.Utilities.FileHandling.io import check_load_df, save_data
    # from Python.Utilities.Logging.log_messages import log_print_email_message as logm
    # from Python.Variable_Generation.Python.AKI_Phenotype.Python.main import main_run_AKI_CKD_Phenotyping
    # from Python.Variable_Generation.Python.variable_standardization_v2 import standardize_variables
    # from Python.Variable_Generation.Python.Residency import generate_residency_variables
    # from Python.Variable_Generation.Python.meds_v2 import meds_generation_v2
    # from Python.Variable_Generation.Python.comorbidites import calculate_charlson_elixhauser_comorbidity_indicies_v2
    # from Python.Variable_Generation.Python.labs import extract_laboratory_variables
    # from Python.Variable_Generation.Python.ckd_result import generate_cdk_results
    # from Python.Variable_Generation.Python.Utilities.General.func_utils import debug_inputs
    
    # from Python.Variable_Generation.Python.variable_generation_v2 import _check_run_display, _convert_units, _return_first_match, _coalesce_convert__aggregrate_and_pivot
