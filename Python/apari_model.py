# -*- coding: utf-8 -*-
"""
Created on Tue Jun 20 17:36:14 2023

@author: ruppert20
"""

import os
import pandas as pd
from typing import List
import random
from .Utilities.FileHandling.io import check_load_df, save_data
from .Model_Toolbox.Python.Pytorch.Training import train_model, get_predictions
from .Model_Toolbox.Python.DataSet import Dataset
from .Model_Toolbox.Python.Pytorch.Model_and_Layers import Model


def train_test_model(dir_dict: dict,
                     dset_names: List[str],
                     train_mode: bool = True,
                     n_gpus: int = 0,
                     n_quick_check: int = None,
                     n_data_load_workers: int = 2,
                     model_checkpoint_fp: str = None,
                     validation_group: str = 'Validation',
                     oversample_rate: int = 3,                    
                     icu_model_cols: List[str] = ['surgical_service_ct_surgery',
                                                'surgical_service_ob_gyn', 
                                                'pre_op_station_icu', 
                                                'surgical_service_general',
                                                'preop_e_sofa', 
                                                'surgical_service_vascular',
                                                'pre_op_station_ward',
                                                'surgical_service_neurosurgery',
                                                'admit_e_sofa', 
                                                'asa_score_numeric',
                                                'surgical_service_oral',
                                                'surgical_service_ortho', 
                                                'surgery_provider_id', 
                                                'primary_procedure', 
                                                'any_intraop_rbc',
                                                'adi_natrank', 
                                                'admit_priority_elective',
                                                'body_temperature', 
                                                'diastolic_blood_pressure', 
                                                'etco2', 
                                                'heart_rate', 
                                                'pip', 
                                                'respiratory_rate', 
                                                'spo2', 
                                                'systolic_blood_pressure',
                                                'tidal_volume',
                                                'poa_cci_aids', 
                                                'poa_diabetes',
                                                'poa_cci_pud', 
                                                'poa_icancer', 
                                                'poa_icvd',
                                                'poa_ichf',
                                                'poa_cci_dementia',
                                                'poa_cci_diabwc', 
                                                'poa_imi',
                                                'poa_imcancer',
                                                'poa_cci_msld',
                                                'poa_cci_hp', 
                                                'seen_in_ed_yn',
                                                'asa_1_plus',
                                                'asa_2_plus',
                                                'asa_3_plus',
                                                'asa_4_plus',
                                                'asa_5_plus',
                                                'admit_e_sofa_1_plus',
                                                'admit_e_sofa_2_plus', 
                                                'admit_e_sofa_3_plus', 
                                                'admit_e_sofa_4_plus', 
                                                'preop_e_sofa_1_plus', 
                                                'preop_e_sofa_2_plus', 
                                                'preop_e_sofa_3_plus', 
                                                'preop_e_sofa_4_plus',
                                                'any_preop_rbc',
                                                'admit_priority_emergent',
                                                'admit_priority_trauma_alert',
                                                'admit_priority_urgent', 
                                                'gender_concept_id_female',
                                                'pre_op_station_emergency_department',
                                                'pre_op_station_other',
                                                'poa_icpd', 
                                                'payer_concept_id_blue_cross', 
                                                'payer_concept_id_commercial',
                                                'payer_concept_id_federal_non_cms',
                                                'payer_concept_id_managed_care',
                                                'payer_concept_id_medicaid', 
                                                'payer_concept_id_medicaid_hmo',
                                                'payer_concept_id_medicare', 
                                                'payer_concept_id_medicare_hmo',
                                                'smoking_status_current', 
                                                'smoking_status_former',
                                                'smoking_status_never', 
                                                'smoking_status_unknown',
                                                'surgical_service_ent', 
                                                'surgical_service_podiatry',
                                                'surgical_service_tacs', 
                                                'surgical_service_urology',
                                                'procedure_urgency_urgent',
                                                'procedure_urgency_emergent', 
                                                'procedure_urgency_elective',
                                                'age', 
                                                'poa_cci',
                                                'adi_staterank',
                                                'preop_rbc_volume',
                                                'intraop_rbc_volume'],
                     mortality_model_cols: List[str] = ['poa_cci_aids',
                                                        'poa_diabetes',
                                                        'poa_cci_pud',
                                                        'poa_icancer',
                                                        'poa_icvd',
                                                        'poa_ichf',
                                                        'poa_cci_dementia',
                                                        'poa_cci_diabwc',
                                                        'poa_imi',
                                                        'poa_imcancer',
                                                        'poa_cci_msld',
                                                        'poa_cci_hp',
                                                        'seen_in_ed_yn',
                                                        'asa_score_numeric',
                                                        'asa_1_plus',
                                                        'asa_2_plus',
                                                        'asa_3_plus',
                                                        'asa_4_plus',
                                                        'asa_5_plus',
                                                        'admit_e_sofa_1_plus',
                                                        'admit_e_sofa_2_plus',
                                                        'admit_e_sofa_3_plus',
                                                        'admit_e_sofa_4_plus',
                                                        'preop_e_sofa_1_plus',
                                                        'preop_e_sofa_2_plus',
                                                        'preop_e_sofa_3_plus',
                                                        'preop_e_sofa_4_plus',
                                                        'any_preop_rbc',
                                                        'any_intraop_rbc',
                                                        'admit_priority_emergent',
                                                        'admit_priority_elective',
                                                        'admit_priority_trauma_alert',
                                                        'admit_priority_urgent',
                                                        'gender_concept_id_female',
                                                        'pre_op_station_ward',
                                                        'pre_op_station_icu',
                                                        'pre_op_station_emergency_department',
                                                        'pre_op_station_other',
                                                        'poa_icpd',
                                                        'payer_concept_id_blue_cross',
                                                        'payer_concept_id_commercial',
                                                        'payer_concept_id_federal_non_cms',
                                                        'payer_concept_id_managed_care',
                                                        'payer_concept_id_medicaid',
                                                        'payer_concept_id_medicaid_hmo',
                                                        'payer_concept_id_medicare',
                                                        'payer_concept_id_medicare_hmo',
                                                        'smoking_status_current',
                                                        'smoking_status_former',
                                                        'smoking_status_never',
                                                        'smoking_status_unknown',
                                                        'surgical_service_ent',
                                                        'surgical_service_general',
                                                        'surgical_service_neurosurgery',
                                                        'surgical_service_ob_gyn',
                                                        'surgical_service_oral',
                                                        'surgical_service_ortho',
                                                        'surgical_service_podiatry',
                                                        'surgical_service_ct_surgery',
                                                        'surgical_service_tacs',
                                                        'surgical_service_vascular',
                                                        'surgical_service_urology',
                                                        'procedure_urgency_urgent',
                                                        'procedure_urgency_emergent',
                                                        'procedure_urgency_elective',
                                                        'age',
                                                        'poa_cci',
                                                        'preop_e_sofa',
                                                        'admit_e_sofa',
                                                        'adi_natrank',
                                                        'adi_staterank',
                                                        'preop_rbc_volume',
                                                        'intraop_rbc_volume',
                                                        'primary_procedure',
                                                        'surgery_provider_id',
                                                        'body_temperature',
                                                        'diastolic_blood_pressure',
                                                        'etco2',
                                                        'heart_rate',
                                                        'pip',
                                                        'respiratory_rate',
                                                        'spo2',
                                                        'systolic_blood_pressure',
                                                        'tidal_volume']):

    if train_mode:
        for lr in [1e-4, 9e-5, 8e-5]:
            for batch_size in [32, 64]:
                for dropout in [0.1, 0.2, 0.4]:
                    for hidden_dim in [64, 74, 84]:
                        for outcome in ['hospital_mortality', 'prolonged_icu_stay']:
                            for dset_name in dset_names:

                                td: dict = check_load_df(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'Resource_Files', 'model_config.yaml'))
                                config: dict = td.get('model')
                                config['dataset'] = td.get('dataset')
                                config['dataset']['h5_file'] = os.path.join(dir_dict.get('dataset'), dset_name)
                                config['dataset']['target_outcome_index'] = outcome
                                config['dataset']['filter_columns'] = mortality_model_cols if outcome == 'hospital_mortality' else icu_model_cols

                                config.update({
                                    # Dataset
                                    'train_group': 'Development',
                                    'val_group': validation_group,
                                    'test_group': 'Test',

                                    # Model
                                    'hidden_dim': hidden_dim,
                                    'n_time_series_layers': 1,
                                    'target_idx': outcome,

                                    # Optimization and regularization
                                    'lr': lr,
                                    'dropout': dropout,
                                    # 'weight_decay': wd,
                                    # 'optimizer': optim,

                                    # Training and early stopping
                                    'batch_size': batch_size,
                                    'monitor': 'val_auprc_mean',
                                    'save_dir': os.path.join(dir_dict.get('model'), dset_name.split('_')[0]),

                                    # Misc
                                    'gpus': n_gpus,  # Number of GPUs to use #TODO: add ability to caclulate GPUS avaialble, then check the max between allowed and avaialble
                                    'n': n_quick_check,  # To quickly debug, set n to an integer # of samples for quickly making sure everything works
                                    'num_workers': n_data_load_workers,  # number of cpu cores to use for data loader
                                    'facility_zip': dset_name.split('_')[0]
                                })
                                if oversample_rate >= 2:
                                    config['dataset']['train_group_subset'] = oversample(h5_file=config.get('dataset').get('h5_file'),
                                                                                         outcome=outcome,
                                                                                         group_key=config.get('train_group'),
                                                                                         outcome_key='y',
                                                                                         oversample_rate=oversample_rate)
                                train_model(config)
    else:
        assert os.path.exists(model_checkpoint_fp)

        # load model
        model = Model.load_from_checkpoint(model_checkpoint_fp)

        # prepare dataset to spec
        for dset_name in dset_names:
            ds_dict: dict = model.config['dataset']
            ds_dict['h5_file'] = os.path.join(dir_dict.get('dataset'), dset_name)
            ds_dict['k_folds'] = None
            ds_dict['cohort'] = 'Validation'
            dset: Dataset = Dataset(**ds_dict)

            # make predictions
            predictions_df: pd.DataFrame = get_predictions(model, data=dset, config=model.config, return_actual=True)
            save_data(df=predictions_df,
                      out_path=os.path.join(dir_dict.get('model'), f'{dset.cohort}_predictions.csv'),
                      index=True)


def oversample(h5_file: str, outcome: str, group_key: str, outcome_key: str = 'y', oversample_rate: int = 3) -> List[int]:

    # ensure the oversample rate is an integer greater than or equal to 2
    assert isinstance(oversample_rate, int) and oversample_rate >=2, f'The oversample rate must be an integer greater than or equal to 2; however, {oversample_rate} was recieved'

    # load the outcome as a pandas series from the .h5 dataset file
    outcome_series: pd.Series = check_load_df(h5_file, dataset=outcome_key, group=group_key, columns=[outcome]).loc[:, outcome]
    
    # determine indexes of positive outcomes
    positive_idx: pd.Series = outcome_series == 1

    # check that there are positive outcomes to resample and make list of the integer indexes of them and then increase the size of the list to achieve the desired oversampling rate
    if positive_idx.any():
        oversample: List[int] = positive_idx.reset_index(drop=True).loc[positive_idx.values].reset_index()['index'].tolist() * (oversample_rate -1)

    else:
        print('There were no positive outcomes to oversample')
        oversample: List[int] = []

    # set the random seed to deterministically randomly sort the indexes to pass through to avoid biasing the training by clustering positive examples at the begining or the end
    random.seed(42)
    return random.shuffle(list(range(outcome_series.shape[0])) + oversample)
