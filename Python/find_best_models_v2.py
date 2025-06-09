# -*- coding: utf-8 -*-
"""
Created on Fri Jan 24 09:32:36 2025

@author: diehu
"""

# -*- coding: utf-8 -*-
"""
Created on Wed Mar 13 06:37:20 2024

@author: tloftus
"""


import os
import pandas as pd
import json
import pytorch_lightning
import torch
import sys
sys.path.append(r"C:\Users\diehu\Documents\GitHub\APARI_AIM2_DEE\Python")

from Model_Toolbox.Python.Pytorch.Model_and_Layers import Model
from Model_Toolbox.Python.model_metrics import compute_model_performance

# sys.path.append(r"S:\2016_223 IDEALIST\4 PROJECTS\ACTIVE\9 Care Analysis Project Tyler\Dee\Active\APARI_2024_CODE\Git3\apari_aim2_mmai\Python\Model_Toolbox\Python\Pytorch")
# from Model_and_Layers import Model


path = r'S:\2016_223 IDEALIST\4 PROJECTS\ACTIVE\9 Care Analysis Project Tyler\Dee\Active\APARI_2024_DATA_V2\procedure_occurrence_test\Results\model\all'


model_list = [f.path for f in os.scandir(path) if f.is_dir()]

df = pd.DataFrame()

for model in model_list:
    path = model    
    iter_name = path.split('2025-', 1)[1]
    
    os.chdir(path)
    
    if os.path.isfile(path+'/predictions_model_Validation.csv'): 
        outcome_file = pd.read_csv('predictions_model_Validation.csv')
        outcome_column = outcome_file.columns[1]
        outcome = outcome_column.split('true_')[1]
        
        perform_table = open('Validation_performance.json')
        
        checkpoint_file = open('best_model_checkpoint.txt')
        
        with open('best_model_checkpoint.txt') as f: checkpoint_path = f.read()
        
        data = json.load(perform_table)
        
        auroc = data['test_auroc_mean']
        auroc_v2 = f'{auroc:.3f}'
        
        auprc = data['test_auprc_mean']
        auprc_v2 = f'{auprc:.3f}'
        
        row = {'outcome': [outcome], 'auroc': [auroc_v2], 'auprc': [auprc_v2], 'checkpoint_path': [checkpoint_path]}
        
        model_df = pd.DataFrame(data=row, index=[iter_name])
        #df = df.append(model_df)
        df = pd.concat([df, model_df])

        
    else:
        pass
        
mortality_pred_models = df[df['outcome'] == 'hospital_mortality']    
prolonged_icu_pred_models = df[df['outcome'] == 'prolonged_icu_stay']

# mortality_pred_models.sort_values(by='auprc', ascending=False, inplace=True) 
# mortality_pred_models.sort_values(by='auroc', ascending=False, inplace=True)
# prolonged_icu_pred_models.sort_values(by='auprc', ascending=False, inplace=True)
# prolonged_icu_pred_models.sort_values(by='auroc', ascending=False, inplace=True)

mortality_pred_models.sort_values(by=['auprc', 'auroc'], ascending=False, inplace=True)
prolonged_icu_pred_models.sort_values(by=['auprc', 'auroc'], ascending=False, inplace=True)

#save to path 
mortality_pred_models.to_csv(r"S:\2016_223 IDEALIST\4 PROJECTS\ACTIVE\9 Care Analysis Project Tyler\Dee\Active\APARI_2024_DATA_V2\procedure_occurrence_test\Results\model\models_ranked_by_auprc\mortality_models_ranked_by_auprc.csv")
prolonged_icu_pred_models.to_csv(r"S:\2016_223 IDEALIST\4 PROJECTS\ACTIVE\9 Care Analysis Project Tyler\Dee\Active\APARI_2024_DATA_V2\procedure_occurrence_test\Results\model\models_ranked_by_auprc\prolonged_icu_models_ranked_by_auprc.csv")

# get checkpoint path from best models
best_mortality_pred_checkpoint = mortality_pred_models.iloc[0]['checkpoint_path']
best_prolonged_icu_stay_pred_checkpoint = prolonged_icu_pred_models.iloc[0]['checkpoint_path']

# use pytorch lightning to load a model checkpoint and find hyperparameters
optimal_params_df = pd.DataFrame()

for checkpoint in [best_mortality_pred_checkpoint, best_prolonged_icu_stay_pred_checkpoint]:
    checkpoint = checkpoint.replace(r'/blue/prismap-ai-core/deehu/conda/envs/APARI1/APARI_models/APARI1/apari_data_dir/', 'S:/2016_223 IDEALIST/4 PROJECTS/ACTIVE/9 Care Analysis Project Tyler/Dee/Active/APARI_2024_DATA_V2/procedure_occurrence_test/')

#/blue/prismap-ai-core/vnolan/conda/envs/APARI1/apari_data_dir/
    model = Model.load_from_checkpoint(checkpoint,map_location=torch.device("cpu"))
    model.config
    
    outcome = model.config.get('outcomes')
    lr = model.config.get('lr')
    dropout = model.config.get('dropout')
    batch_size = model.config.get('batch_size')
    hidden_dim = model.config.get('hidden_dim')
    included_surgery_provider_id = "yes" if "surgery_provider_id" in model.config.get('dataset').get('filter_columns') else "no"
    
    row = {'Batch size': [batch_size], 'Dropout': [dropout], 'Hidden dimensions': [hidden_dim], 'Learning rate': [lr], 'Included surgeon identity': [included_surgery_provider_id]}
    model_param_df = pd.DataFrame(data=row, index=[outcome])
    
    #optimal_params_df = optimal_params_df.append(model_param_df)
    optimal_params_df = pd.concat([optimal_params_df, model_param_df])

optimal_params_df.to_csv(r"S:\2016_223 IDEALIST\4 PROJECTS\ACTIVE\9 Care Analysis Project Tyler\Dee\Active\APARI_2024_DATA_V2\procedure_occurrence_test\Results\model\models_ranked_by_auprc\optimal_params_df.csv")

# r"S:/2016_223 IDEALIST/4 PROJECTS/ACTIVE/9 Care Analysis Project Tyler/Dee/Active/APARI_2024_DATA//Results/model/all/2024-03-14_10-40-45/fold_3/model.ckpt"

# mortality_preds = pd.read_csv(r"S:\2016_223 IDEALIST\4 PROJECTS\ACTIVE\9 Care Analysis Project Tyler\Dee\Active\APARI_2024_DATA_V2\procedure_occurrence_test\Results\model\all\2025-01-16_07-51-16\predictions_model_Validation.csv")
# prolonged_icu_preds = pd.read_csv(r"S:\2016_223 IDEALIST\4 PROJECTS\ACTIVE\9 Care Analysis Project Tyler\Dee\Active\APARI_2024_DATA_V2\procedure_occurrence_test\Results\model\all\2025-01-15_23-16-47\predictions_model_Validation.csv")

# # performance_dict = {{mortality_preds, 'y_true_hospital_mortality', 'y_pred_hospital_mortality'},
# #                     {prolonged_icu_preds, 'y_true_prolonged_icu_stay', 'y_pred_prolonged_icu_stay'}}


# overall = compute_model_performance(df_dict={'mortality': {'df': mortality_preds,
#                                                             'true_label_col': 'y_true_hospital_mortality',
#                                                             'prediction_col': 'y_pred_hospital_mortality'},
#                                               'prolonged_icu': {'df': prolonged_icu_preds,
#                                                             'true_label_col': 'y_true_prolonged_icu_stay',
#                                                             'prediction_col': 'y_pred_prolonged_icu_stay'}},
#                                         model_name='APARI_UF_HEALTH_ALL')

# overall.to_csv(r"S:\2016_223 IDEALIST\4 PROJECTS\ACTIVE\9 Care Analysis Project Tyler\Dee\Active\APARI_2024_DATA_V2\procedure_occurrence_test\Results\model\models_ranked_by_auprc\performance_battery_for_dissertation_uf.csv")


# # read simulated FL results into memory

# fl_result = pd.read_json(r"S:\2016_223 IDEALIST\4 PROJECTS\ACTIVE\9 Care Analysis Project Tyler\Dee\Active\APARI_2024_DATA\Data\dataset\test_local_workspace\simulate_job\cross_site_val\cross_val_results.json")

# fl_result.to_csv(r"C:\Users\tloftus\Dropbox (UFL)\Research projects\3 R01 patient acuity, resource intensity files\Aim 1\Aim 1 external validation results\UF results\UF FL sim\uf_fl_sim.csv")
