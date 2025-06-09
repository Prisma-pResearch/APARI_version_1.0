# -*- coding: utf-8 -*-
"""
Created on Mon Jan 27 10:54:10 2025

@author: diehu
"""


# -*- coding: utf-8 -*-
"""
Created on Wed Mar 16 20:27:54 2022

@author: ruppert20
"""
from tqdm import tqdm
from captum.attr import IntegratedGradients
import torch
import shap

# print("Torch Version:", torch.__version__)
# print("SHAP Version:", shap.__version__)

# import sys
# print("Python Version:", sys.version)
# print("Python Version Info:", sys.version_info)

import sys
sys.path.append(r"C:\Users\diehu\Documents\GitHub\APARI_AIM2_DEE\Python")

# from .Model_and_Layers import Interpretable_Model
# from ..DataSet import Dataset
from Model_Toolbox.Python.Pytorch.Model_and_Layers import Interpretable_Model,Model
from Model_Toolbox.Python.DataSet import Dataset


import pandas as pd
import pickle



            
            
            
## 
def _get_ig_sample(idx: int, data: Dataset, E_baseline: torch.Tensor, model: Interpretable_Model):
    x = data.load_for_Interpretation(idx, model)

    baseline = [torch.zeros_like(z, dtype=z.dtype) for z in x]
    baseline[2] = E_baseline
    baseline = tuple(baseline)

    return tuple(x), baseline


def get_gradients(data: Dataset, E_baseline: torch.Tensor, model: Interpretable_Model):
    # ig = IntegratedGradients(ig_forward)
    ig = IntegratedGradients(model)

    outcome_names: list = ['ICU']  #model.model.hparams.get('config').get('outcomes')

    A = {o: [] for o in outcome_names}

    dN = len(data)

    for sample_idx in tqdm(range(dN)):
        x, baseline = _get_ig_sample(idx=sample_idx, data=data, E_baseline=E_baseline, model=model)

        for target_idx in range(len(outcome_names)):
            attributions = ig.attribute(x, baseline,
                                        # additional_forward_args=lens,
                                        target=target_idx)
            a_cpu = [a.detach().numpy() for a in attributions]
            A[outcome_names[target_idx]].append(a_cpu)

    return A


def get_attributions(data: Dataset, grads: dict, model):

    if len(data.cat_embedding_key) == 1:
        cat_key: str = list(data.cat_embedding_key.keys())[0]
        hdim: int = model.model.config.get('hidden_dim')
    else:
        cat_key: str = ''
    output: dict = {}
    for i in tqdm(range(data.N)):
        for j, outcome in enumerate(list(grads.keys())):
            A = grads[outcome][i]

            temp: pd.Series = pd.Series(dtype=float, name=i)

            for pos, k in enumerate(data.interpretable_keys):
                t = A[pos][0]

                if k in data.variable_length_seq_keys:
                    t = t.sum(axis=0)

                if data.cat_embedding_key.get(cat_key) == k:
                    for pos2, col in enumerate(data.column_names.get(cat_key)):
                        temp.loc[f'a_{col}'] = t[(pos2 * hdim):(pos2 + 1) * hdim].sum()
                else:
                    temp = temp.append(pd.Series(index=data.column_names.get(k), data=t)).rename(i)
            if j in output:
                output[j] = output[j].merge(temp, left_index=True, right_index=True)
            else:
                output[j] = pd.DataFrame(temp)

    return output


def get_feature_values(data: Dataset) -> pd.DataFrame:
    """
    Load Dataset into pandas dataframe.

    Parameters
    ----------
    data : Dataset
        DESCRIPTION.

    Returns
    -------
    pd.DataFrame
        Pandas dataframe with the index as the feature name and the columns as the individual samples.

    """
    return pd.concat([data.get_item_as_series(x) for x in tqdm(range(len(data)), desc='Loading feature values')], axis=1)


def load_attributions(att_fp: str):
    """
    Load pickled attributions and feature values.

    Parameters
    ----------
    att_fp : str
        DESCRIPTION.

    Returns
    -------
    ATTRIBUTIONS : TYPE
        DESCRIPTION.
    VALUES : TYPE
        DESCRIPTION.

    """
    with open(att_fp, 'rb') as f:
        ig = pickle.load(f)

    ATTRIBUTIONS = ig['A']
    VALUES = ig['feature_values']

    return ATTRIBUTIONS, VALUES


def dump_object(fp: str, object_to_dump: any):
    with open(fp, 'wb') as f:
        if isinstance(object_to_dump, torch.Tensor):
            pickle.dump(object_to_dump.numpy(), f, protocol=2)
        else:
            pickle.dump(object_to_dump, f, protocol=2)           
            

            
            
            



if __name__ == "__main__":
    #from Utils.io import find_files
    import os

    # directory: str = 'file _path'
    # model_str: str = 'w_attn_learn_rate_0.001_optim_adam_wdecay_0.0001_dropout_0.2_num_layers_1_hidden_dim_64_time_steps_None_outcomes_[0, 1, 2]_metric_val_auroc_mean'

    # model_ckpt: str = r'{}\.ckpt'.format(model_str.replace('[', r'\[').replace(']', r'\]').replace(' ', r'\s'))

    # model: Interpretable_Model = Interpretable_Model(checkpoint=find_files(directory=directory,
    #                                                                        patterns=[model_ckpt],
    #                                                                        regex=True, recursive=True)[0])
    
    
    
    prolonged_icu_ckpt = r"S:\2016_223 IDEALIST\4 PROJECTS\ACTIVE\9 Care Analysis Project Tyler\Dee\Active\APARI_2024_DATA_V2\procedure_occurrence_test\Results\model\all\2025-01-15_23-16-47\fold_1\model.ckpt"
    hospital_mortality_ckpt = r"S:\2016_223 IDEALIST\4 PROJECTS\ACTIVE\9 Care Analysis Project Tyler\Dee\Active\APARI_2024_DATA_V2\procedure_occurrence_test\Results\model\all\2025-01-16_07-51-16\fold_1\model.ckpt"

# For prolonged ICU model
    directory = os.path.dirname(prolonged_icu_ckpt)  # Folder containing the checkpoint
    model_str = os.path.basename(prolonged_icu_ckpt)  # File name of the checkpoint
    model_ckpt = prolonged_icu_ckpt  # Direct path to the checkpoint file
    

    #model = Interpretable_Model(checkpoint=model_ckpt)
    
 
    base_model = Model.load_from_checkpoint(checkpoint_path=model_ckpt, map_location=torch.device('cpu'))
    # Initialize the interpretable model
    model = Interpretable_Model(model=base_model)
    
  
      
    # Load the prolonged ICU model
    #model: Interpretable_Model = Interpretable_Model(checkpoint=model_ckpt)
    # Initialize the model

    #torch.cuda.is_available() 
 
    cohort: str = 'Development'
    h5_file_path = r'S:\2016_223 IDEALIST\4 PROJECTS\ACTIVE\9 Care Analysis Project Tyler\Dee\Active\APARI_2024_DATA_V2\procedure_occurrence_test\Data\dataset\all_APARI_dataset_v1.0.h5'  # Replace with the actual path to your dataset file
   
    # # data = Dataset(cohort=cohort, h5_file=model.model.hparams.get('config').get('h5_file'),
    #                 target_outcome_index=model.model.hparams.get('config').get('target_idx'))

    # data2 = Dataset(cohort=cohort, h5_file=model.model.hparams.get('config').get('h5_file'),
    #                 target_outcome_index=model.model.hparams.get('config').get('target_idx'))
    
    data = Dataset(
    cohort=cohort,
    h5_file=h5_file_path,
    target_outcome_index=model.model.hparams["config"]["dataset"]["target_outcome_index"],  # Extract from model
    batch_dset_map=model.model.hparams["config"]["dataset"]["batch_dset_map"],  # Extract from model
    filter_columns=model.model.hparams["config"]["dataset"]["filter_columns"],  # Extract from model
    h5_N_key=model.model.hparams["config"]["dataset"]["h5_N_key"],  # Extract from model
    start_key=model.model.hparams["config"]["dataset"]["start_key"],  # Extract from model
    seq_len_key=model.model.hparams["config"]["dataset"]["seq_len_key"],  # Extract from model
    interpretable_keys=['static_numeric', 'static_binary', 'static_cat_embedding', 'time_series_numeric'],  
    cat_embedding_key=model.model.hparams["config"]["dataset"]["cat_embedding_key"],  
    variable_length_seq_keys=model.model.hparams["config"]["dataset"]["variable_length_seq_keys"]
)


    ex = data.load_for_Interpretation(0, model)
   
    E_baseline = model.embed(data.collate_fn([data[x] for x in range(len(data))])).mean(dim=0).unsqueeze(0)
    
    dump_object(fp=os.path.join(directory, f'{model_str}_{cohort}_baseline_embeddings.pkl'), object_to_dump=E_baseline)
    
    # embeded_tensor_pos = list(model.model.hparams["config"]["dataset"]["cat_embedding_key"].values()).index("static_cat_embedding")

    # print(f"Setting embeded_tensor_pos to: {embeded_tensor_pos}")

    #grads = get_gradients(data=data, E_baseline=E_baseline[[0], 0:2], model=model, embeded_tensor_pos=2)
    grads = get_gradients(data=data, E_baseline=E_baseline, model=model)
    dump_object(fp=os.path.join(directory, f'{model_str}_{cohort}_integrated_gradients.pkl'), object_to_dump=grads)

    feature_values = get_feature_values(data)

    A = get_attributions(data=data, grads=grads, model=model)

    ig_comb: str = os.path.join(directory, f'{model_str}_{cohort}_ig_combined.pkl')

    dump_object(fp=ig_comb, object_to_dump={
        'A': A,
        'feature_values': feature_values
    })

    A, feature_values = load_attributions(att_fp=ig_comb)

    background = [torch.zeros_like(x, dtype=x.dtype) for x in ex]
    background[2] = E_baseline

    e2 = shap.GradientExplainer(model, background)

    shap_values = e2.shap_values(ex)

    shap.plots.beeswarm(shap_values[0])

    indv_icu_4 = shap._explanation.Explanation(values=A[0].loc[:, 0],
                                               data=feature_values[0].values,
                                               feature_names=feature_values[0].index,
                                               base_values=0)
    shap.plots.waterfall(indv_icu_4)

    shap.plots.force(indv_icu_4, matplotlib=True)
    shap.plots.bar(indv_icu_4)
    shap.decision_plot(indv_icu_4)

    agg_icu_4 = shap._explanation.Explanation(values=A[0].T,
                                              data=feature_values.values,
                                              feature_names=feature_values.index,
                                              base_values=0)
    shap.plots.beeswarm(agg_icu_4, log_scale=True)
    shap.plots.heatmap(agg_icu_4)
    shap.plots.violin(agg_icu_4)
    shap.plots.force(agg_icu_4, matplotlib=True)

    median_icu_4 = shap._explanation.Explanation(values=A[0].median(axis=1),
                                                 data=feature_values[0].values,
                                                 feature_names=feature_values.index,
                                                 base_values=0)

    mean_icu_4 = shap._explanation.Explanation(values=A[0].mean(axis=1),
                                               # data=feature_values[0].values,
                                               feature_names=feature_values.index,
                                               base_values=0)

    sum_icu_4 = shap._explanation.Explanation(values=A[0].sum(axis=1),
                                              # data=feature_values[0].values,
                                              feature_names=feature_values.index,
                                              base_values=0)

    shap.plots.force(median_icu_4, matplotlib=True)
    shap.plots.bar(median_icu_4, max_display=100)
    shap.plots.bar(mean_icu_4)
    shap.plots.bar(sum_icu_4)

    outcome = 1
    sum_normalized_icu_4 = shap._explanation.Explanation(values=A[outcome].sum(axis=1) / abs(A[outcome].sum(axis=1).sum(axis=0)),
                                                         # data=feature_values[0].values,
                                                         feature_names=feature_values.index,
                                                         base_values=0)
    shap.plots.bar(sum_normalized_icu_4)

    shap.plots.waterfall(sum_normalized_icu_4)
