# -*- coding: utf-8 -*-
"""
Created on Wed Mar 16 20:27:54 2022

@author: ruppert20
"""
from tqdm import tqdm
from captum.attr import IntegratedGradients
import torch
import shap
from .Model_and_Layers import Interpretable_Model
from ..DataSet import Dataset
import pandas as pd
import pickle


def _get_ig_sample(idx: int, data: Dataset, model: Interpretable_Model, E_baseline: torch.Tensor = None, embeded_tensor_pos: int = None):
    """
    Get tensorlist for integrated gradient computation.

    Parameters
    ----------
    idx : int
        sample index.
    data : Dataset
        dataset
        *must have function load_for_Interpretation which takes the idx and model as postional arguments and returns list of float tensors.
    model : Interpretable_Model
        Model obect for categorical embedding. The default is None.
    E_baseline : torch.Tensor, optional
        baseline embeddings tensor. Usually made by taking the mean of all the embeddings in the training set. The default is None.
    embeded_tensor_pos : int, optional
        position of the embedded tensor in the list returned from the load_for_Interpretation function from the dataset. The default is None.

    Returns
    -------
    tuple(tensors)
        tuple of tensors to feed into the intepretable model in order to compute the gradient.
    baseline : tuple(tensors)
        tuple of zero tensors which may include a non-zero embedding baseline tensor.

    """
    if (E_baseline is not None) or (embeded_tensor_pos is not None):
        assert torch.is_tensor(E_baseline), f'E_baseline must be a pytorch tensor, however; a {type(E_baseline)} was found'
        assert isinstance(embeded_tensor_pos, int), f'embeded_tensor_pos must be a pytorch tensor, however; a {type(embeded_tensor_pos)} was found'
    x = data.load_for_Interpretation(idx, model)

    baseline = [torch.zeros_like(z, dtype=z.dtype) for z in x]
    if isinstance(embeded_tensor_pos, int):
        assert embeded_tensor_pos < len(baseline), f'Embeded_tensor_pos: {embeded_tensor_pos} is out of range of the tensor list of length: {len(baseline)}'
        baseline[embeded_tensor_pos] = E_baseline
    baseline = tuple(baseline)

    return tuple(x), baseline


def get_gradients(data: Dataset, E_test: torch.Tensor, E_baseline: torch.Tensor, model: Interpretable_Model, embeded_tensor_pos: int = None):

    ig = IntegratedGradients(model)

    outcome_names: list = model.model.hparams.get('config').get('outcomes')

    A = {o: [] for o in outcome_names}

    dN = len(data)

    for sample_idx in tqdm(range(dN)):
        x, baseline = _get_ig_sample(idx=sample_idx, data=data, E_baseline=E_baseline, model=model, embeded_tensor_pos=embeded_tensor_pos)

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
            if outcome in output:
                output[outcome] = output[outcome].merge(temp, left_index=True, right_index=True)
            else:
                output[outcome] = pd.DataFrame(temp)

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
    from Utils.io import find_files
    import os
    directory: str = 'file _path'
    model_str: str = 'w_attn_learn_rate_0.001_optim_adam_wdecay_0.0001_dropout_0.2_num_layers_1_hidden_dim_64_time_steps_None_outcomes_[0, 1, 2]_metric_val_auroc_mean'

    model_ckpt: str = r'{}\.ckpt'.format(model_str.replace('[', r'\[').replace(']', r'\]').replace(' ', r'\s'))

    model: Interpretable_Model = Interpretable_Model(checkpoint=find_files(directory=directory,
                                                                           patterns=[model_ckpt],
                                                                           regex=True, recursive=True)[0])

    cohort: str = 'test_all'
    data = Dataset(cohort=cohort, h5_file=model.model.hparams.get('config').get('h5_file'),
                   target_outcome_index=model.model.hparams.get('config').get('target_idx'))

    data2 = Dataset(cohort=cohort, h5_file=model.model.hparams.get('config').get('h5_file'),
                    target_outcome_index=model.model.hparams.get('config').get('target_idx'))

    ex2 = data2.load_for_Interpretation(0, model)

    data2.column_names['x_admit_binary']

    ex = data.load_for_Interpretation(0, model)
    a = model(*ex)

    E_baseline = model.embed(data.collate_fn([data[x] for x in range(len(data))])).mean(dim=0).unsqueeze(0)

    dump_object(fp=os.path.join(directory, f'{model_str}_{cohort}_baseline_embeddings.pkl'), object_to_dump=E_baseline)

    grads = get_gradients(data=data, E_baseline=E_baseline, model=model)

    dump_object(fp=os.path.join(directory, f'{model_str}_{cohort}_integrated_gradients.pkl'), object_to_dump=grads)

    feature_values = get_feature_values(data2)

    A = get_attributions(data=data2, grads=grads, model=model)

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
