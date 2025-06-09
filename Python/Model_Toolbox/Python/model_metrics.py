# -*- coding: utf-8 -*-
"""
Created on Tue Dec  7 15:32:39 2021.

@author: ruppert20
"""
import pandas as pd
import numpy as np
from sklearn.metrics import auc, roc_auc_score, roc_curve, confusion_matrix, precision_recall_curve
from typing import Union
from tqdm import tqdm
from .Utilities.ResourceManagement.parallelization_helper import run_function_in_parallel_v2
import os
from .Utilities.Logging.log_messages import log_print_email_message as logm
import matplotlib.pyplot as plt

# TODO: add suport for multiclass prediction vs exisiting binary
# TODO: add ability to export confusion matrix


def youden_threshold(fpr, tpr, thresholds):
    """
    Calculate Youdon Index.

    Parameters
    ----------
    fpr : TYPE
        DESCRIPTION.
    tpr : TYPE
        DESCRIPTION.
    thresholds : TYPE
        DESCRIPTION.

    Returns
    -------
    optimal_threshold : TYPE
        DESCRIPTION.

    """
    optimal_idx = np.argmax(tpr - fpr)
    optimal_threshold = thresholds[optimal_idx]
    return optimal_threshold


def bootstrap(y_true, y_pred, n, fn,
              low_percentile: float = 2.5,
              high_percentile: float = 97.5,
              debug: bool = False,
              fn_kwargs: dict = {}):
    """
    Bootstrap Metrics.

    Parameters
    ----------
    y_true : TYPE
        DESCRIPTION.
    y_pred : TYPE
        DESCRIPTION.
    n : TYPE
        DESCRIPTION.
    fn : TYPE
        DESCRIPTION.
    fn_kwargs : dict, optional
        DESCRIPTION. The default is {}.
    low_percentile : TYPE, optional
        DESCRIPTION. The default is 2.5.
    high_percentile : TYPE, optional
        DESCRIPTION. The default is 97.5.

    Returns
    -------
    val : TYPE
        DESCRIPTION.
    low : TYPE
        DESCRIPTION.
    high : TYPE
        DESCRIPTION.

    """
    val, pred_classes = fn(y_true, y_pred, return_pred_class=True, **fn_kwargs)

    if debug:
        bootstraps = []
        for _ in tqdm(range(n)):
            idx = np.random.randint(0, len(y_true), len(y_true))

            y_true_sample = y_true[idx]
            y_pred_sample = y_pred[idx]

            if len(np.unique(y_true_sample)) > 1:
                bootstraps.append(fn(y_true_sample, y_pred_sample, **fn_kwargs))

    kwargs_list: list = [{'y_true': y_true,
                          'y_pred': y_pred,
                          'fn': fn,
                          'fn_kwargs': fn_kwargs} for _ in range(n)]

    bootstraps = [x['future_result'] for x in run_function_in_parallel_v2(function=_run_bootstrap,
                                                                          kwargs_list=kwargs_list,
                                                                          max_workers=int(os.cpu_count() * 0.75),
                                                                          log_name='Bootstrap Metrics',
                                                                          executor_type='ProcessPool',
                                                                          return_results=True,
                                                                          list_running_futures=False,
                                                                          debug=False)]

    bootstraps = np.array([x for x in bootstraps])

    low = np.percentile(bootstraps, low_percentile, axis=0)
    high = np.percentile(bootstraps, high_percentile, axis=0)

    return val, low, high, pred_classes


def _run_bootstrap(y_true, y_pred, fn, fn_kwargs):
    idx = np.random.randint(0, len(y_true), len(y_true))

    y_true_sample = y_true[idx]
    y_pred_sample = y_pred[idx]

    try:
        return fn(y_true_sample, y_pred_sample, **fn_kwargs)
    except:
        return np.array([np.nan] * 9)


def get_metrics(y_true: Union[pd.Series, np.ndarray, list],
                y_pred_score: Union[pd.Series, np.ndarray, list],
                J: float = None,
                return_pred_class: bool = False):
    """
    Get Performance metrics.

    Parameters
    ----------
    y_true : Union[pd.Series, np.ndarray, list]
        True answer.
    y_pred_score : Union[pd.Series, np.ndarray, list]
        predicted answer.

    Returns
    -------
    np.ndarray
        np array including [auroc, auprc, sens, spec, ppv, npv, acc, f1, Youden-Index]

    """
    if isinstance(J, (float, int)):
        pass
    else:
        fpr, tpr, thresholds = roc_curve(y_true, y_pred_score)
        J = youden_threshold(fpr, tpr, thresholds)
    y_pred_class = (y_pred_score >= J).astype('int')

    # Score-based metrics
    try:
        auroc = roc_auc_score(y_true, y_pred_score)
    except:
        auroc = np.nan

    pr, re, thresh = precision_recall_curve(y_true, y_pred_score)
    auprc = auc(re, pr)

    # Class-based metrics
    cm = confusion_matrix(y_true, y_pred_class)

    try:
        tn = cm[0, 0]
        fn = cm[1, 0]
        tp = cm[1, 1]
        fp = cm[0, 1]

        sens = tp / (tp + fn)
        spec = tn / (tn + fp)
        ppv = tp / (tp + fp)
        npv = tn / (tn + fn)
        acc = (tp + tn) / (tp + fp + fn + tn)
        f1 = (tp) / (tp + 0.5 * (fp + fn))
    except IndexError:
        sens = np.nan
        spec = np.nan
        ppv = np.nan
        npv = np.nan
        acc = np.nan
        f1 = np.nan

    out_arr: np.ndarray = np.array([auroc, auprc, sens, spec, ppv, npv, acc, f1, J])

    if return_pred_class:
        return out_arr, y_pred_class

    return out_arr


def compute(y_true, y_pred_score, outcome, model_string, n_bootstraps: int = 1000, low_percentile: float = 2.5, high_percentile: float = 97.5,
            return_pred_classes: bool = False, J: float = None, single_thread: bool = False):

    try:
        val, low, high, pred_classes = bootstrap(y_true=y_true, y_pred=y_pred_score, n=n_bootstraps, fn=get_metrics, debug=single_thread,
                                                 low_percentile=low_percentile, high_percentile=high_percentile, fn_kwargs={'J': J})
    except ValueError:
        val, low, high, pred_classes = np.array(['Not Calculable'] * 9), np.array(['Not Calculable'] * 9), np.array(['Not Calculable'] * 9), None

    out = {}

    for i, metric in enumerate(['AUROC', 'AUPRC', 'Sensitivity', 'Specificity', 'PPV', 'NPV', 'Accuracy', 'F1', 'Youdon-Index']):
        out[metric] = val[i]
        out[f'{metric}_low_{low_percentile}'] = low[i]
        out[f'{metric}_high_{high_percentile}'] = high[i]

    out['outcome'] = outcome
    out['model'] = model_string
    out['outcome_prevalence'] = f'{(y_true == 1).sum() / len(y_true):.2%}'
    out['n'] = len(y_true)

    if return_pred_classes:
        return out, pred_classes

    return out


def plot_AUC(y_true: Union[pd.Series, np.ndarray],
             y_pred: Union[pd.Series, np.ndarray],
             outcome_name: str = '',
             out_path: str = None):
    '''Plot ROC.'''

    # RF - draw ROC and compute the AUC
    fpr, tpr, thresholds = roc_curve(y_true, y_pred)
    roc_auc = auc(fpr, tpr)
    fig = plt.figure(figsize=(6, 4))
    plt.plot(fpr, tpr, lw=1, color='red', label='AUROC (%0.2f)' % roc_auc)
    # plotting ROCs

    plt.plot([0, 1], [0, 1], color='gray', lw=1, linestyle='--')
    plt.ylabel('True positive rate')
    plt.xlabel('False positive rate')
    plt.title('ROC Curve: {outcome_name}')
    plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    plt.show()

    fig.savefig(out_path, format='svg', bbox_inches='tight')


def plot_AUPRC(y_true: Union[pd.Series, np.ndarray],
               y_pred: Union[pd.Series, np.ndarray],
               outcome_name: str = '',
               out_path: str = None):
    '''Plot PRC '''
    # RF - draw PR-AUC
    precision, recall, threshold_pr = precision_recall_curve(y_true, y_pred)
    pr_auc = auc(recall, precision)

    fig = plt.figure(figsize=(6, 4))
    plt.plot(recall, precision, lw=1, color='red', label='AUPRC (%0.2f)' % pr_auc)
    # plotting PR-AUCs

    plt.ylabel('Precision')
    plt.xlabel('Recall')
    plt.ylim([0, 1])
    plt.xlim([0, 1])
    plt.title('Precision Recall curve for: {outcome_name}')
    plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    plt.show()

    fig.savefig(out_path, format='svg', bbox_inches='tight')


# def plot_CM():
#     # TODO: Finish
#     '''Plot Confusion Matrix'''
#     thresh = thresholds[np.argmax(tpr - fpr)]
#     pred_probs_temp = pred_probs.reshape(-1, 1)
#     y_binarized = preprocessing.binarize(pred_probs_temp, threshold=thresh)
#     cm = metrics.confusion_matrix(y_test, y_binarized)
#     show_confusion_matrix(cm, risk, project=project, directory=directory, class_labels=['No Event', 'Event'])


def compute_model_performance(df_dict: dict, model_name: str, n_bootstraps: int = 1000, low_percentile: float = 2.5, high_percentile: float = 97.5,
                              return_pred_classes: bool = False) -> pd.DataFrame:
    """
    Compute Performance Metrics on df_dict.

    Parameters
    ----------
    df_dict : dict
        dictionary of dataframes and outcome/prediction information.
        {outcome_name1: {df: pd.DataFrame, true_label_col: str, prediction_col: str, model_name: str (optional)},
         outcome_name2: {df: pd.DataFrame, true_label_col: str, prediction_col: str, model_name: str (optional)}}
    model_name : str
        name to use for the model. Note: This can be overwritten by the df_dict.
    n_bootstraps : int, optional
        Number of bootstraps to use to generate the confidence intervals. The default is 1000.
    low_percentile : float, optional
        The bottom percentile to use to generate the confidence interval. The default is 2.5.
    high_percentile : float, optional
        the top percentile to use to generate the confidence interval. The default is 97.5.

    Returns
    -------
    pd.DataFrame
        Model performance metric with outcome names and columns and metric as index.

    """
    out: list = []

    class_dict: dict = {}

    for outcome_name, outcome_dict in df_dict.items():
        logm(f'Computing performance for {outcome_name}', display=True)
        computation_result: Union[tuple, np.ndarray] = compute(y_true=outcome_dict.get('df')[outcome_dict.get('true_label_col')],
                                                               y_pred_score=outcome_dict.get('df')[outcome_dict.get('prediction_col')],
                                                               outcome=outcome_name,
                                                               J=outcome_dict.get('J', None),
                                                               model_string=outcome_dict.get('model_name', model_name),
                                                               n_bootstraps=n_bootstraps,
                                                               low_percentile=low_percentile,
                                                               high_percentile=high_percentile,
                                                               return_pred_classes=return_pred_classes)
        if isinstance(computation_result, tuple):
            stats = computation_result[0]
            classes = computation_result[1]
        else:
            stats = computation_result
        out.append(pd.Series(stats,
                             name=outcome_name))

        if return_pred_classes:
            t_df: pd.DataFrame = outcome_dict.get('df')
            t_df[f'{outcome_name}_pred_class'] = classes
            class_dict[outcome_name] = t_df

    if return_pred_classes:
        return pd.concat(out, axis=1), class_dict

    return pd.concat(out, axis=1)


if __name__ == '__main__':
    # from Utils.io import check_load_df
    predictions: pd.DataFrame = None
    outcomes: pd.DataFrame = None
    outcome_map_dict: dict = {7: '24hr_location_icu',
                              8: '48hr_location_icu',
                              9: '30d_dead',
                              10: '30d_hosp_readmit'}
    overall = compute_model_performance(df_dict={outcome_map_dict.get(n): {'df': predictions,
                                                                           'true_label_col': f'y_true_{n}',
                                                                           'prediction_col': f'y_pred_{n}'} for n in [7, 8, 9, 10]},
                                        model_name='Ruppert_Thesis_overall')
