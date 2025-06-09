# -*- coding: utf-8 -*-
"""
Calculate AUROC/AUPRC confidence intervals for binary outcomes.

Created on Thu Apr 15 18:11:07 2021

@author: ruppert20
"""
import pandas as pd
import numpy as np
from sklearn.metrics import roc_auc_score, auc, precision_recall_curve


def calculate_AUROC_confidence_intervals(df: pd.DataFrame, outcomes: dict, rng_seed: int = 42, n_bootstraps: int = 1000, mode: str = 'AUC') -> pd.DataFrame:
    """
    Calculate AUROC confidence interval from dataframe that has the actual outcome and the prediction.

    Parameters
    ----------
    df : pd.DataFrame
        *outcome column names
        *outcome prediction names
    outcomes : dict
        {"Outcome column name": {"prediction column name": "prediction label name"},
         "Outcome column name 2": {"prediction column name2": "prediction label name2"}}
    rng_seed : int, optional
        random seed to use. The default is 42.
    n_bootstraps : int, optional
        Number of bootstraps to use. The default is 1000.
    mode: str, optional
        Whether AUC, PR-AUC, or Both should be calculated.

    Returns
    -------
    pd.DataFrame:
        DESCRIPTION.

    """
    assert mode in ['Both', 'PR-AUC', 'AUC'], f"The mode must be in ['Both', 'PR-AUC', 'AUC'], however, {mode} was provided."

    out: pd.DataFrame = pd.DataFrame(columns=['count'])

    for outcome, predictions in outcomes.items():

        temp = df[[outcome] + list(predictions.keys())].dropna()

        out.loc[outcome, 'count'] = temp.shape[0]

        for prediction, prediction_label in predictions.items():
            bootstrapped_scores = []
            pr_auc_bootstrapped_scores = []

            # initalize random state from seed
            rng = np.random.RandomState(rng_seed)

            # get n auroc scores via bootstrapping
            for i in range(n_bootstraps):
                # bootstrap by sampling with replacement on the prediction indices
                indices = rng.randint(0, temp.shape[0], temp.shape[0])
                if len(np.unique(temp[outcome].values[indices])) < 2:
                    # We need at least one positive and one negative sample for ROC AUC
                    # to be defined: reject the sample
                    continue
                if mode in ['Both', 'AUC']:
                    bootstrapped_scores.append(roc_auc_score(temp[outcome].values[indices], temp[prediction].values[indices]))

                if mode in ['Both', 'PR-AUC']:
                    precision, recall, threshold_pr = precision_recall_curve(temp[outcome].values[indices], temp[prediction].values[indices])
                    pr_auc_bootstrapped_scores.append(auc(recall, precision))

            if mode in ['Both', 'AUC']:
                # get 95% CI from scores
                sorted_scores = np.array(bootstrapped_scores)
                sorted_scores.sort()

                overall = roc_auc_score(temp[outcome].values, temp[prediction].values)
                confidence_lower = sorted_scores[int(0.05 * len(sorted_scores))]
                confidence_upper = sorted_scores[int(0.95 * len(sorted_scores))]

                out.loc[outcome, f'{prediction_label} AUROC (95% CI)'] = f'{overall:.2f} ({confidence_lower:.2f}-{confidence_upper:.2f})'

            if mode in ['Both', 'AUC']:
                # get 95% CI from scores
                sorted_scores = np.array(pr_auc_bootstrapped_scores)
                sorted_scores.sort()

                precision, recall, threshold_pr = precision_recall_curve(temp[outcome].values, temp[prediction].values)
                overall = auc(recall, precision)
                confidence_lower = sorted_scores[int(0.05 * len(sorted_scores))]
                confidence_upper = sorted_scores[int(0.95 * len(sorted_scores))]

                out.loc[outcome, f'{prediction_label} PR-AUROC (95% CI)'] = f'{overall:.2f} ({confidence_lower:.2f}-{confidence_upper:.2f})'

    return out.reset_index(drop=False).rename(columns={'index': 'outcome'})
