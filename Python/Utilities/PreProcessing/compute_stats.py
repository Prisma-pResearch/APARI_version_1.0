# -*- coding: utf-8 -*-
"""
Module for computing statistics on dataframes and series.

Created on Wed Dec  8 11:27:07 2021.

@author: ruppert20
"""
import pandas as pd
import numpy as np


def median_deviation(vect: pd.Series) -> float:
    """
    Calculate median deviation of an array.

    Parameters
    ----------
    vect : pd.Series or np.ndarray
        Array of input feature.

    Returns
    -------
    float
        median deviation of input array.

    """
    med = np.nanmedian(vect)
    return np.nanmedian(np.abs(vect - med))


def outlier_detection_and_imputation(vect: pd.Series, imputation_series: pd.Series = None,
                                     rounded_vars: list = ["age", "cci", "nephrotoxic", "num_meds"]) -> pd.Series:
    """
    Compute mean, median, percentiles and median deviation for input feature with feature type int and num in feature list. And correct numerical values if there are any outliers. Calls median_deviation function.

    1. Coerce Series to Numeric.
    2. Calculate summary statistics if no imputation_series are provided else fill in values using pre_existing statistics
    3. return result

    Parameters
    ----------
    vect : pd.Series
        Contains numerical values of input feature.
    imputation_series: pd.Series, default: None
        A pre-computed statistic series used to impute out of range of mising values.

    Returns
    -------
    df : pd.DataFrame()
        Dataframe used for numerical imputation, contains statistical features of input feature. Contains the following columns:
            * Feature
            * mean
            * median_dev
            * 0.99
            * 0.01
            * 0.005
            * 0.05
            * 0.95
            * 0.995
            * median

    """
    assert isinstance(vect, pd.Series), f'the input to the outlier_dection method must be a pandas series; however, a {type(vect)} was provided'
    # make deep copy and coerce to numeric
    vect = pd.to_numeric(vect.copy().reset_index(drop=True), errors='coerce')

    if pd.isnull(imputation_series):
        perc_missing: float = vect.isnull().astype(int).mean() * 100
        vect.dropna(inplace=True)
        imputation_series: pd.Series = pd.Series({'mean': np.nanmean(vect),
                                                  'median_dev': median_deviation(vect),
                                                  'mean_dev': vect.mad(),
                                                  'median': vect.quantile(q=0.5),
                                                  '0.005': vect.quantile(q=0.005),
                                                  '0.05': vect.quantile(q=0.05),
                                                  '0.95': vect.quantile(q=0.95),
                                                  '0.995': vect.quantile(q=0.995),
                                                  '0.01': vect.quantile(q=0.01),
                                                  '0.99': vect.quantile(q=0.99),
                                                  'percent_missingness': perc_missing},
                                                 name=vect.name)
        return imputation_series
    elif isinstance(imputation_series, pd.Series):
        # fill missing values with median
        vect.fillna(imputation_series['median'], inplace=True)

        # clip age at 103, impute all others from distribution
        if vect.name == 'age':
            vect.clip(upper=103, inplace=True)
        else:
            # compute modified z-score
            z = vect.apply(lambda x: abs(0.6745 * (x - imputation_series['median'])) / imputation_series['median_dev'] if imputation_series['median_dev'] != 0 else
                           (abs((x - imputation_series['median'])) / (imputation_series['mean_dev'] * 1.253314) if imputation_series['mean_dev'] != 0 else 0))

            # flag upper outliers and replace with value between 95% and 99.5%
            con_over99 = (z > 3.5) & (vect > imputation_series['0.99'])
            if con_over99.any():
                vect[con_over99] = np.random.uniform(imputation_series['0.95'], imputation_series['0.995'], vect[con_over99].shape[0])

            # flag under outliers and replace with value between 0.5% and 5%
            con_under1 = (z > 3.5) & (vect < imputation_series['0.01'])
            if con_under1.any():
                vect[con_under1] = np.random.uniform(imputation_series['0.005'], imputation_series['0.05'], vect[con_under1].shape[0])

            # check if imputation should be rounded
            if vect.name.lower() in rounded_vars:
                vect = vect.round(0).astype(int)

        return vect


def compute_lab_ratio(baseline_df: pd.DataFrame,
                      visit_df: pd.DataFrame,
                      merging_key: list,
                      lab_type: str,
                      min_thresholds_for_score: int,
                      minimum_absolute_thresh: float = None,
                      minimum_ratio_threshold: float = None,
                      maximum_absolute_thresh: float = None,
                      maximum_ratio_threshold: float = None,
                      default_baseline_value: float = None,
                      minimum_baseline_value: float = None,
                      score_label: str = None) -> pd.DataFrame:
    """
    Compute Ratio between baseline and given values to determine if "scoring event(s)" have occurred based on specified criteria.

    Parameters
    ----------
    baseline_df: pd.DataFrame
        DataFrame containing the baseline values for the specified lab.
        It must contain the values specified in the merging key.
    visit_df: pd.DataFrame,
    merging_key: list,
    lab_type: str,
    min_thresholds_for_score: int
        Minimum number of thresholds that need to be satisfied to score a point.
    minimum_absolute_thresh: float, optional
        Minimum absolute threshold number to get a score. Default is None
    minimum_ratio_threshold: float, optional
        Minimum ratio threshold number to get a score. Default is None.
    maximum_absolute_thresh: float, optional
        Maximum absolute threshold number to get a score. Default is None.
    maximum_ratio_threshold: float, optional
        Maximum ratio threshold number to get a score.. Default is None
    default_baseline_value: float, optional
        Default baseline value is none is provided.
    minimum_baseline_value: float, optional
        Minimum baseline value to be eligible to score.
    score_label: str,
        The name of the score column.

    Returns
    -------
    pd.DataFrame
        Scored Dataframe

    """
    # label the baseline
    out = visit_df\
        .merge(baseline_df,
               on=merging_key,
               how='left')

    # fill default value (if present)
    if isinstance(default_baseline_value, (float, int)):
        out[f'baseline_{lab_type.lower()}'].fillna(default_baseline_value, inplace=True)

    # calculate lab ratio
    out['lab_ratio'] = (out[f'{lab_type.lower()}'] / out[f'baseline_{lab_type.lower()}']).values

    stat_dict: dict = {'minimum_ratio_threshold': minimum_ratio_threshold,
                       'minimum_absolute_thresh': minimum_absolute_thresh,
                       'maximum_ratio_threshold': maximum_ratio_threshold,
                       'maximum_absolute_thresh': maximum_absolute_thresh,
                       'minimum_baseline_value': minimum_baseline_value}

    for nm, thresh in stat_dict.items():

        if isinstance(thresh, (float, int)):
            if 'minimum' in nm:
                score_idx: pd.Series = out['lab_ratio' if 'ratio' in nm else
                                           f'baseline_{lab_type.lower()}' if 'baseline' in nm else
                                           f'{lab_type.lower()}'] >= thresh
            else:
                score_idx: pd.Series = out['lab_ratio' if 'ratio' in nm else
                                           f'baseline_{lab_type.lower()}' if 'baseline' in nm else
                                           f'{lab_type.lower()}'] <= thresh

            if score_idx.any():  # account for possiblity of None True
                out.loc[score_idx, nm] = 1

            if not score_idx.all():  # account for possiblity of all True
                out.loc[~score_idx, nm] = 0

    out[score_label] = out[out.columns.intersection(list(stat_dict.keys()))].apply(lambda row: 1 if row.sum() >= min_thresholds_for_score else 0, axis=1)

    return out.drop(columns=list(stat_dict.keys()), errors='ignore')
