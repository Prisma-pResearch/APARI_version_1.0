# -*- coding: utf-8 -*-
"""
Module to get the most recent or oldest value per group in a pandas dataframe.

Created on Wed Oct  5 07:40:20 2022

@author: ruppert20
"""
import pandas as pd


def get_most_recent_or_earliest_values(df: pd.DataFrame,
                                       time_index: str,
                                       level_col: str,
                                       value_col: str,
                                       most_recent: bool = True,
                                       index_cols: list = None,
                                       levels_to_use: list = None):
    """
    Get the most recent or earliest observation of each specified level.

    Parameters
    ----------
    df : pd.DataFrame
        Pandas DataFrame to be filtered.
    time_index : str
        Column with datetime/date values to use for sorting.
    level_col : str
        Column that has the identifer for measurements/observations.
    value_col : str
        Column that has the value of the measurement/observation.
    most_recent: bool, optional
        Whether the most recent or earliest value should be returned for each level. The default is True, which will return the most recent.
    index_cols : list, optional
        Column that has other identifying information such as patient id, encounter_id, etc. that should be preserved in the output,
        but would not interfere with the aggregation. The default is None, where all of the columns not otherwise specified (by the other parameters) are assumed to be index columns.
    levels_to_use : list, optional
        Allows for subsetting a list of labels instead of using all labels when aggregating. The default is None, where the most recent observation of each level will be used.

    Returns
    -------
    pd.DataFrame
        A pandas dataframe with the most recent observation of each level.

    """
    assert isinstance(df, pd.DataFrame), f'The input for the paramter df, must be a pandas dataframe, however, it was found to be of type: {type(df)}'
    for k, v in {'time_index': time_index, 'level_col': level_col, 'value_col': value_col}.items():
        assert v in df.columns, f'The input dataframe must contain the value for the parameter {k}: {v}, but only the following columns were found: {df.columns.tolist()}'
    if isinstance(index_cols, list):
        assert len(df.columns.intersection(index_cols).tolist()) == len(index_cols), f'The following index_cols were missing from the dataframe: {pd.Series(index=index_cols).index.difference(df.columns)}'
    else:
        index_cols: list = df.columns.difference([time_index, level_col, value_col]).tolist()
    if not isinstance(levels_to_use, list):
        levels_to_use: list = df[level_col].dropna().unique().tolist()

    # Filter dataframe
    df = df.loc[df[level_col].isin(levels_to_use),
                index_cols + [time_index, value_col, level_col]].copy()

    # ensure consistent use of nans
    df = df.where(pd.notnull(df), None)

    # ensure time_col is formatted appropriately
    if str(df[time_index].dtype) != 'datetime64[ns]':
        df.loc[:, time_index] = pd.to_datetime(df.loc[:, time_index].values, errors='coerce')

    # drop rows with any na values. There is odd grouping behavior if any of the index cols are missing, the time, value, and level columns are required to get a good result, so dropping any na value is approperiate.
    df.dropna(how='any', inplace=True)

    # get the most recent observation of each group
    return df.sort_values(time_index, ascending=most_recent).groupby([level_col] + index_cols, group_keys=False).last().reset_index(drop=False)
