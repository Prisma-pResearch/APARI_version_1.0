# -*- coding: utf-8 -*-
"""
Split a cohort into train, test, validation through time or randomly.

Created on Fri Apr 23 07:36:31 2021.

@author: ruppert20
"""
import pandas as pd
from sklearn.model_selection import StratifiedShuffleSplit
from sklearn.model_selection import train_test_split
from math import floor


def split_development_validation(df: pd.DataFrame,
                                 project_name: str,
                                 dev_percent: float = 0.7,
                                 val_percent: float = 0.1,
                                 test_percent: float = 0.2,
                                 split_type: str = 'longitudinal',
                                 stratification_columns: list = None,
                                 time_index_col: str = None,
                                 unique_index_col: str = 'row_id',
                                 random_state: int = 20) -> pd.DataFrame:
    """
    Split a dataframe into development, test, and validation cohorts.

    Parameters
    ----------
    df : pd.DataFrame
        Data Frame to be split.
    project_name : str
        Project name.
    dev_percent : float, optional
        Percentage for development. The default is 0.7.
    split_type : str, optional
        type of split ['longitudinal' or 'random']. The default is 'longitudinal'.
    stratification_columns : list, optional
        columns to stratify the dataframe by first before splitting. The default is None.
    unique_index_col : str, optional
        Column which is unique to each line, if one is not provided one will provided for you. The default is 'row_id'.
    random_state : int, optional
        random sate used to split the data. The default is 20.

    Raises
    ------
    Exception
        DESCRIPTION.

    Returns
    -------
    pd.DataFrame
        pandas dataframe with the cohorts labeled in the cohort column.

    """
    assert dev_percent + val_percent + test_percent == 1, 'The development, validation, and test cohorts must add up to 100%%'
    # create a deep copy with a fresh index
    df = df.copy().reset_index(drop=True)

    # create a unique index if one is not specified
    if unique_index_col not in df.columns:
        cols_to_drop: list = [unique_index_col]
        df = df.reset_index(drop=False).rename(columns={'index': unique_index_col})
    else:
        cols_to_drop: list = []

    # create cohort column
    df['cohort'] = None

    # if it is a time split
    if split_type == 'longitudinal':

        assert time_index_col in df.columns, f'The column: {time_index_col}, was not found in the dataframe: {df.columns.tolist()}'

        # sort by time_index
        df.sort_values(time_index_col, ascending=True, inplace=True)

        # stratify split by columns if specified
        if isinstance(stratification_columns, list):
            valuesets = df[stratification_columns].copy().drop_duplicates()

            for _, valueset in valuesets.iterrows():
                print(valueset)
                vs_label = '_'.join(valueset.astype(str))

                rows = df.loc[(df[stratification_columns] == valueset).apply(all, axis=1),
                              [time_index_col, unique_index_col]]\
                    .set_index(unique_index_col)[time_index_col]\
                    .sort_values(ascending=True)

                max_dev_date = pd.to_datetime('1970-01-01').date() if dev_percent == 0 else rows.iloc[floor(rows.shape[0] * dev_percent)].date()

                if dev_percent > 0:
                    df.loc[df[unique_index_col].isin(rows[rows.dt.date <= max_dev_date].index),
                           'cohort'] = f'{project_name}_Development_{vs_label}_{rows.min().date()}_{max_dev_date}'

                max_test_date = pd.to_datetime('1970-01-02').date() if test_percent == 0 else rows.iloc[floor(rows.shape[0] * (dev_percent + test_percent))].date()

                if test_percent > 0:
                    df.loc[df[unique_index_col].isin(rows[(rows.dt.date > max_dev_date) & (rows.dt.date <= max_test_date)].index),
                           'cohort'] = f'{project_name}_Test_{vs_label}_{rows[rows.dt.date > max_dev_date].min().date()}_{max_test_date}'

                if val_percent > 0:
                    df.loc[df[unique_index_col].isin(rows[rows.dt.date > max_test_date].index),
                           'cohort'] = f'{project_name}_Validation_{vs_label}_{rows[rows.dt.date > max_test_date].min().date()}_{rows.dt.date.max()}'

        # split without stratification
        else:

            max_dev_date = pd.to_datetime('1970-01-01').date() if dev_percent == 0 else df.iloc[floor(df.shape[0] * dev_percent), df.columns.get_loc(time_index_col)].date()

            if dev_percent > 0:
                df.loc[df[time_index_col].dt.date <= max_dev_date, 'cohort'] = f'{project_name}_Development_{df[time_index_col].min().date()}_{max_dev_date}'

            max_test_date = pd.to_datetime('1970-01-02').date() if test_percent == 0 else df.iloc[floor(df.shape[0] * (dev_percent + test_percent)), df.columns.get_loc(time_index_col)].date()

            if test_percent > 0:
                df.loc[((df[time_index_col].dt.date > max_dev_date) & (df[time_index_col].dt.date <= max_test_date)),
                       'cohort'] = f'{project_name}_Test_{df.loc[df[time_index_col].dt.date > max_dev_date, time_index_col].min().date()}_{max_test_date}'

            if val_percent > 0:
                df.loc[df[time_index_col].dt.date > max_test_date,
                       'cohort'] = f'{project_name}_Validation_{df.loc[df[time_index_col].dt.date > max_test_date, time_index_col].min().date()}_{df[time_index_col].max().date()}'

    elif split_type == 'random':

        if isinstance(stratification_columns, list):
            sss = StratifiedShuffleSplit(n_splits=1, train_size=dev_percent, random_state=random_state)
            X = df.drop(columns=stratification_columns)
            y = df[stratification_columns]
            for dev_index, val_index in sss.split(X, y):
                df.loc[dev_index, 'cohort'] = f'{project_name}_Development'
                df.loc[val_index, 'cohort'] = f'{project_name}_Validation'

            raise NotImplementedError('Need to check the below implmentation to divide the validation further into test/validation')
            sss = StratifiedShuffleSplit(n_splits=1, train_size=(test_percent / (test_percent + val_percent)), random_state=random_state)
            X = df[df.cohort == f'{project_name}_Validation'].drop(columns=stratification_columns)
            y = df.loc[df.cohort == f'{project_name}_Validation', stratification_columns]
            for test_index, val_index in sss.split(X, y):
                df.loc[test_index, 'cohort'] = f'{project_name}_Test'
                df.loc[val_index, 'cohort'] = f'{project_name}_Validation'

        else:
            development, temp = train_test_split(df,
                                                 train_size=dev_percent,
                                                 random_state=random_state)

            development.loc[:, 'cohort'] = f'{project_name}_Development'

            test, validation = train_test_split(temp,
                                                train_size=(test_percent / (test_percent + val_percent)),
                                                random_state=random_state)

            test.loc[:, 'cohort'] = f'{project_name}_Test'
            validation.loc[:, 'cohort'] = f'{project_name}_Validation'

            df = pd.concat([development, validation, test], axis=0, sort=False)
    else:
        raise Exception('Unsupported slit_type')

    return df.drop(columns=cols_to_drop)
