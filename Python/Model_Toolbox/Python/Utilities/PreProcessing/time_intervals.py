# -*- coding: utf-8 -*-
"""
Module for processing Time Interval Data.

Created on Thu Jul 23 11:47:20 2020

@author: ruppert20
"""
import pandas as pd
# import os
import numpy as np
import psutil
from scipy.sparse.csgraph import connected_components
from typing import Union, List
from copy import deepcopy
from math import ceil
from ..Logging.log_messages import log_print_email_message
from .data_format_and_manipulation import create_dict
from ..ResourceManagement.parallelization_helper import run_function_in_parallel_v2
# from ..General.func_utils import debug_inputs


def condense_in_parallel(df: pd.DataFrame,
                         grouping_columns: list,
                         start_col: str,
                         end_col: str,
                         gap_tolerance_hours: int = 0,
                         col_action_dict: dict = None,
                         custom_overlap_function: callable = None,
                         chunk_size: int = 50,
                         display: bool = False,
                         log_name: str = 'Condense_in_Parallel',
                         serial: bool = False,
                         max_workers: int = 4,
                         **kwargs) -> pd.DataFrame:

    df.dropna(subset=[start_col, end_col], inplace=True)

    if df.shape[0] in [0, 1]:
        return df

    # ensure all groupby columns have values to prevent unexpected behavior
    if df[grouping_columns].isnull().any().any():
        replace_999: bool = True
        df[grouping_columns] = df[grouping_columns].fillna(-999)
    else:
        replace_999: bool = False

    # establish number of unique groups
    groups: pd.DataFrame = df[grouping_columns].drop_duplicates()

    kwargs_list: list = []

    base_kwargs: dict = deepcopy(kwargs)
    for k, v in {'custom_overlap_function': custom_overlap_function,
                 'start_col': start_col,
                 'end_col': end_col,
                 'grouping_columns': grouping_columns,
                 'gap_tolerance_hours': gap_tolerance_hours,
                 'col_action_dict': col_action_dict}.items():
        base_kwargs[k] = v

    if groups.shape[0] < chunk_size:
        base_kwargs['df'] = df
        kwargs_list.append(base_kwargs)
    else:
        num_batches: int = ceil(groups.shape[0] / chunk_size)
        log_print_email_message(message=f'Splitting resampling job into {num_batches} to expedite resampling', display=display, log_name=log_name)
        for i, chunk in enumerate(np.array_split(groups, num_batches)):
            t = deepcopy(base_kwargs)
            t['df'] = df.merge(chunk, on=grouping_columns, how='inner').copy(deep=True)
            kwargs_list.append(t)

    out: pd.DataFrame = pd.concat([x['future_result'] for x in run_function_in_parallel_v2(function=condense_overlapping_segments,
                                                                                           kwargs_list=kwargs_list,
                                                                                           max_workers=min(max_workers, psutil.cpu_count(logical=False)),
                                                                                           update_interval=10,
                                                                                           disp_updates=display,
                                                                                           list_running_futures=display,
                                                                                           return_results=True,
                                                                                           log_name=log_name,
                                                                                           executor_type='ProcessPool',
                                                                                           debug=serial)], axis=0, sort=False, ignore_index=False)

    if replace_999:
        return out.replace({-999: None, '-999': None})

    return out


def condense_overlapping_segments(df: pd.DataFrame, grouping_columns: list, start_col: str, end_col: str,
                                  gap_tolerance_hours: int = 0, col_action_dict: dict = None,
                                  custom_overlap_function: callable = None, **kwargs) -> pd.DataFrame:
    """
    Merge Overlapping time intervals in a pandas dataframe.

    This function does the following actions:
        1. Creates a 2D connectivity graph between start and end columns with a variable hour padding added to the end column date
        2. Overlapping rows are merged into one row with the cell contents specified by the col_action_dict, if no col_action_dict is provided it will be the first row

    Parameters
    ----------
    df:pd.DataFrame
        pandas data frame that is groupedby grouping columns as index with atleast the folling columns
        -start_col
        -end_col
        -atleast one grouping column

    grouping_columns: list
        columns serving as the groupby index

    start_col: str
        datetime which marks the start of the time interval

    end_col: str
        datetime which marks the end of hte time interval

    col_action_dict: dict
        A dictionary which allows control of how overlapping rows are aggregated. The default dictionary created is as follows:
            col_action_dict={'first':list(df.columns), 'grouping':grouping_columns}, where columns 'grouping' key are later removed from the dictionary as they are part of the index

    gap_tolerance_hours:int
        the number of hours allowed between time intervals for them to still be considered one interval

    custom_overlap_function: callable
        Custom function for handling overlapping time intervals within a group. The default behavior is to take the first not null field in each overlapping row group.

    **kwargs
        Keyword arguments for the custom_overlap_function

    Returns
    -------
    pd.DataFrame
        returns a data frame with overlapping rows merged into one row

    Notes
    -----
        Default values are provided for gap tolerance_hours, which is 0 hours

    """
    # get numpy array of admission dates
    start = pd.to_datetime(df[start_col], errors='coerce').to_numpy()

    # get numpy array of discharge dates with a 24 hour padding
    end = (pd.to_datetime(df[end_col], errors='coerce') + np.timedelta64(gap_tolerance_hours, 'h')).to_numpy()

    # make graph
    graph = (start <= end[:, None]) & (end >= start[:, None])

    # find connected components in this graph
    n_components, indices = connected_components(graph)

    # return the df as is if there are no connected pieces
    if n_components == df.shape[0]:
        if custom_overlap_function is None:
            return df.drop(columns=grouping_columns)

        return df.reset_index(drop=True)

    # run custom overlap resolution function
    if custom_overlap_function is not None:
        return df.groupby(indices).apply(lambda df: custom_overlap_function(df=df, start_col=start_col, end_col=end_col, **kwargs)).reset_index(drop=True)

    # get dict of columns (using all columns) that specify how columns should be aggreated
    if isinstance(col_action_dict, dict):
        dic = create_dict(col_action_dict=col_action_dict, start_col=start_col, end_col=end_col)
    else:
        dic = create_dict(col_action_dict={'first': list(df.columns), 'grouping': grouping_columns}, start_col=start_col, end_col=end_col)

    # group the results by these connected components
    try:
        return df.groupby(indices).aggregate(dic).reset_index(drop=True)
    except Exception as excep:
        log_print_email_message(message='check value types for {} and {}'.format(start_col, end_col), display=True,
                                inside_parallel_process=True, error=True)

        log_print_email_message(message_2=f'id {df[grouping_columns[0]].min()} generated an exception during aggregation {excep}', display=True,
                                inside_parallel_process=True, fatal_error=True)


def resolve_overlaps(df: pd.DataFrame,
                     end_col: str,
                     start_col: str,
                     priority_col: str = 'tmp_priority',
                     granularity: str = '1s',
                     max_len: str = None,
                     return_initial_index: bool = False) -> pd.DataFrame:
    """
    Resolve information from rows with overlapping time intervals.

    Parameters
    ----------
    df : pd.DataFrame
        DESCRIPTION.
    end_col : str
        end column for the time interval described in each row.
    start_col : str
        start column for the time interval described in each row.
    priority_col : str, optional
        DESCRIPTION. The default is 'tmp_priority'.
    return_initial_index : bool, optional
        DESCRIPTION. The default is False.
    granularity: str, optional.
        How detailed the resolution of overlaps should be. The Default is 1 second.
    max_len: str, optional
        The maximum length between start and stop events. There default is no limit.

    Returns
    -------
    pd.DataFrame
        DESCRIPTION.

    """
    if df.shape[0] == 1:
        return df.reset_index(drop=True)

    df['xxx_effective_end_xxx'] = df[[start_col, end_col]].apply(lambda row: min(row[end_col], row[start_col] + pd.to_timedelta(max_len)) if isinstance(max_len, str) else row[end_col], axis=1)

    # make a template to insert the relevant information into
#    stime = dt.now()
    clip_date = df['xxx_effective_end_xxx'].max()
    temp: pd.DataFrame = pd.DataFrame(pd.Series({df[start_col].min(): 999, clip_date: 999}).resample(granularity).ffill()).drop(columns=[0])
#    print('index creation: {}'.format(dt.now()-stime))

    # make a deep copy of the df with the index reset to ensure it is unique
#    stime = dt.now()
    df = df.copy().sort_values(start_col).reset_index(drop=True)
#    print('deep copy sort: {}'.format(dt.now()-stime))

    # check if the priority_col exists, if not add one
    if priority_col not in df.columns:
        df[priority_col] = 1

    # get the relevant information from each row
#    stime = dt.now()
    for index, row in df.iterrows():
        if row[start_col] <= clip_date:
            temp[index] = pd.Series({row[start_col]: row[priority_col],
                                     row['xxx_effective_end_xxx']: row[priority_col]}).resample('1s').ffill()
        else:
            print(row)
#    print('itterate through rows: {}'.format(dt.now()-stime))

#    stime = dt.now()
    temp = temp.idxmin(axis=1, skipna=True).reset_index().rename(columns={'index': 'time_series', 0: 'df_index'}).dropna(subset=['df_index'])
#    print('get indicies and reset index: {}'.format(dt.now()-stime))

    temp['grouping_col'] = 'temp'

#    stime = dt.now()
    temp['time_group'] = temp.groupby('grouping_col', group_keys=False)['df_index'].apply(lambda x: (x != x.shift()).astype(int).cumsum())
#    print('label groups: {}'.format(dt.now()-stime))

    temp.loc[:, end_col] = temp.loc[:, 'time_series']

    temp.rename(columns={'time_series': start_col}, inplace=True)

#    stime = dt.now()
    temp_out = temp.groupby('time_group').agg({start_col: 'min', end_col: 'max', 'df_index': 'first'}).reset_index(drop=True)
#    print('aggregation: {}'.format(dt.now()-stime))

#    stime = dt.now()
    df.drop(columns=['xxx_effective_end_xxx'], inplace=True)
    out = temp_out.merge(df.reset_index().drop(columns=[start_col, end_col]), left_on='df_index', right_on='index', how='inner')[df.columns.tolist() + ['df_index'] if return_initial_index else df.columns.tolist()]
#    print('merge via index: {}'.format(dt.now()-stime))

    return out


def resample_and_condense(df: pd.DataFrame,
                          start_date_col: str,
                          grouping_col: Union[str, int, List[int], List[str]],
                          end_date_col: Union[str, None] = None,
                          gap_tolerance_hours: int = 0) -> pd.DataFrame:
    """
    Resample Series down to the hour and consense overlapping segments.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to condense.
    start_date_col : str
        the start date column.
    grouping_col : Union[str, int, List[int, str]]
        The column or list of columns used to group the dataframe.
    end_date_col : Union[str, None], optional
        end date column. The default is None.

    Returns
    -------
    df : pd.DataFrame
        Resampled and Condensed DataFrame.

    """
    if isinstance(start_date_col, str):
        assert start_date_col in df.columns, f'The value for the parameter start_date_col: {start_date_col} was not found in the dataframe'
        df.rename(columns={start_date_col: 'period_start'}, inplace=True)
        df.loc[:, 'period_start'] = df.loc[:, 'period_start'].dt.floor('H').values

    if isinstance(end_date_col, str):
        assert end_date_col in df.columns, f'The value for the parameter end_date_col: {end_date_col} was not found in the dataframe'
        df.rename(columns={end_date_col: 'period_end'}, inplace=True)
        df.loc[:, 'period_end'] = df.loc[:, 'period_end'].dt.ceil('H').values

        df = df.groupby(grouping_col)

        df = df\
            .apply(condense_overlapping_segments,
                   start_col='period_start',
                   end_col='period_end',
                   gap_tolerance_hours=gap_tolerance_hours,
                   grouping_columns=[grouping_col] if isinstance(grouping_col, (str, int)) else grouping_col,
                   single_group=False if df.ngroups > 1 else True)\
            .reset_index(drop=False).drop(columns=['index', 'level_1'], errors='ignore')

    return df
