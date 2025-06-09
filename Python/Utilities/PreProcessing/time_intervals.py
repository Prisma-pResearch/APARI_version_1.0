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
from sympy import Line, Point
import time
from datetime import datetime as dt
from tqdm import tqdm
from ..FileHandling.io import check_load_df
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
        return df.groupby(indices, group_keys=False).apply(lambda df: custom_overlap_function(df=df, start_col=start_col, end_col=end_col, **kwargs)).reset_index(drop=True)

    # get dict of columns (using all columns) that specify how columns should be aggreated
    if isinstance(col_action_dict, dict):
        dic = create_dict(col_action_dict=col_action_dict, start_col=start_col, end_col=end_col)
    else:
        dic = create_dict(col_action_dict={'first': list(df.columns), 'grouping': grouping_columns}, start_col=start_col, end_col=end_col)

    # group the results by these connected components
    try:
        return df.groupby(indices, group_keys=False).aggregate(dic).reset_index(drop=True)
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
    temp_out = temp.groupby('time_group', group_keys=False).agg({start_col: 'min', end_col: 'max', 'df_index': 'first'}).reset_index(drop=True)
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

        df = df.groupby(grouping_col, group_keys=False)

        df = df\
            .apply(condense_overlapping_segments,
                   start_col='period_start',
                   end_col='period_end',
                   gap_tolerance_hours=gap_tolerance_hours,
                   grouping_columns=[grouping_col] if isinstance(grouping_col, (str, int)) else grouping_col,
                   single_group=False if df.ngroups > 1 else True)\
            .reset_index(drop=False).drop(columns=['index', 'level_1'], errors='ignore')

    return df


def solve_timeline_or_segments(df: pd.DataFrame,
                              start_col: str,
                              end_col: str,
                              gap_tolerance: Union[str, int, None] = None,
                              compute_intersections: bool = False,
                              data_type: str = 'datetime',
                              resolution: str = 'minute',
                              show_progress: bool = True) -> (pd.DataFrame, Union[None, pd.DataFrame, np.ndarray], Union[None, pd.DataFrame]):
    
    """
    Solve for which 2D line segments interact and rescontruct timeline/numberline from smallest to largest segments of where which segments are involved.
    
    Actions
    -------
        1. Format start and end_cols to be either datetime or integer based on input.
        2. Validate start and stop are both present and that start is before the end, throwing out invalid rows.
        3. Sort based on start date, reseting index to get a neutral representation (bring any existing indexes into the dataframe as long as they do not conflict with any existing columns).
        4. Validate the that gap_tolerance (if provided) if a valid string that can be translated into a pandas timedelta if datetime else is an integer
        5. Create numpy arrays of start/stop series
        6. Created a Boolean matrix of intersecting segments (graph)
        7. Check if there are any intersections in the graph
            A. if None
                return the formated input df, None, and the reconstructed timeline 
            B. If Some and compute intersections selected
                return the formatted input df, The intersections of each segment, and the reconstructed timeline
            C. If some and compute intersections is set to false
                return the formatted input df, a numpy array of which segments interact with each other, and a reconstructed timeline of when each segment group starts and ends
        
    
    Parameters
    ----------
    df: pd.DataFrame
        Input pandas dataframe with at least the following columns:
            * start_col (parameter)
            * end_col (parameter)
    start_col: str
        Name of the column with the start information
    end_col: str,
        Name of the column with the end information
    gap_tolerance: Union[int, str, None], optional
        Either None, the default, where no gap tolerance is allowed for interactions or an integer value (for interger types) or a string that can be translated into a valid pands timeDelta.
    computer_intersections: bool, optional
        Whether precise intersections should be calculated, or just which groups interact (the default)
    data_type: str, optional
        The accepted data_types are 'int' and 'datetime', the source start_col and end_col will be coerced to these types.
    resolution: Union[str, None], optional
        The precision of the start/end times that should be used when solving for reconstructing a timeline of events.
        This will be the unit of time that event boundaries will be offset in order to not have boundaries occuring concamitantly.
        The default is minute, where 1 minute will be subtracted from the end or added to the start.
        The accepted inputs are 'day', 'hour', 'minute', 'second', and None
        
    Returns
    -------
    Tuple
        df: pd.DataFrame,
            The original df, after Actions 1-3
        intersections: Union[None, np.ndarray, pd.DataFrame]
            The intersection of segments from the df. If they exist, None if they do not.
            Whether it is a numpy array of interacting groups or a pandas dataframe of which segments interact which at each time period depends on the compute_intersections parameter.
        timeline: pd.DataFrame
            The timeline of events taking into account the interactions/intersections of events. How interactions/intersections are handled is based on the compute_intersections parameter.
            
    """
    assert data_type in ['datetime', 'int'], f'Only datetime and interger values are currently supported. However a data_type of {data_type} was indicated.'
    assert resolution in ['day', 'hour', 'minute', 'second', None], f'only day, hour, minute, and second resolutions are allowed; however, {resolution} was passed as the resolution'
    
    # format the start/end columns
    df: pd.DataFrame = check_load_df(df.dropna(subset=[start_col, end_col], how='any').copy(deep=True), desired_types={x: data_type for x in [start_col, end_col]})\
        .sort_values(start_col, ascending=True)\
        .query(f'{start_col} < {end_col}')\
        .reset_index(drop=any([str(x) in df.columns for x in df.index.names]) or (df.index.name is None))
    
    # check gap tolerance is in correct format
    if data_type == 'datetime':
        if isinstance(gap_tolerance, str):
            try:
                pd.to_timedelta(gap_tolerance)
            except:
                raise AssertionError(f'The input gap_tolerance: {gap_tolerance} is not a valid timedelta')
        elif gap_tolerance is not None:
            raise AssertionError(f'The gap_tolerance must be either None or a valid timedelta string; however, an input of {type(gap_tolerance)} was found.')


    elif data_type == 'int':
        assert (gap_tolerance is None) | isinstance(gap_tolerance, int)
        
    #define start and end
    start = df[start_col].to_numpy()
    end = (df[end_col] + (pd.to_timedelta(gap_tolerance or '0 seconds') if data_type == 'datetime' else (gap_tolerance or 0))).to_numpy()
    
    # make graph
    graph = (start <= end[:, None]) & (end >= start[:, None])
    
    if graph.sum() == graph.shape[0]:
        out: pd.DataFrame = df[[start_col, end_col]].copy(deep=True)
        out['ids'] = pd.Series(range(0, out.shape[0]), index=out.index).apply(lambda x: set({x}))
        return df, None, out
    
    if compute_intersections:
        intersections: List[pd.DataFrame] = []
        
        # iterate through each row of the graph
        for i in tqdm(range(0, graph.shape[0]), desc='Computing Intersections', disable=not show_progress):
            
            # find where true
            intersecting_lines: np.ndarray = np.where(graph[i, :])[0]
            
            # only proceed if the line overlaps with more than itself
            if intersecting_lines.shape[0] != 1:
                # define the reference line
                base_line: Line = Line(Point(0, int(time.mktime(pd.to_datetime(df.iloc[i, df.columns.get_loc(start_col)]).timetuple())) if data_type == 'datetime' else df.iloc[i, df.columns.get_loc(start_col)]),
                                       Point(0, int(time.mktime(pd.to_datetime(df.iloc[i, df.columns.get_loc(end_col)]).timetuple()) if data_type == 'datetime' else df.iloc[i, df.columns.get_loc(end_col)])))
                # iterate through all the intersections except for itself
                for j in [x for x in intersecting_lines if x != i]:
                    # solve for the intersection
                    intersect: Line = base_line.intersection(Line(Point(0, int(time.mktime(pd.to_datetime(df.iloc[j, df.columns.get_loc(start_col)]).timetuple())) if data_type == 'datetime' else df.iloc[j, df.columns.get_loc(start_col)]),
                                                                  Point(0, int(time.mktime(pd.to_datetime(df.iloc[j, df.columns.get_loc(end_col)]).timetuple()) if data_type == 'datetime' else df.iloc[j, df.columns.get_loc(end_col)]))))[0]
                    # append the result
                    intersections.append(pd.Series({'ref_idx': i, 'src_idx': j, 'start': intersect.bounds[1], 'end': min(intersect.bounds[3], base_line.bounds[3]) , 'line': intersect,
                                                    'start_dt': dt.fromtimestamp(intersect.bounds[1]) if data_type == 'datetime' else intersect.bounds[1],
                                                    'end_dt': dt.fromtimestamp(min(intersect.bounds[3], base_line.bounds[3])) if data_type == 'datetime' else min(intersect.bounds[3], base_line.bounds[3])}).to_frame().T)
        # concatenate the results
        intersections: pd.DataFrame = pd.concat(intersections, axis=0, ignore_index=True)
        
        # remove symetric overlaps
        intersections['dups'] = intersections.apply(lambda row: '|'.join([str(x) for x in sorted([row.ref_idx, row.src_idx])]), axis=1)
        intersections: pd.DataFrame = intersections.drop_duplicates(subset=['dups'], keep='first').drop(columns=['dups'])
        
        # define output
        out: List[pd.DataFrame] = []
        
        # define 1 diff unit
        dif_unit: Union[pd.Timedelta, int] =  (pd.to_timedelta(f'1 {resolution}') if data_type == 'datetime' else 1)
        
        # initalize the cursor one time unit before the start
        cursor_dt: Union[int, dt] = df.iloc[0, df.columns.get_loc(start_col)] - dif_unit
        
        for i in tqdm(range(0, df.shape[0]), desc='harmonizing time intervals', disable=not show_progress):
            ## define components that make of the time series sequence ##
            
            # bounds
            t_st: Union[int, dt] = max(df.iloc[i, df.columns.get_loc(start_col)], cursor_dt + dif_unit)
            t_stp: Union[int, dt] = df.iloc[i, df.columns.get_loc(end_col)]
            ts_bounds: pd.Series = pd.Series(data=[i, i], index=[t_st,
                                                                 t_stp])
            
            # cover case when cursor already passed over the end of the row, such as in the case when it was completely encapsulated in a previous event
            if ts_bounds.index.max() != df.iloc[i, df.columns.get_loc(end_col)]:
                continue
            
            # starts
            ts_starts: pd.Series = intersections[intersections.ref_idx.isin([i])].set_index('start_dt').src_idx.copy(deep=True)
            
            # ends
            ts_ends: pd.Series = intersections[intersections.ref_idx.isin([i])].set_index('end_dt').src_idx.copy(deep=True)
            
            # add starts -1 to the ends to reflect time up to that moment setting the values to i
            tmp_ends: pd.Series = pd.Series(index=ts_starts.copy(deep=True).index - dif_unit, dtype=float).fillna(i).astype(int)
            
            # add ends +1 to the starts to reflect after the border setting the values to i
            tmp_starts: pd.Series = pd.Series(index=ts_ends.copy(deep=True).index + dif_unit, dtype=float).fillna(i).astype(int)
            
            # together all of these conditions define all of the discrete bounds of intersection that are possible without repeats or discontinuities in time
            ts_seq: pd.Series = pd.concat([ts_bounds, ts_starts, ts_ends, tmp_ends, tmp_starts], ignore_index=False).sort_index(ascending=True)
            
            # filter to ensure there are no time periods already covered, or the diffing extends into a future time
            ts_seq: pd.Series = ts_seq[(ts_seq.index >= t_st) & (ts_seq.index <= t_stp)]
            
            # aggregate duplicate bounds and convert to series of sets
            ts_seq: pd.Series = ts_seq.groupby(level=0).agg({set}).iloc[:, 0]
            
            # interate through all intersections that contain the bound, as well as the primary sequence. Use a copy as to not modify the source while iterating through it.
            for idx , tmp_set in ts_seq.copy(deep=True).items():

                # attempt to add the base incase not present
                tmp_set.add(i)
                
                # iterate through the intersections to find if they intersect with the boundary line
                for _, row in intersections[intersections.ref_idx == i].iterrows():
                    
                    if (idx >= row.start_dt) and (idx <= row.end_dt):
                        # if intersect add to the set
                        tmp_set.add(row.src_idx)
                        
                # update the source
                ts_seq[idx] = tmp_set
                
            # convert to dataframe and format for appending to output
            out_stage: pd.DataFrame = ts_seq.rename('ids').reset_index(drop=False).rename(columns={'index': 'boundary'})
            
            ## Determne Start and End times for each segment###
            
            # assign group id in order to easily groupby each segment
            out_stage['id_group'] = (out_stage.ids != out_stage.ids.shift()).astype(int).cumsum()
            
            # append to output and update cursor with end of time window after grouping by id_group and getting min & max of each segment
            out.append(out_stage.groupby('id_group')['ids'].first().to_frame()
                       .merge(out_stage.groupby('id_group')['boundary'].min().rename(start_col).to_frame()
                              .merge(out_stage.groupby('id_group')['boundary'].max().rename(end_col),
                                     how='left', left_index=True, right_index=True),
                              how='left',
                              left_index=True, right_index=True).reset_index(drop=True))

            cursor_dt: Union[int, dt] = df.iloc[i, df.columns.get_loc(end_col)]
                
        out: pd.DataFrame = pd.concat(out, axis=0, ignore_index=True)

        return df, intersections, out

    else:
        # group connected components together
        n_components, indices = connected_components(graph)
        
        # return processed origninal df sorted by start col, which indexes are touching (but not how they interact), and the start and stop time of each distinct group along with what indexes are involved
        return df, indices, df[[start_col, end_col]].reset_index(drop=False).rename(columns={'index': 'ids'})\
            .groupby(indices, group_keys=False).agg({start_col: 'min', end_col: 'max', 'ids': set})
       