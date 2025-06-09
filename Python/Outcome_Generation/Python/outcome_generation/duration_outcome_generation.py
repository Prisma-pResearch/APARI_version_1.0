# -*- coding: utf-8 -*-
"""
Created on Wed Dec 23 12:49:47 2020

@author: ruppert20
"""
import pandas as pd
import sqlite3 as sq
from ..Utilities.Logging.log_messages import log_print_email_message as logm


def prepare_for_computaiton(source_df: pd.DataFrame,
                            df: pd.DataFrame,
                            df_name: str,
                            time_intervals: list,
                            label: str,
                            visit_start_col: str,
                            visit_end_col: str,
                            visit_detail_end_col: str,
                            visit_detail_type: str) -> tuple:
    """
    Prepare for duration outcome calculation by storing processed data into sqlite database.

    Parameters
    ----------
    source_df : pd.DataFrame
        source dataframe to store in database.
    df : pd.DataFrame
        dataframe to store in database.
    df_name : str
        table name used in sqlite database.
    time_intervals : list
        list of time intervals to compute.
    label : str
        label for the outcome in the output columns.

    Returns
    -------
    tuple (conn: sqlite database connection,
           columns: list of columns to compute,
           source_df: processed pandas dataframe to iterate over in next step)
        DESCRIPTION.

    """
    # create sqlite database in memory
    conn = sq.connect(':memory:')

    # add data to db
    df.to_sql(df_name, conn, index=False)

    columns: list = []

    for interval in time_intervals:
        for k, v in {visit_detail_type: visit_detail_end_col, 'adm': visit_start_col}.items():

            temp_col: str = f'{k}_{interval.lower()}'

            if interval != 'disch':
                source_df.loc[:, temp_col] = source_df.loc[:, v].apply(lambda x: x + pd.to_timedelta(interval))

            columns.append(temp_col)

    # create deep copy with clean index
    source_df = source_df.copy().reset_index(drop=True)

    for col in columns:
        source_df[f'{label}_{col}'] = None

    source_df.loc[:, 'max_d'] = source_df.loc[:, [visit_end_col] + [x for x in columns if 'disch' not in x]].apply(max, axis=1)

    # add source data to db
    source_df.to_sql('base_df', conn, index=False)

    return conn, columns, source_df


def compute_durations(conn,
                      df: str,
                      columns: list,
                      source_df: pd.DataFrame,
                      censor_start: str,
                      label: str,
                      pid: str,
                      row_index: str,
                      mortality_inclusive_durations: list,
                      visit_end_col: str,
                      visit_detail_type: str) -> pd.DataFrame:
    """
    Compute duration based outcomes using a sqlite database and source dataframe.

    Actions:
        1. Iterate through the source dataframe to check generate the cumulative exposure to various conditions within certain time constraints relative to the visit_detail and the visit

    Parameters
    ----------
    conn : TYPE
        sqlite database connection.
    df : str
        name of the database table to check.
    columns : list
        list of outcome columns to generate.
    source_df : pd.DataFrame
        dataframe containig index of visit_detail and visit times and ids.
    censor_start : str
        the column used as a minimum valid datetime e.g. visit_start_col or visit_detail_end_col.
    label : str
        label added to label the type of outcome e.g. mv.
    pid : str
        patient id column.
    row_index : str
        the row index used to iterate over the source df (e.g. encounter_deiden_id, unique_index_col, merged_enc_id).
    mortality_inclusive_durations : list
        list of durations that will count time that is dead towards exposure to a given condition (e.g. '30d').

    Returns
    -------
    source_df : TYPE
        DESCRIPTION.

    """
    # create mv outcomes for each unique_index_col that have a visit_detail_end_datetime
    for _, row in source_df.copy().iterrows():

        if isinstance(getattr(row, censor_start), pd.Timestamp):

            temp_row = pd.read_sql(f'''select
                                           *
                                       from
                                           base_df
                                       WHERE
                                           base_df.{row_index} = {("'" + getattr(row, row_index) + "'") if (type(getattr(row, row_index)) == str) else getattr(row, row_index)}''',
                                   conn, parse_dates=['max_d', censor_start]).iloc[0, :]

            # develop query for mv times
            query = f'''SELECT
                            {("'" + getattr(row, row_index) + "'") if (type(getattr(row, row_index)) == str) else getattr(row, row_index)} {row_index},'''

            lb: str = '\n'

            for col in columns:
                query += f"""{lb}min(julianday({df}.end_datetime),
                                     julianday('{getattr(temp_row, col.replace('adm_disch', visit_end_col).replace(f'{visit_detail_type}_disch', visit_end_col))}')) - max(julianday('{getattr(temp_row, censor_start)}'),
                                                                                                                                                           julianday({df}.start_datetime)) as {label}_{col},"""

            query = query[:-1] + f'''{lb}FROM
                                        {df}
                                    WHERE
                                        {df}.{pid} = {("'" + getattr(temp_row, pid) + "'") if (type(getattr(temp_row, pid)) == str) else getattr(temp_row, pid)}
                                        AND
                                        julianday({df}.end_datetime) > julianday('{getattr(temp_row, censor_start)}')
                                        AND
                                        julianday({df}.start_datetime) < julianday('{temp_row.max_d}')
                                '''

            # read result
            result = pd.read_sql(query, conn)

            if result.shape[0] == 0:
                # fill in zeros for durations
                source_df.loc[(source_df[row_index] == getattr(row, row_index)), [f'{label}_{x}' for x in columns]] = 0

                # fill in zeros or n's depending on which is appropriate
                for interval in columns:

                    if 'disch' in interval:
                        n = (row[visit_end_col].normalize() - getattr(row, censor_start).normalize()).days + 1
                    else:
                        n = int(interval.split('_')[-1][:-1])

                    source_df.loc[(source_df[row_index] == getattr(row, row_index)), f'{label}_{interval}_cal'] = 0

                    source_df.loc[(source_df[row_index] == getattr(row, row_index)), f'{label}_free_{interval}_cal'] = n

                if visit_detail_type in columns[0]:
                    temp_lab: str = visit_detail_type
                else:
                    temp_lab: str = 'adm'

                source_df.loc[(source_df[row_index] == getattr(row, row_index)), f'{label}_{temp_lab}_disch_gt_48h'] = 0
                source_df.loc[(source_df[row_index] == getattr(row, row_index)), f'{label}_{temp_lab}_disch_gt_2d'] = 0

                continue

            # get all numeric values
            num = result._get_numeric_data()

            # set all negative values to none
            num[num < 0] = None

            tmp: pd.DataFrame = result.groupby(row_index).agg('sum').reset_index()

            source_df.loc[(source_df[row_index] == getattr(row, row_index)), [f'{label}_{x}' for x in columns]] = tmp[[f'{label}_{x}' for x in columns]].iloc[0, :].to_numpy()

            # compute calendar day outcomes
            days_to_query = f'''SELECT DISTINCT
                            base_df.{row_index},
                            max(julianday(date(base_df.{censor_start})), julianday(date({df}.start_datetime))) - julianday(date(base_df.{censor_start})) as cal_days_to_{label},
                            min(julianday(date({df}.end_datetime)), julianday(date(base_df.max_d))) - max(julianday(date(base_df.{censor_start})), julianday(date({df}.start_datetime))) as {label}_duration_cal_days,
                            max(julianday(base_df.{censor_start}), julianday({df}.start_datetime)) - julianday(base_df.{censor_start}) as days_to_{label},
                            julianday(date(base_df.{visit_end_col})) - julianday(date(base_df.{censor_start})) as dischg_relative_delta
                            FROM
                                base_df
                                LEFT JOIN {df} ON base_df.{pid} = {df}.{pid}
                            WHERE
                                base_df.{row_index} = {("'" + getattr(row, row_index) + "'") if (type(getattr(row, row_index)) == str) else getattr(row, row_index)}
                                AND
                                julianday({df}.end_datetime) > julianday(base_df.{censor_start})
                                AND
                                julianday({df}.start_datetime) < julianday(base_df.max_d)
                            ORDER BY
                            max(julianday(date(base_df.{censor_start})), julianday(date({df}.start_datetime))) - julianday(date(base_df.{censor_start}))'''

            days_to_query_result: pd.DataFrame = pd.read_sql(days_to_query, conn)

            # add one day to each duration
            days_to_query_result.loc[:, f'{label}_duration_cal_days'] = days_to_query_result.loc[:, f'{label}_duration_cal_days'].apply(lambda x: x + 1)

            # print(days_to_query_result)

            # base_series = pd.Series(index=list(range(0, int(days_to_query_result.iloc[-1, 1:3].sum()))), dtype='float64')

            base_series = pd.Series(index=list(range(0, (temp_row.max_d.normalize() - temp_row[censor_start].normalize()).days + 1)), dtype='float64')

            for _, row2 in days_to_query_result.iterrows():

                for j in range(0, int(row2[f'{label}_duration_cal_days'])):

                    base_series[row2[f'cal_days_to_{label}'] + j] = 1

            if visit_detail_type in columns[0]:
                temp_lab: str = visit_detail_type
            else:
                temp_lab: str = 'adm'

            source_df.loc[(source_df[row_index] == getattr(row, row_index)), f'cal_days_to_{temp_lab}_{label}'] = days_to_query_result.iloc[0, 1]

            source_df.loc[(source_df[row_index] == getattr(row, row_index)), f'days_to_{temp_lab}_{label}'] = days_to_query_result.iloc[0, 3]

            for interval in columns:

                if 'disch' in interval:
                    n = int(days_to_query_result.iloc[0, 4]) + 1
                else:
                    n = int(interval.split('_')[-1][:-1])

                source_df.loc[(source_df[row_index] == getattr(row, row_index)), f'{label}_{interval}_cal'] = sum(base_series.iloc[0: n].dropna())

                source_df.loc[(source_df[row_index] == getattr(row, row_index)), f'{label}_free_{interval}_cal'] = n - sum(base_series.iloc[0: n].dropna())

                if interval in mortality_inclusive_durations:

                    if isinstance(row.clean_death_date, pd.Timestamp):

                        relative_death_date = (row.clean_death_date.normalize() - getattr(row, censor_start).normalize()).days

                        if ((relative_death_date <= n) & (relative_death_date >= 0)):
                            # calcuated how many days the person was dead in the interval, add one day if the relative death date was not already counted in the pimary outcome
                            dead_days = (n - relative_death_date - 1) + (0 if (base_series[relative_death_date] == 1) else 1)

                            source_df.loc[(source_df[row_index] == getattr(row, row_index)), f'dead_or_{label}_{interval}_cal'] = sum(base_series.iloc[0: n].dropna()) + dead_days

                            source_df.loc[(source_df[row_index] == getattr(row, row_index)), f'alive_{label}_free_{interval}_cal'] = n - sum(base_series.iloc[0: n].dropna()) - dead_days

            source_df.loc[(source_df[row_index] == getattr(row, row_index)), f'{label}_{temp_lab}_disch_gt_48h'] = 1 if source_df.loc[(
                source_df[row_index] == getattr(row, row_index)), f'{label}_{temp_lab}_disch'].iloc[0] > 2 else 0
            source_df.loc[(source_df[row_index] == getattr(row, row_index)), f'{label}_{temp_lab}_disch_gt_2d'] = 1 if source_df.loc[(
                source_df[row_index] == getattr(row, row_index)), f'{label}_{temp_lab}_disch_cal'].iloc[0] > 2 else 0

    return source_df


def generate_duration_outcomes(source_df: pd.DataFrame,
                               df: pd.DataFrame,
                               pid: str,
                               eid: str,
                               unique_index_col: str,
                               visit_start_col: str,
                               visit_end_col: str,
                               visit_detail_start_col: str,
                               visit_detail_end_col: str,
                               prep_func: callable,
                               visit_detail_type: str = 'surg',
                               time_intervals: list = ['2D', '3D', '7D', '30D', 'disch'],
                               label: str = 'mv',
                               mortality_inclusive_durations: list = ['30d'], **logging_kwargs) -> pd.DataFrame:
    """
    Generate duration outcomes.

    Parameters
    ----------
    source_df : pd.DataFrame
        template for calculating the outcomes that must contain the following columns:
            *pid
            *eid
            *unique_index_col
            *visit_start_col
            *dischg_datetime
            *visit_detail_end_col
    df : pd.DataFrame
        data source for calculating the outcomes eg. respiratory, internal stations.
        It must contain the following columns:
            *pid
            *eid
    pid : str
        column name for the patient id
    eid : str
        column name for the encounter id
    prep_func : callable
        function to prepare the df passed into the function for outcome generation.
        This function must take the following arguments:
            *pid
            *eid
            *source_df (the same source_df referenced above)
            *df (the same df referenced above)
        and return a dataframe with atleast the following columns:
            *pid
            *eid
            *unique_index_col
            *visit_start_col
            *dischg_datetime
            *visit_detail_end_col
    time_intervals : list, optional
        the time intervals from visit_detail_end_col or visit_start_col to evalute for the complication duration. The default is ['2D', '3D', '7D', '30D', 'disch'].
    label : str, optional
        the name of the outcome e.g mv, icu, etc.. The default is 'mv'.

    Returns
    -------
    tuple
    out : pd.DataFrame
        Dataframe containg the duration outcomes.
    log: pd.DataFrame
        log file for the process

    """
    logm(message=f'Preparing for {label} duration computation', **logging_kwargs)
    if 'measurement_name' in df.columns:
        # Filter for all respiratory device information
        df = df[df['measurement_name'] == 'respiratory_device']
        df = df.drop(columns=['measurement_name', 'measured_value'])

    conn, columns, base_df = prepare_for_computaiton(source_df=source_df.copy(),
                                                     df=prep_func(source_df=source_df.copy(),
                                                                  visit_detail_start_col=visit_detail_start_col,
                                                                  visit_detail_end_col=visit_detail_end_col,
                                                                  eid=eid,
                                                                  pid=pid,
                                                                  df=df.copy()),
                                                     df_name=label,
                                                     visit_start_col=visit_start_col,
                                                     visit_end_col=visit_end_col,
                                                     visit_detail_end_col=visit_detail_end_col,
                                                     visit_detail_type=visit_detail_type,
                                                     time_intervals=time_intervals,
                                                     label=label)

    logm(message=f'calculating {label} postop intervals', **logging_kwargs)

    visit_detail_outcomes = compute_durations(conn=conn, df=label, columns=[x for x in columns if visit_detail_type in x], source_df=source_df.copy(),
                                              visit_end_col=visit_end_col, visit_detail_type=visit_detail_type,
                                              censor_start=visit_detail_end_col, label=label, pid=pid, row_index=unique_index_col,
                                              mortality_inclusive_durations=mortality_inclusive_durations)

    logm(message=f'calculating {label} encounter intervals', **logging_kwargs)

    admit_outcomes = compute_durations(conn=conn, df=label, columns=[x for x in columns if 'adm' in x],
                                       visit_end_col=visit_end_col, visit_detail_type=visit_detail_type,
                                       source_df=source_df.copy().drop_duplicates(subset=[eid]).dropna(subset=[eid]),
                                       censor_start=visit_start_col, label=label, pid=pid, row_index=eid,
                                       mortality_inclusive_durations=mortality_inclusive_durations)

    logm(message=f'formatting {label} results', **logging_kwargs)

    out = visit_detail_outcomes[[x for x in visit_detail_outcomes.columns if 'adm_' not in x]].merge(admit_outcomes[[eid] + [x for x in admit_outcomes.columns if 'adm_' in x]],
                                                                                                     how='left',
                                                                                                     on=eid)

    # get all numeric values
    num = out._get_numeric_data()

    # set all positive values below 0.001 to 0
    num[((num < 1E-3) & (num > 0))] = 0

    for interval in mortality_inclusive_durations:

        for col in [x for x in out.columns if ((f'{interval}_cal' in x) and ('alive' not in x) and ('dead' not in x))]:

            prefix = 'alive' if 'free' in col else 'dead_or'

            temp_col: str = f'{prefix}_{col}'

            if temp_col not in out.columns:
                out.loc[:, temp_col] = out.loc[:, col]
            else:
                out.loc[out[temp_col].isna(), temp_col] = out.loc[out[temp_col].isna(), col]

    return out
