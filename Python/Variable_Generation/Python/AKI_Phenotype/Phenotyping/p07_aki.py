# -*- coding: utf-8 -*-
"""
Created on Mon Nov 30 21:27:45 2020

@author: renyuanfang
"""
import os
import pandas as pd
from datetime import timedelta
import numpy as np
from Phenotyping.utils import eGFR_fun, KeGFR_fun
from Utils.file_operations import load_data


def p07_find_minimum_creatinine_within_past_days(creatinine: pd.DataFrame, past_day: int, col_name: str, eid: str):
    '''
    This function is used to find the minimum creatinine within past predefined days.
    For each row with a time point x, and parameter past_day = y, this function find the minimum creatinine within time periods [x - y days, x].

    Parameters
    ----------
        creatinine: pandas.DataFrame
            creatinine dataframe, must contain following columns:
            * pid
            * eid
            * lab_result
            * inferred_specimen_datetime
        past_day: int
            define the look back window
        col_name: str
            column name of the result column
        eid: str
            encounter id column name, default value = ''

    Returns
    -------
    pandas.DataFrame
        filtered creatinine dataframe
    -----
    '''
    creatinine[col_name] = creatinine['lab_result']
    creatinine = creatinine.sort_values([eid, 'inferred_specimen_datetime']).reset_index(drop=True)
    i = 1
    while True:
        creatinine['prev_cr'] = creatinine.groupby([eid])['lab_result'].shift(i)
        creatinine['prev_cr_time'] = creatinine.groupby([eid])['inferred_specimen_datetime'].shift(i)
        creatinine['gap'] = (creatinine['inferred_specimen_datetime'] - creatinine['prev_cr_time']) / timedelta(days=1)
        con = creatinine['gap'] <= past_day
        if sum(con) > 0:
            creatinine.loc[con, col_name] = creatinine.loc[con, [col_name, 'prev_cr']].apply(lambda x: np.min(x), axis=1)
            i += 1
        else:
            break
    creatinine = creatinine.drop(columns=['prev_cr', 'prev_cr_time', 'gap'])
    return creatinine

def p07_determine_aki(creatinine):
    '''
    The function is used to determine if patient has aki if one of following conditions is satified:
        * under RRT
        * creatinine >= 4
        * increasement of creatinine within 48 hours is greater than 0.3
        * creatinine / reference creatinine >= 1.5

    1. Split the data into two parts, one part is within 7 days admission,
       and another part is greater than 7 days admission (if an encounter do not have creatinine within 7 days admission,
       the first creatinine is treated as within 7 days admission)
    2. For data within 7 days admission, use reference creatinine determined when calculating egfr staging (determinined before running this function).
    3. For data greater than 7 days admission, we determine reference creatinine as
        * if previous avaliable date or current date (date of current datetime) has aki, we use last available creatinine
        * else use minimum creatinine within last 7 days.
    4. determine aki if one of four conditions is satisfied.
    5. update aki flag for current date using the worst aki flag during that day.

    Parameters
    ----------
        creatinine: pandas.DataFrame
            creatinine dataframe, must contain following columns:
            * elsp_days
            * under_rrt
            * creatinine_greater_4
            * creatinine_increase_greater_03
            * lab_ref_cr_ration_greater_1_5
            * specimen_date
            * inferred_specimen_datetime
            * reference_creatinine
            * minimum_creatinine_past_7d
            * eid
            This dataframe should be sorted by column [eid, inferred_specimen_datetime]

    Returns
    -------
    pandas.DataFrame
        creatinine dataframe with updated columns 'reference_creatinine' and 'lab_ref_cr_ration_greater_1_5'; and newly inserted columns 'aki_flag'.

    Notes
    -----
    Can add a floatchart for determining reference creatinine when doing aki determination
    '''
    ####check if all creatinine within 7 days admission
    creatinine = creatinine.copy().reset_index(drop=True)
    con = creatinine['elsp_days'] > 7
    if sum(con) == 0:
        ####if within the 7 days admission, RC do not change.
        creatinine['aki_flag'] = 0
        creatinine.loc[creatinine['under_rrt'] + creatinine['creatinine_greater_4'] + creatinine['creatinine_increase_greater_03'] + creatinine['lab_ref_cr_ration_greater_1_5'] > 0, 'aki_flag'] = 1
    else:
        all_dates = creatinine['specimen_date'].unique().tolist()
        ####If first creatinine is not within 7 days, RC do not change
        if sum(~con) == 0:
            con.iloc[0] = False
        creatinine['aki_flag'] = np.nan
        creatinine.loc[~con, 'aki_flag'] = creatinine.loc[~con, ['under_rrt', 'creatinine_greater_4', 'creatinine_increase_greater_03', 'lab_ref_cr_ration_greater_1_5']].apply(lambda x: 1 if sum(x) > 0 else 0, axis=1)

        ####processing creatinine out of 7 days admission range
        aki_flags = creatinine[~con].groupby(['specimen_date'])['aki_flag'].max().tolist()
        i = sum(~con)
        while i < len(creatinine):
            ####last date has aki_flag, RC do not change
            index = len(aki_flags) - 1
            cur_date = creatinine.iloc[i]['specimen_date']
            if cur_date == all_dates[index]:
                ###if cur_date or last_date have aki, we use last available creatinine
                if aki_flags[index] == 1 or (index > 0 and aki_flags[index - 1] == 1):
                    ref_creatinine = creatinine.iloc[i-1]['reference_creatinine']
                else:
                    ref_creatinine = creatinine.iloc[i]['minimum_creatinine_past_7d']
            else:
                ###if last_date have aki, we use last available creatinine
                if aki_flags[index] == 1:
                    ref_creatinine = creatinine.iloc[i-1]['reference_creatinine']
                else:
                    ref_creatinine = creatinine.iloc[i]['minimum_creatinine_past_7d']
            if creatinine.iloc[i]['reference_creatinine'] != ref_creatinine:
                creatinine.at[i, 'reference_creatinine'] = ref_creatinine
                creatinine.at[i, 'lab_ref_cr_ration_greater_1_5'] = (1 if creatinine.iloc[i]['lab_result'] / ref_creatinine >= 1.5 else 0)
            if creatinine.iloc[i]['under_rrt'] + creatinine.iloc[i]['creatinine_greater_4'] + creatinine.iloc[i]['creatinine_increase_greater_03'] + creatinine.iloc[i]['lab_ref_cr_ration_greater_1_5'] > 0:
                creatinine.at[i, 'aki_flag'] = 1
            else:
                creatinine.at[i, 'aki_flag'] = 0

            if cur_date == all_dates[index]:
                aki_flags[index] = max(aki_flags[index], creatinine.iloc[i]['aki_flag'])
            else:
                aki_flags.append(creatinine.iloc[i]['aki_flag'])
            i += 1
    return creatinine

def p07_get_aki_stage(row):
    '''
    This function is used to group AKI stage including 'Stage 3 + RRT', 'Stage 3', 'Stage 2' and 'Stage 1'.

    1. Check if under rrt, if yes, 'Stage 3 + RRT'
    2. Check if creatinine value greater than 4 or creatinine / reference_creatinine ratio is >= 3, if yes, 'Stage 3'
    3. Check if creatinine / reference_creatinine ratio is >= 2, if yes, 'Stage 2'
    4. Otherwise, 'Stage 1'

    Parameters
    ----------
        row: pandas.Series
            A row of a dataframe, must contain the following columns:
                * under_rrt
                * lab_result
                * reference_creatinine
                * creatinine_greater_4

    Returns
    -------
    str
        the AKI stage
    '''
    rcr = row.lab_result / row.reference_creatinine
    if row.under_rrt == 1:
        return 'Stage 3 + RRT'
    elif row.creatinine_greater_4 == 1 or rcr >= 3:
        return 'Stage 3'
    elif rcr >= 2:
        return 'Stage 2'
    else:
        return 'Stage 1'

def p07_calculate_kegfr(kegfr_creatinine, race_correction: bool, eid: str):
    '''
    This function calculates the KeGFR value

    1. Determine base_egfr, base_cr, last_kegfr_dt, last_kegfr_cr and calcuate first KeGFR value
       * if reference creatinine calculated in egfr staging is available:
           base_egfr calcuated using age, sex, race, reference creatinine
           base_cr, last_kegfr_cr = reference creatinine
           last_kegfr_dt = admit_datetime
       * else,
           base_egfr calcuated using age, sex, race, min(first_creatinine, mdrd(if ckd))
           base_cr, last_kegfr_cr =  min(first_creatinine, mdrd(if ckd))
           last_kegfr_dt = datetime of first creatinine
    2. Find first creatinine which is at least 12 hours from last keGFR calculation, calculate KeGFR;
       and use current creatinine value and datetime to update last_kegfr_cr and last_kegfr_dt.
       Note that creatinine between 12 hour gap will not be calculated.
    3. Repeat step 2 until there is no creatinine.

    Parameters
    ----------
        kegfr_creatinine: pandas.DataFrame
            kegfr_creatinine dataframe, must contains the following:
            * eid
            * inferred_specimen_datetime
            * reference_creatinine
            * age
            * sex
            * race
            * admit_datetime
            * ref_cr_lab
            * lab_result
        race_correction: bool
            race correction indicator
        eid: str
            encounter id column, default value = ''

    Returns
    -------
    pandas.DataFrame
        Dataframe with newly generated column 'kegfr'
    '''
    df = kegfr_creatinine.copy()
    first_creatinine = df.sort_values([eid, 'inferred_specimen_datetime']).drop_duplicates([eid], keep='first')
    con = first_creatinine['reference_creatinine'].notnull()
    first_creatinine.loc[con, 'base_egfr'] = first_creatinine.loc[con, ['age', 'sex', 'race', 'reference_creatinine']].apply(lambda x: eGFR_fun(x[0], x[1], x[2], x[3], race_correction), axis=1)
    first_creatinine.loc[con, 'base_cr'] = first_creatinine.loc[con, 'reference_creatinine']
    first_creatinine.loc[con, 'last_kefgr_dt'] = first_creatinine.loc[con, 'admit_datetime']
    first_creatinine.loc[con, 'last_kegfr_cr'] = first_creatinine.loc[con, 'reference_creatinine']

    con = first_creatinine['reference_creatinine'].isnull()
    first_creatinine.loc[con, 'base_egfr'] = first_creatinine.loc[con, ['age', 'sex', 'race', 'ref_cr_lab']].apply(lambda x: eGFR_fun(x[0], x[1], x[2], x[3], race_correction), axis=1)
    first_creatinine.loc[con, 'base_cr'] = first_creatinine.loc[con, 'ref_cr_lab']
    first_creatinine.loc[con, 'last_kefgr_dt'] = first_creatinine.loc[con, 'inferred_specimen_datetime']
    first_creatinine.loc[con, 'last_kegfr_cr'] = first_creatinine.loc[con, 'ref_cr_lab']

    base = first_creatinine[[eid, 'base_egfr', 'base_cr', 'last_kegfr_cr', 'last_kefgr_dt']]
    df_result = []
    while len(df) > 0:
        df = df.merge(base, on=eid, how = 'left')
        con = df['inferred_specimen_datetime'] < df['last_kefgr_dt'] + timedelta(hours=12)
        cr_less_than_12hr = df[con]
        if len(df_result) == 0:
            ###make the first one to be basic egfr
            first = cr_less_than_12hr.drop_duplicates([eid], keep='first')
            first['kegfr'] = first['base_egfr']
            cr_less_than_12hr = cr_less_than_12hr.merge(first['kegfr'], left_index = True, right_index = True, how = 'left')
        df_result.append(cr_less_than_12hr)

        df = df[~con]
        if len(df) == 0:
            break
        temp = df.drop_duplicates([eid], keep='first')
        temp['gap'] = (temp['inferred_specimen_datetime'] - temp['last_kefgr_dt']) / timedelta(hours=1)
        temp.loc[:, 'kegfr'] = temp.loc[:, ['base_cr', 'base_egfr', 'last_kegfr_cr', 'lab_result', 'gap']].apply(lambda x: KeGFR_fun(x[0],x[1], x[2], x[3], x[4]), axis=1)
        temp  = temp.drop(columns=['gap'])
        df_result.append(temp)

        df = df.merge(temp[['kegfr']], left_index = True, right_index =  True, how = 'left')
        df = df[df['kegfr'].isnull()]
        base = temp[[eid, 'base_egfr', 'base_cr', 'lab_result', 'inferred_specimen_datetime']].rename(columns={'lab_result': 'last_kegfr_cr', 'inferred_specimen_datetime': 'last_kefgr_dt'})
        df = df.drop(columns=['base_egfr', 'base_cr', 'last_kegfr_cr', 'last_kefgr_dt', 'kegfr'])
    df = pd.concat(df_result, ignore_index=True)
    df = df.sort_values([eid, 'inferred_specimen_datetime', 'kegfr'])
    return df[[eid, 'inferred_specimen_datetime', 'lab_result', 'kegfr']]


def p07_generate_aki(inmd_dir: str, race_correction: bool, eid: str, pid: str, batch: int):
    '''
    This function is used to generate aki flag and generate summary of aki related statistic.

    1. Preprocessing creatinine dataframe by dropping duplicates, removing creatinine with missing eid.
    2. Find minimum creatinine wihin past 48 hours / 7 days by applying p07_find_minimum_creatinine_within_past_days function.
    3. We only determine aki using creatinine taken from admission day to discharge datetime.
    3. Find recent dialysis time, check if within 7 days
    4. Check if creatinine >= 4
    5. Find base reference creatinine, if reference creatinine calcualted in egfr staging exists, use it;
       otherwise use minimum of ([first creatine, mdrd (if have ckd)]) as reference creatinine
    6. Determine aki by applying p07_determine_aki function
    7. Calculate kiGFR by applying p07_calculate_kegfr function
    8. Generate daily AKI within entire hospital admission
        * generate all calander days from admission to discharge
        * correct aki flag if that day has rrt, and also change worst AKI stage of that day as 'Stage 3 + RRT'
        * carry forward aki flag and worst aki stage for missing ones, if still missing, regard as no aki
    9. Determine AKI episodes, if there are more than 2 continus days with no aki, the next first aki day is the start of new aki episodes.
    10.Find the worst aki stage within en episode, and calculate the days of each episode.
    11.Generate aki summary file and save.

    Parameters
    ----------
        inmd_dir:
            intermediate directory location
        batch: int
            batch number, default value = 0
        race_correction: bool
            race correction indicator
        eid: str
            encounter id column name
        pid: str
            patient id column name, default value = 'patient_deiden_id'

    Returns
    -------
    None

    Notes
    -----
    INSERT FLOWCHART
    '''
    encounter = load_data(os.path.join(inmd_dir, 'encounter_ckd', 'encounter_ckd_{}.csv'.format(batch)),
                          parse_dates=['admit_datetime', 'dischg_datetime'], pid=pid, eid=eid)

    # preprocessing creatinine
    creatinine = load_data(os.path.join(inmd_dir, 'filtered_labs', 'filtered_labs_{}.csv'.format(batch)),
                           usecols=[eid, 'inferred_specimen_datetime', 'lab_result'],
                           parse_dates=['inferred_specimen_datetime'],
                           pid=pid, eid=eid)
    creatinine = creatinine[creatinine[eid].notnull()]
    creatinine = creatinine.drop_duplicates([eid, 'inferred_specimen_datetime', 'lab_result'])
    creatinine['specimen_date'] = creatinine['inferred_specimen_datetime'].dt.date
    creatinine = creatinine.merge(encounter[[eid, 'admit_datetime', 'dischg_datetime', 'age', 'sex',
                                             'race', 'reference_creatinine', 'mdrd', 'ckd']], on=eid, how = 'left')

    # find minimum creatinine wihin past 48 hours / 7 days
    creatinine = p07_find_minimum_creatinine_within_past_days(creatinine=creatinine, past_day=2, col_name='minimum_creatinine_past_48h', eid=eid)
    creatinine = p07_find_minimum_creatinine_within_past_days(creatinine=creatinine, past_day=7, col_name='minimum_creatinine_past_7d', eid=eid)
    creatinine = creatinine[(creatinine['inferred_specimen_datetime'].dt.date >= creatinine['admit_datetime'].dt.date) & (creatinine['inferred_specimen_datetime'] <= creatinine['dischg_datetime'])]
    creatinine['creatinine_increase_greater_03'] = 0
    creatinine.loc[creatinine['lab_result'] - creatinine['minimum_creatinine_past_48h'] >= 0.3, 'creatinine_increase_greater_03'] = 1

    # find recent dialysis time, check if within 7 days
    dialysis = load_data(os.path.join(inmd_dir, 'dialysis_time', 'dialysis_time_{}.csv'.format(batch)),
                         parse_dates=['dialysis_time'], pid=pid, eid=eid)
    if len(dialysis) == 0:
        creatinine['under_rrt'] = 0
        creatinine['recent_rrt_time_7_days'] = np.nan
    else:
        dialysis = dialysis.sort_values(['dialysis_time'])
        creatinine = creatinine.sort_values(['inferred_specimen_datetime'])
        creatinine = pd.merge_asof(creatinine, dialysis, left_on='inferred_specimen_datetime', right_on='dialysis_time',
                                   by = eid, direction='backward')
        creatinine = creatinine.rename(columns={'dialysis_time': 'recent_rrt_time_7_days'})
        creatinine['under_rrt'] = 0
        creatinine.loc[creatinine['inferred_specimen_datetime'].dt.date <= creatinine['recent_rrt_time_7_days'].dt.date + timedelta(days=7), 'under_rrt'] = 1
        creatinine.loc[creatinine['under_rrt'] == 0, 'recent_rrt_time_7_days'] = np.nan

    ###creatinine >= 4
    creatinine['creatinine_greater_4'] = 0
    creatinine.loc[creatinine['lab_result'] >= 4, 'creatinine_greater_4'] = 1

    ###check if cre/ref_cre >= 1.5
    creatinine['elsp_days'] = (creatinine['inferred_specimen_datetime'].dt.date - creatinine['admit_datetime'].dt.date) / timedelta(days=1)
    creatinine = creatinine.sort_values([eid, 'inferred_specimen_datetime']).reset_index(drop=True)
    first_creatinine = creatinine.drop_duplicates([eid])
    first_creatinine = first_creatinine[first_creatinine['reference_creatinine'].isnull()]
    first_creatinine['reference_creatinine'] = first_creatinine['lab_result']
    con = first_creatinine['ckd'].isin([0, '0', 'Insufficient Data'])
    first_creatinine.loc[con, 'reference_creatinine'] = first_creatinine.loc[con, ['lab_result', 'mdrd']].apply(lambda x: np.min(x), axis=1)
    first_creatinine = first_creatinine[[eid, 'reference_creatinine']].rename(columns={'reference_creatinine': 'ref_cr_lab'})

    creatinine = creatinine.merge(first_creatinine, on=eid, how = 'left')
    ####copy one creatinine used to calculate kegfr
    kegfr_creatinine = creatinine.copy()
    con = creatinine['reference_creatinine'].isnull()
    creatinine.loc[con, 'reference_creatinine'] = creatinine.loc[con, 'ref_cr_lab']
    creatinine = creatinine.drop(columns=['ref_cr_lab'])

    creatinine['lab_ref_cr_ration_greater_1_5'] = 0
    creatinine.loc[(creatinine['lab_result'] / creatinine['reference_creatinine']) >= 1.5, 'lab_ref_cr_ration_greater_1_5'] = 1

    ###determine aki for each creatinine
    creatinine = creatinine.groupby([eid]).apply(p07_determine_aki).reset_index(drop=True)

    ###determine aki stage
    creatinine['aki_stage'] = np.nan
    creatinine.loc[creatinine['aki_flag'] == 1, 'aki_stage'] = creatinine.loc[creatinine['aki_flag'] == 1].apply(p07_get_aki_stage, axis=1)

    ###calculate kegfr
    kegfr = p07_calculate_kegfr(kegfr_creatinine=kegfr_creatinine, race_correction=race_correction, eid=eid)
    creatinine = creatinine.merge(kegfr, on = [eid, 'inferred_specimen_datetime', 'lab_result'], how = 'left')

    ####add aki flag dialy
    aki_day = creatinine.groupby([eid, 'specimen_date'])['aki_flag'].max().reset_index()
    aki_stage = creatinine[creatinine['aki_flag'] == 1].groupby([eid, 'specimen_date'])['aki_stage'].max().reset_index()
    aki_day = aki_day.merge(aki_stage, on = [eid, 'specimen_date'], how = 'left')
    aki_day['aki_stage'] = aki_day['aki_stage'].fillna('no stage')

    dischg_date = creatinine[[eid, 'dischg_datetime']]
    dischg_date['dischg_date'] = pd.to_datetime(dischg_date['dischg_datetime']).dt.date
    dischg_date = dischg_date[[eid, 'dischg_date']].rename(columns={'dischg_date': 'specimen_date'}).drop_duplicates()
    aki_day = pd.concat([aki_day, dischg_date], ignore_index = True)
    aki_day = aki_day.sort_values([eid, 'specimen_date']).drop_duplicates([eid, 'specimen_date'])

    aki_day['next_date'] = aki_day.groupby([eid])['specimen_date'].shift(-1)
    aki_day['gap'] = (aki_day['next_date'] - aki_day['specimen_date']) / timedelta(days=1)

    missing = aki_day[aki_day['gap'] > 1]
    missing['t_id'] = missing[eid]
    df_list = []
    for row in missing.itertuples():
        e_id = row.t_id
        begin_date = row.specimen_date
        gap = int(row.gap)
        dates = []
        for i in range(1, gap):
            dates.append(begin_date + timedelta(days=i))
        df = pd.DataFrame({'specimen_date': dates})
        df[eid] = e_id
        df_list.append(df)

    aki_day = pd.concat([aki_day] + df_list, ignore_index=True)
    aki_day = aki_day.sort_values([eid, 'specimen_date'])

    # correct aki flag and worst aki stage using rrt information
    if len(dialysis) > 0:
        # get all days under dialysis
        dialysis['dialysis_day'] = dialysis['dialysis_time'].dt.date
        rrt_days = dialysis[[eid, 'dialysis_day']].drop_duplicates()
        rrt_days['flag'] = 1
        aki_day = aki_day.merge(rrt_days, left_on=[eid, 'specimen_date'], right_on=[eid, 'dialysis_day'], how = 'left')
        con = (aki_day['flag'] == 1) & (aki_day['aki_stage'] != 'Stage 3 + RRT')
        aki_day.loc[con, 'aki_flag'] = 1
        aki_day.loc[con, 'aki_stage'] = 'Stage 3 + RRT'
        aki_day = aki_day.drop(columns=['dialysis_day', 'flag'])

    aki_day['aki_flag'] = aki_day.groupby([eid])['aki_flag'].ffill()
    aki_day['aki_stage'] = aki_day.groupby([eid])['aki_stage'].ffill()
    aki_day.loc[aki_day['aki_flag'] == 0, 'aki_stage'] = np.nan
    aki_day = aki_day.drop(columns=['next_date', 'gap'])

    ####get episodes
    aki_episodes = aki_day[[eid, 'specimen_date', 'aki_flag']]
    aki_episodes['prev_aki'] = aki_episodes.groupby([eid])['aki_flag'].shift(1)
    aki_episodes['prev_sec_aki'] = aki_episodes.groupby([eid])['aki_flag'].shift(2)
    aki_episodes['next_aki'] = aki_episodes.groupby([eid])['aki_flag'].shift(-1)
    aki_episodes['next_sec_aki'] = aki_episodes.groupby([eid])['aki_flag'].shift(-2)

    aki_episodes['episode_begin'] = 0
    con = (aki_episodes['aki_flag'] == 1) & (aki_episodes['prev_aki'] != 1) & (aki_episodes['prev_sec_aki'] != 1)
    aki_episodes.loc[con, 'episode_begin'] = 1

    aki_episodes['episode_end'] = 0
    con = (aki_episodes['aki_flag'] == 1) & (aki_episodes['next_aki'] != 1) & (aki_episodes['next_sec_aki'] != 1)
    aki_episodes.loc[con, 'episode_end'] = 1

    aki_episodes = aki_episodes[aki_episodes['episode_begin'] + aki_episodes['episode_end'] > 0]
    aki_episodes = aki_episodes[[eid, 'specimen_date', 'episode_begin', 'episode_end']]
    aki_episodes['next_date'] = aki_episodes.groupby([eid])['specimen_date'].shift(-1)

    aki_episodes = aki_episodes[aki_episodes['episode_begin'] == 1]
    aki_episodes['episode_begin_date'] = aki_episodes['specimen_date']
    con = aki_episodes['episode_end'] == 1
    aki_episodes.loc[con, 'episode_end_date'] = aki_episodes.loc[con, 'specimen_date']
    aki_episodes.loc[~con, 'episode_end_date'] = aki_episodes.loc[~con, 'next_date']
    aki_episodes['episode_days'] = (aki_episodes['episode_end_date'] - aki_episodes['episode_begin_date']) / timedelta(days=1)
    aki_episodes['episode_days'] = aki_episodes['episode_days'] + 1
    aki_episodes['flag'] = 1
    aki_episodes['episode'] = aki_episodes.groupby([eid])['flag'].cumsum()
    aki_episodes = aki_episodes[[eid, 'episode_begin_date', 'episode_end_date', 'episode_days', 'episode']]

    # map episode to daily aki
    no_aki = aki_day[aki_day['aki_flag'] == 0]
    have_aki = aki_day[aki_day['aki_flag'] == 1]
    have_aki = have_aki.merge(aki_episodes, on=eid, how = 'left')
    con = (have_aki['specimen_date'] >= have_aki['episode_begin_date']) & (have_aki['specimen_date'] <= have_aki['episode_end_date'])
    have_aki = have_aki[con]
    aki_day = pd.concat([no_aki, have_aki], ignore_index = True)
    aki_day = aki_day.sort_values([eid, 'specimen_date']).drop(columns=['episode_begin_date', 'episode_end_date', 'episode_days'])
    aki_worst_stage = have_aki.groupby([eid, 'episode'])['aki_stage'].max().reset_index()
    aki_episodes = aki_episodes.merge(aki_worst_stage, on = [eid, 'episode'], how = 'left').rename(columns={'aki_stage': 'worst_aki_stage_in_episode'})

    ####get aki_summary
    aki_summary = encounter[[pid, eid, 'admit_datetime', 'dischg_datetime', 'sex', 'race', 'age', 'final_class', 'egfr', 'mdrd']]
    aki_summary = aki_summary[aki_summary[eid].isin(creatinine[eid])]

    aki_day = aki_day.sort_values([eid, 'specimen_date']).reset_index(drop=True)
    creatinine = creatinine.sort_values([eid, 'inferred_specimen_datetime']).reset_index(drop=True)
    dialysis = dialysis.sort_values([eid, 'dialysis_time']).reset_index(drop=True)
    first_aki_date = aki_day[aki_day['aki_flag'] == 1].groupby([eid], as_index = False)['specimen_date'].min().rename(columns={'specimen_date': 'first_aki_date'})
    ref_cr_min = creatinine.sort_values([eid, 'reference_creatinine']).drop_duplicates([eid], keep='first')
    ref_cr_min = ref_cr_min[[eid, 'inferred_specimen_datetime', 'reference_creatinine']].rename(columns={'reference_creatinine': 'min_reference_creatinine', 'inferred_specimen_datetime': 'min_reference_creatinine_datetime'})
    ref_cr_max = creatinine.sort_values([eid, 'reference_creatinine'], ascending=False).drop_duplicates([eid], keep='first')
    ref_cr_max = ref_cr_max[[eid, 'inferred_specimen_datetime', 'reference_creatinine']].rename(columns={'reference_creatinine': 'max_reference_creatinine', 'inferred_specimen_datetime': 'max_reference_creatinine_datetime'})
    first_cr = creatinine[[eid, 'inferred_specimen_datetime', 'lab_result']].drop_duplicates([eid], keep='first').rename(columns={'lab_result': 'first_creatinine_value', 'inferred_specimen_datetime': 'first_creatinine_datetime'})
    last_cr = creatinine[[eid, 'inferred_specimen_datetime', 'lab_result']].drop_duplicates([eid], keep='last').rename(columns={'lab_result': 'last_creatinine_value', 'inferred_specimen_datetime': 'last_creatinine_datetime'})
    cr_min = creatinine.sort_values([eid, 'lab_result']).drop_duplicates([eid], keep='first')
    cr_min = cr_min[[eid, 'inferred_specimen_datetime', 'lab_result']].rename(columns={'lab_result': 'min_creatinine', 'inferred_specimen_datetime': 'min_creatinine_datetime'})
    cr_max = creatinine.sort_values([eid, 'lab_result'], ascending=False).drop_duplicates([eid], keep='first')
    cr_max = cr_max[[eid, 'inferred_specimen_datetime', 'lab_result']].rename(columns={'lab_result': 'max_creatinine', 'inferred_specimen_datetime': 'max_creatinine_datetime'})
    number_episodes = aki_episodes.groupby([eid], as_index=False)['episode'].max().rename(columns={'episode': 'number_of_aki_episodes'})
    stage_aki_days=aki_day.groupby([eid, 'aki_stage'])['episode'].count().reset_index()
    stage_1 = stage_aki_days.loc[stage_aki_days['aki_stage'] == 'Stage 1', [eid, 'episode']].rename(columns={'episode': 'days_in_stage_1'})
    stage_2 = stage_aki_days.loc[stage_aki_days['aki_stage'] == 'Stage 2', [eid, 'episode']].rename(columns={'episode': 'days_in_stage_2'})
    stage_3 = stage_aki_days.loc[stage_aki_days['aki_stage'] == 'Stage 3', [eid, 'episode']].rename(columns={'episode': 'days_in_stage_3'})
    stage_3_rrt = stage_aki_days.loc[stage_aki_days['aki_stage'] == 'Stage 3 + RRT', [eid, 'episode']].rename(columns={'episode': 'days_in_stage_3_rrt'})
    aki_worst_stage = aki_day[[eid, 'specimen_date', 'aki_stage']].sort_values([eid, 'aki_stage'], ascending = False).drop_duplicates([eid], keep='first').rename(columns={'aki_stage': 'worst_aki_staging', 'specimen_date': 'worst_aki_stage_date'})
    aki_worst_stage = aki_worst_stage[aki_worst_stage['worst_aki_staging'].notnull()]
    last_aki = aki_day[[eid, 'aki_flag']].drop_duplicates([eid], keep='last').rename(columns={'aki_flag': 'discharge_aki_status'})
    aki_overall = aki_day.groupby([eid], as_index = False)['aki_flag'].max().rename(columns={'aki_flag': 'aki_overall'})
    first_rrt = dialysis.drop_duplicates([eid], keep='first').rename(columns={'dialysis_time': 'first_rrt_datetime_record'})
    last_rrt = dialysis.drop_duplicates([eid], keep='last').rename(columns={'dialysis_time': 'last_rrt_datetime_record'})
    maximum_episodes = min(aki_episodes['episode'].max(), 20)

    for subdf in [first_aki_date, ref_cr_min, ref_cr_max, first_cr, last_cr, cr_min, cr_max, number_episodes, stage_1, stage_2, stage_3, stage_3_rrt, aki_worst_stage, last_aki, aki_overall, first_rrt, last_rrt]:
        aki_summary = aki_summary.merge(subdf, on=eid, how = 'left')
    for x in ['number_of_aki_episodes', 'days_in_stage_1', 'days_in_stage_2', 'days_in_stage_3', 'days_in_stage_3_rrt']:
        aki_summary[x] = aki_summary[x].fillna(0)
    aki_summary['recurrent_aki'] = np.nan
    aki_summary.loc[aki_summary['number_of_aki_episodes'] == 1, 'recurrent_aki'] = 0
    aki_summary.loc[aki_summary['number_of_aki_episodes'] > 1, 'recurrent_aki'] = 1
    aki_summary['aki_early_3d'] = 0
    con = (aki_summary['first_aki_date'].notnull()) & (aki_summary['first_aki_date'] <= aki_summary['admit_datetime'].dt.date + timedelta(days=2))
    aki_summary.loc[con, 'aki_early_3d'] = 1
    ###find worst stage for aki_early_3d
    worst_stage_early = aki_day.merge(encounter[[eid, 'admit_datetime']], on=eid, how = 'left')
    worst_stage_early = worst_stage_early[(worst_stage_early['aki_stage'].notnull()) & (worst_stage_early['specimen_date'] <= worst_stage_early['admit_datetime'].dt.date + timedelta(days=2))]
    if len(worst_stage_early) > 0:
        worst_stage_early = worst_stage_early.groupby([eid], as_index = False)['aki_stage'].max()
        worst_stage_early = worst_stage_early.rename(columns={'aki_stage': 'worst_aki_stage_3d'})
    else:
        worst_stage_early = pd.DataFrame(columns=[eid, 'worst_aki_stage_3d'])
    aki_summary = aki_summary.merge(worst_stage_early, on=eid, how = 'left')

    for i in range(1, int(maximum_episodes) + 1):
        cur_episode = aki_episodes[aki_episodes['episode'] == i]
        cur_episode['episode_{}'.format(i)] = cur_episode[['episode_begin_date', 'episode_end_date']].apply(lambda x: str(x[0]) + ' - ' + str(x[1]), axis=1)
        cur_episode = cur_episode.rename(columns={'worst_aki_stage_in_episode': 'worst_aki_stage_in_episode_{}'.format(i)})
        cur_episode = cur_episode[[eid, 'episode_{}'.format(i), 'worst_aki_stage_in_episode_{}'.format(i)]]
        aki_summary = aki_summary.merge(cur_episode, on=eid, how = 'left')

    if not os.path.exists(os.path.join(inmd_dir, 'encounter_aki')):
        os.makedirs(os.path.join(inmd_dir, 'encounter_aki'))

    creatinine.to_csv(os.path.join(inmd_dir, 'encounter_aki', 'encounter_final_aki_{}.csv'.format(batch)), index = False)
    aki_summary.to_csv(os.path.join(inmd_dir, 'encounter_aki', 'encounter_aki_summary_{}.csv'.format(batch)), index = False)
    aki_day.to_csv(os.path.join(inmd_dir, 'encounter_aki', 'encounter_aki_daily_{}.csv'.format(batch)), index = False)
    aki_episodes.to_csv(os.path.join(inmd_dir, 'encounter_aki', 'encounter_aki_episodes_{}.csv'.format(batch)), index = False)
