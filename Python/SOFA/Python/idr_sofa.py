# -*- coding: utf-8 -*-
"""
SOFA Calculation Module.

Created on Fri Apr  9 11:36:26 2021

@author: renyuanfang

Modified 9/28/2022 by Ruppert20
"""
import pandas as pd
import numpy as np
from datetime import timedelta


def determine_mv(respiratory: pd.DataFrame, eid: str):
    """
    Determine mechnical ventilation status for SOFA calculation.

    patient has device with ventilator, but not under surgery,
    not in station OR, Procedure suite, and Ward, are treated as under MV.

    Parameters
    ----------
    respiratory: pd.DataFrame
        respiratory dataframe, must contain following columns:
            *eid
            *intraop_y_n
            *station_type
            *device_type
    eid: str
        encounter id column name

    Returns
    -------
    MV dataframe
        contains all the time points under MV.
    """
    respiratory['GA_ind_method'] = np.nan
    respiratory.loc[respiratory['intraop_y_n'] == 'Y', 'GA_ind_method'] = 'or_case'
    con = (respiratory['GA_ind_method'].isnull()) & (respiratory['station_type'].isin(['OR', 'Procedure suite']))
    respiratory.loc[con, 'GA_ind_method'] = 'or_station'
    con = (respiratory['GA_ind_method'].isnull()) & ((respiratory['station_type'] == 'Ward') & (respiratory['device_type'] == 'ventilator'))
    respiratory.loc[con, 'GA_ind_method'] = 'Ward_inv_ventilator'
    respiratory['GA_ind'] = 0
    respiratory.loc[respiratory['GA_ind_method'].notnull(), 'GA_ind'] = 1

    respiratory['device_MV_group'] = respiratory['device_type']
    respiratory.loc[respiratory['GA_ind'] == 1, 'device_MV_group'] = 'GA'

    respiratory['MV'] = 0
    respiratory.loc[respiratory['device_MV_group'] == 'ventilator', 'MV'] = 1
    return respiratory.loc[respiratory['MV'] == 1, [eid, 'respiratory_datetime']].dropna()


def calculate_sofa(encounters: pd.DataFrame, bp_map: pd.DataFrame,
                   gcs_score: pd.DataFrame, respiratory: pd.DataFrame,
                   labs: pd.DataFrame, meds: pd.DataFrame,
                   start_time_col: str, end_time_col: str,
                   frequency: int, lookback_window: int, ff_limit: int,
                   eid: str):
    """
    Calculate sofa score.

    1. extract map values, prefer invasive_map value to noninvasive_map value
    2. extract gcs score
    3. extract fio2, spo2 value from respiratory file, and also mv value by calling function 'determine_mv'
    4. extarct bilirubin, platelets, creatinine, pao2 value from lab file based on loinc codes
    5. extract pressors from meds file, for ['dopamine', 'dobutamine', 'norepinephrine', 'epinephrine'], convert unit
       mg/hr, mg/kg/hr to mcg/kg/min, and only keep values with unit 'mcg/kg/min'
    6. All values extracted from above steps are limited to timeframe [start_time_col, end_time_col], exclude extracted
       values not in the defined range
    7. calculate pf ratio = pao2 / (fio2 / 100), merge pao2 and fio2 where each pao2 matches a fio2 value with the
       maximum time <= pao2 time
    8. calculate spf ratio = (spo2 / (fio2 / 100) - 64) / 0.84, merge spo2 and fio2 using the similar approach in Step 6
    9. merge all extracted and calculated values, sample the time between [start_time_col, end_time_col] into hourly
    10. For each sampled hour, find the worst value with past 'lookback_window' hours.
    11. Only keep the timepoints that are in the frequency we want.
    12. For those are still missing valus, we do carry forward within past 'ff_limit' hours.
    13. Calculate sofa scores individual components, cardio, resp, coag, liver, cns, renal

    Parameters
    ----------
    encounters : pd.DataFrame
        encounters dataframe, must contain following columns:
            *eid
            *start_time_col
            *end_time_col
    bp_map: pd.DataFrame
        blood_pressure dataframe, must contain following columns:
            *eid
            *start_time_col
            *end_time_col
    gcs_score: pd.DataFrame
        glasgow score dataframe, must contain following columns:
            *eid
            *glasgow_coma_datetime
            *glasgow_coma_adult_score
    respiratory: pd.DataFrame
        respiratory dataframe, must contain following columns:
            *eid
            *respiratory_datetime
            *spo2
            *fio2
    labs: pd.DataFrame
        labs dataframe, must contain following columns:
            *eid
            *inferred_spec_datetime
            *stamped_and_inferred_loinc_code
            *cleaned_result
    meds: pd.DataFrame
        meds dataframe, must contain following columns:
            *eid
            *taken_datetime
            *pressor_name
            *total_dose_character
            *med_dose_unit_desc
    start_time_col : str
        column name of encounters dataframe, define the datetime to start producing
    end_time_col : str
        column name of encounters dataframe, define the datetime to end producing
    frequency : int
        define the frequency of calculating sofa score, unit = hour, eg. every 1 hour or 4 hour
    lookback_window : int
        define the lookback window of measurements we use to calculate sofa score, unit = hour, eg. using data within 24 hours to calculate sofa score
    ff_limit : int
        define the feed forward limit, how far back can you look to find a value, when missing from lookback window, unit = hour
    eid: str
        encounter id column name

    Returns
    -------
    None.

    """
    encounters[start_time_col] = pd.to_datetime(encounters[start_time_col].values)
    encounters[end_time_col] = pd.to_datetime(encounters[end_time_col].values)

    # 1. extract maps value
    bp_map['bp_datetime'] = pd.to_datetime(bp_map['bp_datetime'])
    bp_map['map_value'] = bp_map['invasive_map'].copy()
    con = bp_map['map_value'].isnull()
    bp_map.loc[con, 'map_value'] = bp_map.loc[con, 'noninvasive_map'].copy()
    bp_map = bp_map.merge(encounters[[eid, start_time_col, end_time_col]], on=eid, how='left')
    bp_map = bp_map[(bp_map['bp_datetime'] >= bp_map[start_time_col]) & (bp_map['bp_datetime'] <= bp_map[end_time_col])]
    bp_map = bp_map[[eid, 'bp_datetime', 'map_value']].dropna()

    # 2. extract gcs
    gcs_score = gcs_score.query("measurement_name == 'glasgow_coma_adult_score'").rename(columns={'measurement_value': 'glasgow_coma_adult_score'})
    gcs_score['glasgow_coma_datetime'] = pd.to_datetime(gcs_score['glasgow_coma_datetime'])
    gcs_score = gcs_score.merge(encounters[[eid, start_time_col, end_time_col]], on=eid, how='left')
    gcs_score = gcs_score[(gcs_score['glasgow_coma_datetime'] >= gcs_score[start_time_col]) & (gcs_score['glasgow_coma_datetime'] <= gcs_score[end_time_col])]
    gcs_score = gcs_score[[eid, 'glasgow_coma_datetime', 'glasgow_coma_adult_score']].dropna()

    # 3. extract spo2, fio2, ventilation values from respiratory file
    respiratory['respiratory_datetime'] = pd.to_datetime(respiratory['respiratory_datetime'])
    respiratory = respiratory.merge(encounters[[eid, start_time_col, end_time_col]], on=eid, how='left')
    respiratory = respiratory[(respiratory['respiratory_datetime'] >= respiratory[start_time_col]) & (respiratory['respiratory_datetime'] <= respiratory[end_time_col])]
    fio2 = respiratory.query("measurement_name.isin(['fio2_resp', 'fio2_labs'])", engine='python').rename(columns={'measured_value': 'fio2'})[[eid, 'respiratory_datetime', 'fio2']].dropna()
    spo2 = respiratory.query("measurement_name == 'sp02'", engine='python').rename(columns={'measured_value': 'spo2'})[[eid, 'respiratory_datetime', 'spo2']].dropna()
    mv = determine_mv(respiratory=respiratory, eid=eid)
    del respiratory

    # 4. extract bilirubin, creatinine, platelets, PaO2, from labs
    labs['measurement_datetime'] = pd.to_datetime(labs['measurement_datetime'].values)
    labs = labs.merge(encounters[[eid, start_time_col, end_time_col]], on=eid, how='left')
    labs = labs[(labs['measurement_datetime'] >= labs[start_time_col]) & (labs['measurement_datetime'] <= labs[end_time_col])]
    bilirubin = labs.loc[labs.stamped_and_inferred_loinc_code.isin(['42719-5', '1975-2']), [eid, 'measurement_datetime', 'value_as_number']].dropna()
    creatinine = labs.loc[labs.stamped_and_inferred_loinc_code.isin(['38483-4', '2160-0']), [eid, 'measurement_datetime', 'value_as_number']].dropna()
    platelets = labs.loc[labs.stamped_and_inferred_loinc_code.isin(['777-3', '778-1', '26515-7']), [eid, 'measurement_datetime', 'value_as_number']].dropna()
    pao2 = labs.loc[labs.stamped_and_inferred_loinc_code.isin(['19255-9', '2703-7']), [eid, 'measurement_datetime', 'value_as_number']].dropna()
    del labs

    # 5. extract pressor from meds
    pressor_groups = ['dopamine', 'dobutamine', 'norepinephrine', 'epinephrine', 'vasopressin', 'phenylephrine']
    meds['taken_datetime'] = pd.to_datetime(meds['taken_datetime'])
    meds = meds.merge(encounters[[eid, start_time_col, end_time_col]], on=eid, how='left')
    meds = meds[(meds['taken_datetime'] >= meds[start_time_col]) & (meds['taken_datetime'] <= meds[end_time_col])]
    meds = meds[meds['pressor_name'].isin(pressor_groups)]
    con = (meds['pressor_name'].isin(['dopamine', 'dobutamine', 'norepinephrine', 'epinephrine'])) & (meds['med_dose_unit_desc'] == 'mg/kg/hr')
    meds.loc[con, 'total_dose_character'] = meds.loc[con, 'total_dose_character'].apply(lambda x: x * 1000 / 60)
    meds.loc[con, 'med_dose_unit_desc'] = 'mcg/kg/min'
    con = (meds['pressor_name'].isin(['dopamine', 'dobutamine', 'norepinephrine', 'epinephrine'])) & (meds['med_dose_unit_desc'] == 'mg/hr') & (meds['med_dosing_weight'].notnull())
    meds.loc[con, 'total_dose_character'] = meds.loc[con, ['total_dose_character', 'med_dosing_weight']].apply(lambda x: x[0] * 1000 / 60 / x[1], axis=1)
    meds.loc[con, 'med_dose_unit_desc'] = 'mcg/kg/min'

    con = (meds['total_dose_character'] > 0) & (meds['pressor_name'].isin(['vasopressin', 'phenylephrine']))
    vas_phe = meds[con]
    con = (meds['pressor_name'].isin(['dopamine', 'dobutamine', 'norepinephrine', 'epinephrine'])) & (meds['total_dose_character'] > 0) & (meds['med_dose_unit_desc'] == 'mcg/kg/min')
    other_pressors = meds[con]
    con = ((other_pressors['pressor_name'] == 'dopamine') & (other_pressors['total_dose_character'] <= 50)) | ((other_pressors['pressor_name'] == 'dobutamine') & (other_pressors['total_dose_character'] <= 40)) \
        | ((other_pressors['pressor_name'] == 'norepinephrine') & (other_pressors['total_dose_character'] <= 15)) | ((other_pressors['pressor_name'] == 'epinephrine') & (other_pressors['total_dose_character'] <= 5))
    other_pressors = other_pressors[con]
    meds = pd.concat([vas_phe, other_pressors], ignore_index=True)
    meds = meds[[eid, 'taken_datetime', 'total_dose_character', 'pressor_name']].dropna()

    # 7. deal with outlier
    fio2.fio2 = fio2.fio2.astype(float).astype(int)
    spo2.spo2 = spo2.spo2.astype(float).astype(int)
    gcs_score.glasgow_coma_adult_score = gcs_score.glasgow_coma_adult_score.astype(float).astype(int)
    fio2 = fio2[(fio2['fio2'] >= 21) & (fio2['fio2'] <= 100)]
    spo2 = spo2[(spo2['spo2'] > 1) & (spo2['spo2'] <= 100)]
    gcs_score = gcs_score[(gcs_score['glasgow_coma_adult_score'] >= 3) & (gcs_score['glasgow_coma_adult_score'] <= 15)]
    bilirubin = bilirubin[(bilirubin['value_as_number'] >= 0.03) & (bilirubin['value_as_number'] <= 44)]
    platelets = platelets[(platelets['value_as_number'] >= 2) & (platelets['value_as_number'] <= 1900)]
    creatinine = creatinine[(creatinine['value_as_number'] >= 0.1) & (creatinine['value_as_number'] <= 20)]
    pao2 = pao2[(pao2['value_as_number'] > 0) & (pao2['value_as_number'] <= 800)]
    bp_map = bp_map[(bp_map['map_value'] > 0) & (bp_map['map_value'] <= 300)]

    # 8. for map, gcs, bilirubin, platelets, creatinine, pressor, ventilation, find the hourly frequency data
    gcs_score = gcs_score.rename(columns={'glasgow_coma_datetime': 'time'})
    bp_map = bp_map.rename(columns={'bp_datetime': 'time'})
    bilirubin = bilirubin.rename(columns={'measurement_datetime': 'time', 'value_as_number': 'bilirubin'})
    platelets = platelets.rename(columns={'measurement_datetime': 'time', 'value_as_number': 'platelets'})
    creatinine = creatinine.rename(columns={'measurement_datetime': 'time', 'value_as_number': 'creatinine'})
    pao2 = pao2.rename(columns={'measurement_datetime': 'time', 'value_as_number': 'pao2'})
    spo2 = spo2.rename(columns={'respiratory_datetime': 'time'})
    fio2 = fio2.rename(columns={'respiratory_datetime': 'time'})

    pao2 = pao2.sort_values(['time'])
    fio2 = fio2.sort_values(['time'])
    pf = pd.merge_asof(pao2, fio2, on='time', by=eid, direction='backward')
    pf['fio2'] = pf['fio2'].fillna(21)
    pf['pf'] = pf['pao2'] / (pf['fio2'] / 100)
    pf = pf.drop(columns=['pao2', 'fio2'])

    spo2 = spo2.sort_values(['time'])
    spf = pd.merge_asof(spo2, fio2, on='time', by=eid, direction='backward')
    spf['fio2'] = spf['fio2'].fillna(21)
    spf['spf'] = (spf['spo2'] / (spf['fio2'] / 100) - 64) / 0.84
    spf = spf.drop(columns=['spo2', 'fio2'])
    spf = spf[spf['spf'] > 0]

    df_data = encounters[[eid, start_time_col]].rename(columns={start_time_col: 'time'})
    df_data = pd.concat([df_data, encounters[[eid, end_time_col]].rename(columns={end_time_col: 'time'})], ignore_index=True)
    df_data.time = pd.to_datetime(df_data.time.values)
    df_data = df_data.merge(gcs_score, on=[eid, 'time'], how='outer')
    df_data = df_data.merge(bp_map, on=[eid, 'time'], how='outer')
    df_data = df_data.merge(bilirubin, on=[eid, 'time'], how='outer')
    df_data = df_data.merge(platelets, on=[eid, 'time'], how='outer')
    df_data = df_data.merge(creatinine, on=[eid, 'time'], how='outer')
    df_data = df_data.merge(pf, on=[eid, 'time'], how='outer')
    df_data = df_data.merge(spf, on=[eid, 'time'], how='outer')
    for x in pressor_groups:
        df_temp = meds[meds['pressor_name'] == x].drop(columns=['pressor_name'])
        df_temp = df_temp.rename(columns={'taken_datetime': 'time', 'total_dose_character': x})
        df_data = df_data.merge(df_temp, on=[eid, 'time'], how='outer')
    mv['mv'] = 1
    df_data = df_data.merge(mv.rename(columns={'respiratory_datetime': 'time'}), on=[eid, 'time'], how='outer')

    agg = {'glasgow_coma_adult_score': 'min',
           'map_value': 'min',
           'bilirubin': 'max',
           'platelets': 'min',
           'creatinine': 'max',
           'pf': 'min',
           'spf': 'min',
           'dopamine': 'max',
           'dobutamine': 'max',
           'norepinephrine': 'max',
           'epinephrine': 'max',
           'vasopressin': 'max',
           'phenylephrine': 'max',
           'mv': 'max'}
    df_data_hour = df_data.set_index([eid, 'time'])
    df_data_hour = df_data_hour.groupby(level=0).resample(rule='1h', level=1, origin='start').agg(agg)
    df_worst = df_data_hour.groupby(level=0, as_index=False).rolling(int(lookback_window), min_periods=1).agg(agg)
    del df_data_hour
    df_worst = df_worst.reset_index(level=0, drop=True)
    df_worst = df_worst.reset_index()

    # for map, gcs, creatinine, bilirubin, platelets, creatinine, if desired hour is missing, we use feed forward filling
    df_worst['stay_id'] = 1
    df_worst['stay_id'] = df_worst.groupby(eid, as_index=False)['stay_id'].cumsum()
    df_worst['stay_id'] = ((df_worst['stay_id'] - 1) / int(frequency)).astype(int)
    df_worst['next_time'] = df_worst.groupby(eid)['time'].shift(-1)
    df_worst = df_worst.merge(encounters[[eid, end_time_col]], on=eid, how='left')
    df_worst.loc[df_worst['next_time'].isnull(), 'next_time'] = df_worst.loc[df_worst['next_time'].isnull(), end_time_col]

    df_sofa_data = df_worst.drop_duplicates([eid, 'stay_id'], keep='last')
    del df_worst
    df_sofa_data = df_sofa_data.drop(columns=['time', end_time_col]).rename(columns={'next_time': 'time'})
    df_sofa_data = df_sofa_data.drop_duplicates([eid, 'time'])
    if ff_limit > lookback_window:
        for name, tempdf in zip(['glasgow_coma_adult_score', 'map_value',
                                 'bilirubin', 'platelets', 'creatinine', 'pf', 'spf'], [gcs_score, bp_map, bilirubin, platelets, creatinine, pf, spf]):
            df_sofa_data = df_sofa_data.sort_values(['time'])
            con = df_sofa_data[name].isnull()
            if sum(con) == 0:
                continue
            tempdf = tempdf.sort_values(['time'])
            tdf = tempdf.rename(columns={name: name + '_2', 'time': 'time_2'})
            df_sofa_data = pd.merge_asof(df_sofa_data, tdf, left_on='time', right_on='time_2', by=eid, direction='backward')
            con = (df_sofa_data[name].isnull()) & (df_sofa_data[name + '_2'].notnull()) & (df_sofa_data['time'] - df_sofa_data['time_2'] <= timedelta(hours=ff_limit))
            df_sofa_data.loc[con, name] = df_sofa_data.loc[con, name + '_2']
            df_sofa_data = df_sofa_data.drop(columns=[name + '_2', 'time_2'])

    # Calculate SOFA scores
    df_sofa_data['cardio'] = np.nan
    df_sofa_data.loc[(pd.isnull(df_sofa_data['cardio']))
                     & ((df_sofa_data['dopamine'] > 15) | (df_sofa_data['epinephrine'] > 0.1)
                        | (df_sofa_data['norepinephrine'] > 0.1)), 'cardio'] = 4

    df_sofa_data.loc[(pd.isnull(df_sofa_data['cardio'])) & ((df_sofa_data['dopamine'] > 5)
                                                            | (df_sofa_data['epinephrine'] <= 0.1)
                                                            | (pd.notnull(df_sofa_data['phenylephrine']))
                                                            | (pd.notnull(df_sofa_data['vasopressin']))
                                                            | (df_sofa_data['norepinephrine'] <= 0.1)),
                     'cardio'] = 3

    df_sofa_data.loc[(pd.isnull(df_sofa_data['cardio'])) & ((df_sofa_data['dopamine'] <= 5)
                                                            | (pd.notnull(df_sofa_data['dobutamine']))),
                     'cardio'] = 2

    df_sofa_data.loc[(pd.isnull(df_sofa_data['cardio'])) & ((df_sofa_data['map_value'] < 70)),
                     'cardio'] = 1
    df_sofa_data['cardio'] = df_sofa_data['cardio'].fillna(0)

    df_sofa_data['resp'] = np.nan
    df_sofa_data.loc[(pd.isnull(df_sofa_data['resp'])) & ((df_sofa_data['mv'] > 0)
                                                          & (df_sofa_data['pf'] < 100)), 'resp'] = 4
    df_sofa_data.loc[(pd.isnull(df_sofa_data['resp'])) & ((df_sofa_data['mv'] > 0)
                                                          & (df_sofa_data['pf'] < 200)), 'resp'] = 3
    df_sofa_data.loc[(pd.isnull(df_sofa_data['resp'])) & (df_sofa_data['pf'] < 300), 'resp'] = 2
    df_sofa_data.loc[(pd.isnull(df_sofa_data['resp'])) & (df_sofa_data['pf'] < 400), 'resp'] = 1
    df_sofa_data.loc[(pd.isnull(df_sofa_data['resp'])) & (df_sofa_data['pf'] >= 400), 'resp'] = 0

    df_sofa_data.loc[(pd.isnull(df_sofa_data['resp'])) & ((df_sofa_data['mv'] > 0)
                                                          & (df_sofa_data['spf'] < 100)), 'resp'] = 4
    df_sofa_data.loc[(pd.isnull(df_sofa_data['resp'])) & ((df_sofa_data['mv'] > 0)
                                                          & (df_sofa_data['spf'] < 200)), 'resp'] = 3
    df_sofa_data.loc[(pd.isnull(df_sofa_data['resp'])) & (df_sofa_data['spf'] < 300), 'resp'] = 2
    df_sofa_data.loc[(pd.isnull(df_sofa_data['resp'])) & (df_sofa_data['spf'] < 400), 'resp'] = 1
    df_sofa_data['resp'] = df_sofa_data['resp'].fillna(0)

    df_sofa_data['coag'] = np.nan
    df_sofa_data.loc[(pd.isnull(df_sofa_data['coag'])) & (df_sofa_data['platelets'] < 20), 'coag'] = 4
    df_sofa_data.loc[(pd.isnull(df_sofa_data['coag'])) & (df_sofa_data['platelets'] < 50), 'coag'] = 3
    df_sofa_data.loc[(pd.isnull(df_sofa_data['coag'])) & (df_sofa_data['platelets'] < 100), 'coag'] = 2
    df_sofa_data.loc[(pd.isnull(df_sofa_data['coag'])) & (df_sofa_data['platelets'] < 150), 'coag'] = 1
    df_sofa_data['coag'] = df_sofa_data['coag'].fillna(0)

    df_sofa_data['liver'] = np.nan
    df_sofa_data.loc[(pd.isnull(df_sofa_data['liver'])) & (df_sofa_data['bilirubin'] > 12), 'liver'] = 4
    df_sofa_data.loc[(pd.isnull(df_sofa_data['liver'])) & (df_sofa_data['bilirubin'] >= 6), 'liver'] = 3
    df_sofa_data.loc[(pd.isnull(df_sofa_data['liver'])) & (df_sofa_data['bilirubin'] >= 2), 'liver'] = 2
    df_sofa_data.loc[(pd.isnull(df_sofa_data['liver'])) & (df_sofa_data['bilirubin'] >= 1.2), 'liver'] = 1
    df_sofa_data['liver'] = df_sofa_data['liver'].fillna(0)

    df_sofa_data['cns'] = np.nan
    df_sofa_data.loc[(pd.isnull(df_sofa_data['cns'])) & (df_sofa_data['glasgow_coma_adult_score'] < 6), 'cns'] = 4
    df_sofa_data.loc[(pd.isnull(df_sofa_data['cns'])) & (df_sofa_data['glasgow_coma_adult_score'] <= 9), 'cns'] = 3
    df_sofa_data.loc[(pd.isnull(df_sofa_data['cns'])) & (df_sofa_data['glasgow_coma_adult_score'] <= 12), 'cns'] = 2
    df_sofa_data.loc[(pd.isnull(df_sofa_data['cns'])) & (df_sofa_data['glasgow_coma_adult_score'] <= 14), 'cns'] = 1
    df_sofa_data['cns'] = df_sofa_data['cns'].fillna(0)

    df_sofa_data['renal'] = np.nan
    df_sofa_data.loc[(pd.isnull(df_sofa_data['renal'])) & (df_sofa_data['creatinine'] > 5), 'renal'] = 4
    df_sofa_data.loc[(pd.isnull(df_sofa_data['renal'])) & (df_sofa_data['creatinine'] >= 3.5), 'renal'] = 3
    df_sofa_data.loc[(pd.isnull(df_sofa_data['renal'])) & (df_sofa_data['creatinine'] >= 2), 'renal'] = 2
    df_sofa_data.loc[(pd.isnull(df_sofa_data['renal'])) & (df_sofa_data['creatinine'] >= 1.2), 'renal'] = 1
    df_sofa_data['renal'] = df_sofa_data['renal'].fillna(0)
    df_sofa_data['sofa_score'] = df_sofa_data['cardio'] + df_sofa_data['resp'] + df_sofa_data['coag'] + df_sofa_data['liver'] + df_sofa_data['cns'] + df_sofa_data['renal']

    return df_sofa_data


if __name__ == '__main__':
    pass
