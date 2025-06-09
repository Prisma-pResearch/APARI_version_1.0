# -*- coding: utf-8 -*-
"""
Module of Pandas Aggregation Functions.

Created on Wed Feb  9 08:44:58 2022

@author: ruppert20
"""
import pandas as pd
from scipy.stats import median_abs_deviation as mad
from typing import Union
from pandas.core.resample import DatetimeIndexResampler as dtir
import re


def nan_tolerant_min(series: pd.Series) -> any:
    """
    NULL tolerant implementation of pandas.Series.min.

    Parameters
    ----------
    series : pd.Series
        input series to aggregate.

    Returns
    -------
    any
        return minimum value contained input series.

    """
    return series.dropna().min()


def _numeric_aggregators(input_v) -> pd.Series:
    ct: pd.Series = input_v.count()
    return pd.DataFrame({f'{ct.name}_sum': input_v.sum(),
                         f'{ct.name}_min': input_v.min(),
                         f'{ct.name}_max': input_v.max(),
                         f'{ct.name}_count': ct,
                         f'{ct.name}_mean': input_v.mean(),
                         f'{ct.name}_median': input_v.median(),
                         f'{ct.name}_std': input_v.std(),
                         f'{ct.name}_mad': input_v.apply(mad, nan_policy='omit')})


def _mean_only(input_v) -> float:
    return input_v.mean()


def _sum_indicator(input_v) -> pd.Series:
    ct: pd.Series = input_v.sum()
    return pd.DataFrame({f'{ct.name}_sum': ct,
                         f'{ct.name}_ind': (ct > 0).astype(int)})


def _max(input_v) -> float:
    return input_v.max(skipna=True)


def _default_non_numeric_agg(input_v) -> pd.Series:
    ct: pd.Series = input_v.count()
    return pd.DataFrame({f'{ct.name}_count': ct})


def _worst_station_agg(series: pd.Series) -> str:
    if series.str.contains(r'HOME', case=False).any():
        return 'HOME'
    elif series.str.contains(r'OR', case=False).any():
        return 'OR'
    elif series.str.contains(r'ICU', case=False).any():
        return 'ICU'
    else:
        return 'WARD'


def _icu_agg(series: pd.Series) -> str:
    if series.str.contains(r'ICU', case=False).any():
        return 1
    elif series.dropna().shape[0] == 0:
        return None
    else:
        return 0


def _ICU_or_OR_agg(series: pd.Series) -> str:
    if series.str.contains(r'ICU|^OR$', case=False).any():
        return 1
    elif series.dropna().shape[0] == 0:
        return None
    else:
        return 0


def _binary_station_agg(series: Union[pd.Series, dtir]) -> Union[pd.DataFrame, dict]:
    """
    Extract binary location types from input.

    Parameters
    ----------
    series : Union[pd.Series, pd.core.resample.DatetimeIndexResampler]
        Both pandas series and pd.core.resample.DatetimeIndexResampler are accepted, and will produce the same labels.
        The only difference is the output type.

    Returns
    -------
    dict if the input is a pandas series
    pd.DataFrame if the input is a resample object


    """
    if isinstance(series, dtir):
        temp: pd.Series = series.apply(_binary_station_agg)
        out: pd.DataFrame = pd.json_normalize(temp)
        out.index = temp.index
        return out
    levels = series.dropna().drop_duplicates().str.lower().replace({'pass': None}).dropna()

    return {'ICU': None if levels.shape[0] == 0 else 1 if levels.str.contains(r'icu', regex=True, case=False).any() else 0,
            'WARD': None if levels.shape[0] == 0 else 1 if levels.str.contains(r'ward', regex=True, case=False).any() else 0,
            'HOME': None if levels.shape[0] == 0 else 1 if levels.str.contains(r'home', regex=True, case=False).any() else 0,
            'ED': None if levels.shape[0] == 0 else 1 if levels.str.contains(r'^ED$', regex=True, case=False).any() else 0,
            'OR': None if levels.shape[0] == 0 else 1 if levels.str.contains(r'^OR$', regex=True, case=False).any() else 0,
            'PROC': None if levels.shape[0] == 0 else 1 if levels.str.contains(r'Procedure', regex=True, case=False).any() else 0,
            'IMC': None if levels.shape[0] == 0 else 1 if levels.str.contains(r'IMC', regex=True, case=False).any() else 0,
            'PACU': None if levels.shape[0] == 0 else 1 if levels.str.contains(r'PACU', regex=True, case=False).any() else 0}


def _worst_resp_device_agg(series: pd.Series) -> str:

    if series.isin(['ventilator']).any():
        return 'ventilator'
    elif series.isin(['supplemental oxygen']).any():
        return 'supplemental oxygen'
    elif series.isin(['noninvasive ventilator']).any():
        return 'noninvasive ventilator'
    elif series.isin(['room air']).any():
        return 'room air'
    else:
        return None


def _worst_cam_agg(series: pd.Series) -> str:
    if series.str.contains(r'Positive|Yes', case=False).any():
        return 'POSITIVE'
    elif series.str.contains(r'Unable to Assess', case=False).any():
        return 'UNABLE_TO_ASSESS'
    elif series.dropna().shape[0] > 0:
        return 'NEGATIVE'
    else:
        return None


def _numeric_sum(series: pd.Series) -> float:
    return pd.to_numeric(series.where(pd.notnull(series), None), errors='coerce').sum()

# def _agg_instructions(instruction_dir: str) -> pd.DataFrame:
#     return check_load_df('', patterns=[r'[A-z]_instructions\.csv'], directory=instruction_dir).drop_duplicates(subset=['column_name'])


def _worst_level_of_assistance(series: pd.Series) -> str:
    if isinstance(series, tuple):
        series: pd.Series = series[1]

    levels = pd.Series(series.dropna().drop_duplicates().str.lower().str.split(';', expand=True).values.reshape(-1)).drop_duplicates().dropna().tolist()

    if 'dependent, patient does less than 25%' in levels:
        return 'Dependent'
    elif 'maximum assist, patient does 25-49%' in levels:
        return 'Maximum'
    elif 'moderate assist, patient does 50-74%' in levels:
        return 'Moderate'
    elif 'minimal assist, patient does 75% or more' in levels:
        return 'Minimal'
    elif ('assistance by 4 or more caregivers' in levels) or ('adl assistance by 4 or more caregivers' in levels):
        return 'Moderate'
    elif ('adl assistance by 2-3 caregivers' in levels) or ('assistance by 2-3 caregivers' in levels) or ('assistance with re-learning adl activities' in levels):
        return 'Minimal'
    elif 'independent' in levels:
        return 'Independent'
    else:
        return None


def _best_activity(series: pd.Series) -> str:
    levels = pd.Series(series.dropna().drop_duplicates().str.lower().str.split(';', expand=True).values.reshape(-1)).drop_duplicates().dropna()

    if levels.isin(['ambulated in hall', 'ambulate in hall']).any():
        return 'Hall'
    elif levels.isin(['ambulated to bathroom', 'bathroom privileges']).any():
        return 'Restroom'
    elif levels.isin(['ambulated in room', 'ambulate in room']).any():
        return 'Room'
    elif levels.isin(['stand at bedside', 'sitting in chair', 'transferred to chair', 'transferred bed to/from chair', 'stood at bedside',
                      'up in wheelchair', 'performed mobility by wheelchair', 'chair', 'up in chair']).any():
        return 'Chair'
    elif levels.isin(['bedrest', 'stretcher', 'repositioned/turned in bed', 'turn with assistance', 'dangle', 'up in stretcher chair',
                      'performed adl activities in bed', 'positioned in chair position in bed', 'transferred to bed', 'bed in chair position',
                      'transferred bed/chair to/from bedside commode', 'performed adl activities out of bed', 'dangled', 'sat edge of bed, feet supported on floor',
                      'in bed', 'other (comment)', 'total care sport', 'turn']).any():
        return 'Bed'
    else:
        return None


def _best_ambulation_response(series: pd.Series) -> str:
    levels = pd.Series(series.dropna().drop_duplicates().str.lower().str.split(';', expand=True).values.reshape(-1)).drop_duplicates().dropna()

    if levels.isin(['tolerated ambulation well', 'does not require skilled therapy to ambulate', 'tolerated well', 'tolerated fairly well']).any():
        return 'Well'
    elif levels.isin(['tolerated ambulation poorly (comment)', 'seated rests required', 'tolerated poorly']).any():
        return 'Poor'
    else:
        None


def _best_pressure_relief(series: pd.Series) -> str:
    return 1 if series.str.contains('yes', case=False).any() else 0 if series.str.contains('no', case=False).any() else None


def _worst_assistive_device(series: pd.Series) -> str:
    levels = pd.Series(series.dropna().drop_duplicates().str.lower().str.split(';', expand=True).values.reshape(-1)).drop_duplicates().dropna()

    if levels.isin(['patient lift', 'mechanical lift/sling', 'lift', 'overhead lift', 'sling']).any():
        return 'Lift'
    elif levels.isin(['wheelchair']).any():
        return 'Wheelchair'
    elif levels.isin(['standard walker', 'walker', 'four wheel walker', 'cane', 'front wheel walker', 'four point cane']).any():
        return 'Walker/Cane'
    elif levels.isin(['crutches']).any():
        return 'Crutches'
    elif levels.isin(['other (comment)', 'gait belt', 'other (comment)', 'stand assist device', 'hand held assitance']).any():
        return 'Other'
    elif levels.isin(['none', 'no assistive device']).any():
        'No assistive device'
    else:
        None


def _worst_positioning_frequency(series: pd.Series) -> str:

    levels = pd.Series(series.dropna().drop_duplicates().str.split(';', expand=True).values.reshape(-1)).drop_duplicates().dropna()

    if levels.isin(['every 2 hours']).any():
        return 'Every 2 hours'
    elif levels.isin(['requires assist to turn']).any():
        return 'Requires assist to turn'
    elif levels.isin(['per order (comment)']).any():
        return 'Per order (comment)'
    elif levels.isin(['able to turn self']).any():
        return 'Able to turn self'
    else:
        None


def _contributing_factors_agg(series: Union[pd.Series, dtir]) -> Union[pd.DataFrame, dict]:
    """
    Extract binary contributing factors from input.

    Parameters
    ----------
    series : Union[pd.Series, pd.core.resample.DatetimeIndexResampler]
        Both pandas series and pd.core.resample.DatetimeIndexResampler are accepted, and will produce the same labels.
        The only difference is the output type.

    Returns
    -------
    dict if the input is a pandas series
    pd.DataFrame if the input is a resample object


    """
    if isinstance(series, dtir):
        temp: pd.Series = series.apply(_contributing_factors_agg)
        out: pd.DataFrame = pd.json_normalize(temp)
        out.columns = [f'{temp.name}_{x.lower().replace(" ", "_").replace("(", "").replace(")", "")}' for x in out.columns]
        out.index = temp.index
        return out
    levels = pd.Series(series.dropna().drop_duplicates().str.split(';', expand=True).values.reshape(-1)).drop_duplicates().dropna()

    return {'Impaired Mobility': None if levels.shape[0] == 0 else 1 if levels.str.contains(r'Impaired\sMobility', regex=True, case=False).any() else 0,
            'Obesity': None if levels.shape[0] == 0 else 1 if levels.str.contains(r'Obesity', regex=True, case=False).any() else 0,
            'Risk of Dislodging LDAs': None if levels.shape[0] == 0 else 1 if levels.str.contains(r'Risk\sof\sDislodging\sLDAs', regex=True, case=False).any() else 0,
            'Unable to Follow Commands': None if levels.shape[0] == 0 else 1 if levels.str.contains(r'Unable\sto\sFollow\sCommands', regex=True, case=False).any() else 0,
            'Restraints': None if levels.shape[0] == 0 else 1 if levels.str.contains(r'Restraints', regex=True, case=False).any() else 0,
            'Other': None if levels.shape[0] == 0 else 1 if levels.str.contains(r'other', regex=True, case=False).any() else 0}


def _worst_transport_method(series: pd.Series) -> str:

    levels = pd.Series(series.dropna().drop_duplicates().str.lower().str.split(';', expand=True).values.reshape(-1)).drop_duplicates().dropna()

    if levels.isin(['stretcher', 'bed', 'stretcher chair', 'bed - icu', 'bed - unit', 'bed - specialty', 'bed_unit',
                   'stretcher bed - icu', 'stretcher - eye', 'bed_icu', 'bed bed - unit', 'bed bed - icu', 'bed - unit bed',
                    'stretcher bed - unit', 'bed_specialty', 'crib', 'wheelchair bed', 'stretcher bed', 'bed - icu stretcher',
                    'bed - unit stretcher', 'bed - icu bed', 'bed wheelchair', 'stretcher stretcher chair', 'bed bed']).any():
        return 'Bed'
    elif levels.isin(['wheelchair', 'wagon', 'infant seat', 'stroller', 'other (comment) wagon', 'being carried']).any():
        return 'Wheelchair'
    elif levels.isin(['ambulatory']).any():
        return 'Ambulatory'
    else:
        None


def _worst_transport_with(series: Union[pd.Series, dtir]) -> Union[pd.DataFrame, dict]:
    """
    Extract binary transport with reqyirements from input.

    Parameters
    ----------
    series : Union[pd.Series, pd.core.resample.DatetimeIndexResampler]
        Both pandas series and pd.core.resample.DatetimeIndexResampler are accepted, and will produce the same labels.
        The only difference is the output type.

    Returns
    -------
    dict if the input is a pandas series
    pd.DataFrame if the input is a resample object


    """
    if isinstance(series, dtir):
        temp: pd.Series = series.apply(_worst_transport_with)
        out: pd.DataFrame = pd.json_normalize(temp)
        out.columns = [f'{temp.name}_{x.lower().replace(" ", "_").replace("(", "").replace(")", "")}' for x in out.columns]
        out.index = temp.index
        return out
    levels = pd.Series(series.dropna().drop_duplicates().str.lower().str.split(';', expand=True).values.reshape(-1)).drop_duplicates().dropna()

    return {'Physician': None if levels.shape[0] == 0 else 1 if levels.str.contains(r'Doctor', regex=True, case=False).any() else 0,
            'Nurse': None if levels.shape[0] == 0 else 1 if levels.str.contains(r'Nurse', regex=True, case=False).any() else 0,
            'IV': None if levels.shape[0] == 0 else 1 if levels.str.contains(r'IV', regex=True, case=False).any() else 0,
            'Oxygen': None if levels.shape[0] == 0 else 1 if levels.str.contains(r'Oxygen', regex=True, case=False).any() else 0,
            'Vent': None if levels.shape[0] == 0 else 1 if levels.str.contains(r'Vent', regex=True, case=False).any() else 0,
            'Monitor': None if levels.shape[0] == 0 else 1 if levels.str.contains(r'Monitor', regex=True, case=False).any() else 0,
            'Accompanied': None if levels.shape[0] == 0 else 1 if levels.isin(['transport team', 'caregiver', 'parent(s)']).any() else 0,
            'Other': None if levels.shape[0] == 0 else 1 if levels.str.contains(r'other', regex=True, case=False).any() else 0}


def _best_repositioned(series: pd.Series) -> str:

    levels = pd.Series(series.dropna().drop_duplicates().str.lower().str.split(';', expand=True).values.reshape(-1)).drop_duplicates().dropna()

    if levels.isin(['turns self']).any():
        return 'Self Turn'
    if levels.isin(['standing']).any():
        return 'Standing'
    elif levels.isin(['up in chair', 'sitting', 'edge of bed with legs dangling']).any():
        return 'Sitting'
    elif levels.isin(['supine', 'lying left side', 'lying right side', 'pillow support', 'semi fowlers', 'tilt',
                      'wedge support', 'prone', 'reverse trendelenburg', 'trendelenburg']).any():
        return 'Laying'
    elif levels.isin(['other (comment)']).any():
        return 'Other'
    else:
        None


def _worst_aki_cat_agg(series: pd.Series) -> str:
    """
    Return worst AKI/AKD stage.

    Parameters
    ----------
    series : pd.Series
        pandas series of type object.

    Returns
    -------
    str
        worst aki stage as a string.

    """
    if series.str.contains(r'AKD\sStage\s3\s\+\sRRT', case=False, regex=True).any():
        return 'AKD Stage 3 + RRT'
    elif series.str.contains(r'Persisent\sAKI\sStage\s3\s\+\sRRT', case=False, regex=True).any():
        return 'Persisent AKI Stage 3 + RRT'
    elif series.str.contains(r'AKI\sStage\s3\s\+\sRRT', case=False, regex=True).any():
        return 'AKI Stage 3 + RRT'
    elif series.str.contains(r'AKD\sStage\s3', case=False, regex=True).any():
        return 'AKD Stage 3'
    elif series.str.contains(r'Persisent\sAKI\sStage\s3', case=False, regex=True).any():
        return 'Persisent AKI Stage 3'
    elif series.str.contains(r'AKI\sStage\s3', case=False, regex=True).any():
        return 'AKI Stage 3'
    elif series.str.contains(r'AKD\sStage\s2', case=False, regex=True).any():
        return 'AKD Stage 2'
    elif series.str.contains(r'Persisent\sAKI\sStage\s2', case=False, regex=True).any():
        return 'Persisent AKI Stage 2'
    elif series.str.contains(r'AKI\sStage\s2', case=False, regex=True).any():
        return 'AKI Stage 2'
    elif series.str.contains(r'AKD\sStage\s1', case=False, regex=True).any():
        return 'AKD Stage 1'
    elif series.str.contains(r'Persisent\sAKI\sStage\s1', case=False, regex=True).any():
        return 'Persisent AKI Stage 1'
    elif series.str.contains(r'AKI\sStage\s1', case=False, regex=True).any():
        return 'AKI Stage 1'
    elif series.str.contains(r'Recovery\sfrom\sAKD', case=False, regex=True).any():
        return 'Recovery from AKD'
    elif series.str.contains(r'Recovery\sfrom\sPersisent\sAKI', case=False, regex=True).any():
        return 'Recovery from Persisent AKI'
    elif series.str.contains(r'Recovery\sfrom\sRapidly\sReversible AKI', case=False, regex=True).any():
        return 'Recovery from Rapidly Reversible AKI'
    elif series.str.contains(r'No\sAKI', case=False, regex=True).any():
        return 'No AKI'
    else:
        return None


def _worst_aki_binarization_agg(series: pd.Series) -> str:
    """
    Return worst AKI/AKD stage binarized into categories.

    Parameters
    ----------
    series : Union[pd.Series, pd.core.resample.DatetimeIndexResampler]
        Both pandas series and pd.core.resample.DatetimeIndexResampler are accepted, and will produce the same labels.
        The only difference is the output type.

    Returns
    -------
    dict if the input is a pandas series
    pd.DataFrame if the input is a resample object

    """
    if isinstance(series, dtir):
        temp: pd.Series = series.apply(_worst_aki_binarization_agg)
        out: pd.DataFrame = pd.json_normalize(temp)
        out.index = temp.index
        return out

    aki_cat: str = str(_worst_aki_cat_agg(series)).lower()

    return {'aki': 0 if 'no aki' in aki_cat else 1 if 'stage 1' in aki_cat else 2 if 'stage 2' in aki_cat else 3 if bool(re.search(r'stage\s3$', aki_cat)) else 4 if 'rrt' in aki_cat else None,
            'akd': 1 if 'akd' in aki_cat else None if aki_cat == 'none' else 0,
            'persistent': 1 if 'persisent' in aki_cat else None if aki_cat == 'none' else 0,
            'recovery': 1 if 'recovery' in aki_cat else None if aki_cat == 'none' else 0,
            'rapidly_reversible': 1 if 'rapidly reversible' in aki_cat else None if aki_cat == 'none' else 0}


def _worst_ckd(series: pd.Series) -> str:
    if series.astype(str).str.contains('ESRD', case=False, regex=True).any():
        return 'ESRD'
    elif series.astype(str).str.contains(r'^1', regex=True, case=False).any():
        return 'CKD'
    elif series.dropna().shape[0] > 0:
        return 'NO CKD'
    else:
        return None
