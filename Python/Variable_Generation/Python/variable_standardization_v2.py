# -*- coding: utf-8 -*-
"""Standardizing and generate demographic variables."""
import pandas as pd
import random
from typing import Dict, List
from .Utilities.PreProcessing.data_format_and_manipulation import check_format_series
from .Utilities.Logging.log_messages import log_print_email_message as logm
from .preprocess_and_transform import harmonize_categories_v2


def standardize_variables(encounter_data_frame: pd.DataFrame,
                          lookup_table_df: pd.DataFrame,
                          mode: str,
                          cols: list = ['gender_concept_id', 'race_concept_id', 'language', 'ethnicity_concept_id', 'marital_status', 'smoking_status', 'bmi', 'age', 'payer_concept_id', 'admitted_from_concept_id',
                                        'admitting_service', 'anesthesia_type', 'admit_priority', 'procedure_urgency', 'surgical_service', 'sched_post_op_location', 'scheduled_surgical_service'],
                          config_dict: Dict[str, str] = {'gender_concept_id': {'default': 'xxxrandomxxx', 'other': 'xxxrandomxxx'},
                                                         'race_concept_id': {'default': None, 'other': None},
                                                         'language': {'default': 'NON-ENGLISH', 'other': 'NON-ENGLISH'},
                                                         'ethnicity_concept_id': {'default': None, 'other': None},
                                                         'marital_status': {'default': None, 'other': None},
                                                         'smoking_status': {'default': None, 'other': None},
                                                         'payer_concept_id': {'default': 'MISSING', 'other': 'OTHER'},
                                                         'admitted_from_concept_id': {'default': 'NON-TRANSFER', 'other': 'NON-TRANSFER'},
                                                         'admitting_service': {'default': None, 'other': None},
                                                         'anesthesia_type': {'default': 'GENERAL', 'other': 'LOCAL/REGIONAL'},
                                                         'admit_priority': {'default': 'NON-EMERGENCY', 'other': 'NON-EMERGENCY'},
                                                         'procedure_urgency': {'default': 'NON-EMERGENCY', 'other': 'NON-EMERGENCY'},
                                                         'surgical_service': {'default': 'OTHER', 'other': 'OTHER'},
                                                         'sched_post_op_location': {'default': 'NON-ICU', 'other': 'NON-ICU'},
                                                         'scheduled_surgical_service': {'default': 'OTHER', 'other': 'OTHER'}},
                          **logging_kwargs):
    """
    Generate demographic variables.

    Parameters
    ----------
        encounter_data_frame : pandas.DataFrame
            Input DataFrame to have transformation of demographic variables can contain following columns:
                * sex
                * race
                * ethnicity
                * language
                * marital_status
                * smoking_status
                * admit_date
                * birth_date
                * height_cm or height_in
                * weight_kgs or weight_lbs
        source_type: str
            options are "UF", "PCORI", "OMOP"

    Returns
    -------
    padnas.DataFrame
        encounter_data_frame_copy: The transformed dataframe. The original dataframe is not changed.

    Notes
    -----
    * gender - If missing, is randomly assigned ["MALE" or "FEMALE"]
    * race -  Replaced by one of the following groups ["OTHER", "AA", "WHITE"].
              If missing is marked as "MISSING".
    * ethnicity - Replaced by one of the following groups ["HISPANIC", "NON-HISPANIC"].
                  If missing is marked as "MISSING".
    * language - Replaced by one of following groups ["ENGLISH", "NON-ENGLISH"].
                 If missing is marked as "NON-ENGLISH".
    * marital status - Replaced by one of following groups ["SINGLE", "DIVORCED", "MARRIED"].
                       If missing is marked as "MISSING".
    * smoking_status - Replace by one of following groups ["NEVER", "FORMER", "CURRENT"].
                       If missing is marked as "MISSING".
    * age - Is generated if the `admit_date` and `birth_date` field is present in the dataframe.
            Is calculated as the difference between those dates
    * bmi - Is generated if the `height_cm` and `weight_kg` fields are present in the dataframe.
            Is calculated as by using the formula for BMI. weight / ((height ^ 2) * 10000)
    """
    assert (mode in ['surgery', 'icu', 'admission']) or pd.isnull(mode), f'Unrecognized mode: {mode}, mode must be in ["icu", "surgery", None]'

    for var in cols:
        logm(message=f'Generating: {var}', **logging_kwargs)
        if var in ['age', 'bmi']:
            if var in encounter_data_frame.columns:
                if encounter_data_frame[var].notnull().all():
                    continue
            encounter_data_frame = _generate_age(df=encounter_data_frame) if var == 'age' else _generate_bmi(df=encounter_data_frame)
        else:
            encounter_data_frame.loc[:,
                                     lookup_table_df.loc[lookup_table_df.variable_name == var.replace('procedure_urgency', 'admit_priority').replace('scheduled_surgical_service', 'surgical_service'),
                                                         'var_gen_name'].iloc[0] + ('_procedure' if (var == 'procedure_urgency') else '_scheduled' if (var == 'scheduled_surgical_service') else '')] = harmonize_categories_v2(column=check_format_series(encounter_data_frame[var] if var in encounter_data_frame.columns else pd.Series(index=encounter_data_frame.index, data=None), desired_type='sparse_int'),
                                                                                                                                                                                                                                replacement_dict=lookup_table_df.loc[lookup_table_df.variable_name == var.replace('procedure_urgency', 'admit_priority'), ['concept_id', 'var_gen_value']].drop_duplicates().set_index('concept_id').var_gen_value.to_dict(),
                                                                                                                                                                                                                                default_value=_random_choice if config_dict.get(var).get('default') == 'xxxrandomxxx' else config_dict.get(var).get('default'),
                                                                                                                                                                                                                                other_value=_random_choice if config_dict.get(var).get('other') == 'xxxrandomxxx' else config_dict.get(var).get('other'),
                                                                                                                                                                                                                                options=lookup_table_df.loc[lookup_table_df.variable_name == var.replace('procedure_urgency', 'admit_priority').replace('scheduled_surgical_service', 'surgical_service'), 'var_gen_value'].drop_duplicates().tolist())

    if mode in ['icu', 'surgery']:
        encounter_data_frame = _generate_time_to_event(df=encounter_data_frame, visit_detail_start_col=f'{mode}_start_datetime', visit_start_col='visit_start_datetime')

        if 'sched_start_datetime' in encounter_data_frame.columns:
            encounter_data_frame = _generate_time_to_event(df=encounter_data_frame, visit_detail_start_col='sched_start_datetime', visit_start_col='visit_start_datetime')

    encounter_data_frame = _generate_visit_start_derived_vars(df=encounter_data_frame, visit_start_col='visit_start_datetime')

    return encounter_data_frame


def _generate_visit_start_derived_vars(df: pd.DataFrame, visit_start_col: str) -> pd.DataFrame:
    """Calculate dateime derived vars from admisstion datetime."""
    # verify required columns exist in the correct format
    assert visit_start_col in df.columns, f'Required Columns: visit_start_col: {visit_start_col} is missing from the dataframe'
    df.loc[:, visit_start_col] = check_format_series(ds=df[visit_start_col], desired_type='datetime').fillna(pd.Timestamp.today())

    # generate derived vars
    df.loc[:, 'admit_day'] = df[visit_start_col].dt.strftime('%A')
    df.loc[:, 'admit_month'] = df[visit_start_col].dt.strftime('%b')
    df.loc[:, 'admit_year'] = df[visit_start_col].dt.year
    df.loc[:, 'admit_hour'] = df[visit_start_col].dt.hour
    df.loc[:, 'night_admission'] = ((df.admit_hour < 7) | (df.admit_hour > 18)).astype(int)

    return df


def _generate_time_to_event(df: pd.DataFrame, visit_detail_start_col: str, visit_start_col: str) -> pd.DataFrame:
    """Calculate dateime derived vars from admisstion datetime."""
    # verify required columns exist in the correct format
    assert visit_start_col in df.columns, f'Required Columns: visit_start_col: {visit_start_col} is missing from the dataframe'
    assert visit_detail_start_col in df.columns, f'Required Columns: visit_detail_start_col: {visit_detail_start_col} is missing from the dataframe'
    df.loc[:, visit_start_col] = check_format_series(ds=df[visit_start_col], desired_type='datetime').fillna(pd.Timestamp.today())
    df.loc[:, visit_detail_start_col] = check_format_series(ds=df[visit_detail_start_col], desired_type='datetime')

    # calculate hours between surgery and
    df.loc[:, 'sched_time_to_surgery' if 'sched' in visit_detail_start_col else f"time_to_{visit_detail_start_col.replace('_start_datetime', '')}"] = ((df[visit_detail_start_col] - df[visit_start_col]).dt.total_seconds() / 3600).clip(lower=0)

    return df


def _random_choice(options: List[any]) -> any:
    return random.choice(options)


def _generate_age(df: pd.DataFrame, visit_start_col: str = 'visit_start_datetime'):
    """Calculate age from admit and birth date."""
    # verify required columns exist in the correct format
    assert visit_start_col in df.columns, f'Required column: visit_start_col: {visit_start_col} not present in dataframe'
    assert 'birth_date' in df.columns, 'Required column: birth_date not present in dataframe'

    # impute todays date for visit_start_col if missing
    df.loc[:, visit_start_col] = check_format_series(ds=df[visit_start_col], desired_type='datetime').fillna(pd.Timestamp.today())
    df.loc[:, 'birth_date'] = check_format_series(ds=df.birth_date, desired_type='datetime')

    # calculate age
    df.loc[:, 'age'] = (df[visit_start_col] - df['birth_date']).astype('timedelta64[Y]').astype(str).fillna('MISSING')

    return df


def _generate_bmi(df: pd.DataFrame):
    """Calculate bmi from height and weight."""
    # verify required columns exist in the correct untis
    if 'height_in' in df.columns:
        df.loc[:, 'height_cm'] = 2.54 * check_format_series(ds=df.height_in, desired_type='float')
        df.drop(columns=['height_in'], inplace=True)
    if 'weight_lbs' in df.columns:
        df.loc[:, 'weight_kg'] = 0.45359237 * check_format_series(ds=df.weight_lbs, desired_type='float')
        df.drop(columns=['weight_lbs'], inplace=True)

    assert 'height_cm' in df.columns, 'Required column: height_cm not present in dataframe'
    assert 'weight_kg' in df.columns, 'Required column: weight_kgs not present in dataframe'

    # perform calculation
    calculable_bmi_mask: pd.Series = df.height_cm.notnull() & df.weight_kg.notnull()
    df.loc[calculable_bmi_mask, 'bmi'] = (check_format_series(ds=df.weight_kg, desired_type='float')
                                          / (check_format_series(ds=df.height_cm, desired_type='float') ** 2)
                                          * 10000)
    df.loc[~calculable_bmi_mask, 'bmi'] = 'MISSING'

    return df
