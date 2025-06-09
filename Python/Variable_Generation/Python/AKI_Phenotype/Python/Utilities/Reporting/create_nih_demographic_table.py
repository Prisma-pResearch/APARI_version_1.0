# -*- coding: utf-8 -*-
"""
Legacy Module for formatting IDR Data into NIH format.

Created on Wed Oct 30 10:53:57 2019.

@author: ruppert20
"""
import pandas as pd
import re
import numpy as np
from ..Database.connect_to_database import get_SQL_database_connection as get_conn


def match_NIH_demographics_format(df: pd.DataFrame, race_col: str = 'race', ethnicity_col: str = 'ethnicity', sex_col: str = 'sex') -> pd.DataFrame:
    """
    Format Dataframe values into NIH standard.

    Parameters
    ----------
    df : pd.DataFrame
        DESCRIPTION.
    race_col : str, optional
        DESCRIPTION. The default is 'race'.
    ethnicity_col : str, optional
        DESCRIPTION. The default is 'ethnicity'.
    sex_col : str, optional
        DESCRIPTION. The default is 'sex'.

    Returns
    -------
    df : TYPE
        DESCRIPTION.

    """
    df.loc[((df[ethnicity_col].astype(str).str.contains('HISPANIC', flags=re.IGNORECASE, na=False)
             & ~df[ethnicity_col].astype(str).str.contains('NOT', flags=re.IGNORECASE, na=False))
            | df[race_col].astype(str).str.contains('HISPANIC', flags=re.IGNORECASE, na=False)), ethnicity_col] = 'Hispanic or Latino'

    df.loc[df[race_col].isin(['AMERICAN INDIAN']), race_col] = 'American Indian/Alaska Native'

    df.loc[df[race_col].isin(['ASIAN']), race_col] = 'Asian'

    df.loc[df[race_col].isin(['PACIFIC ISLANDER']), race_col] = 'Native Hawaiian or Other Pacific Islander'

    df.loc[df[race_col].isin(['BLACK', 'BLACK HISPANIC']), race_col] = 'Black or African American'

    df.loc[(df[race_col].isin(['WHITE HISPANIC', 'WHITE', 'HISPANIC'])), race_col] = 'White'

    df.loc[df[race_col].isin(['MULTIRACIAL']), race_col] = 'More Than One Race'

    df.loc[df[race_col].isin(['OTHER', 'PATIENT REFUSED', 'UNKNOWN', '?', '??', None, '', np.nan]) | df[race_col].isnull(), race_col] = 'Unknnown or Not Reported'

    df.loc[df[ethnicity_col].isin(['NOT HISPANIC']), ethnicity_col] = 'Not Hispanic or Latino'

    df.loc[df[ethnicity_col].isin(['PATIENT REFUSED', 'UNKNOWN', '?', '??', None, '', np.nan]) | df[ethnicity_col].isnull(), ethnicity_col] = 'Unknown/Not Reported Ethnicity'

    df.loc[df[sex_col].isin(['FEMALE']), sex_col] = 'Female'

    df.loc[df[sex_col].isin(['MALE']), sex_col] = 'Male'

    df.loc[~df[sex_col].isin(['Male', 'Female']), sex_col] = 'Unknown/Not Reported'

    return df


def create_NIH_demographics_table(df: pd.DataFrame, race_col: str = 'race', ethnicity_col: str = 'ethnicity', sex_col: str = 'sex', patient_id_col: str = 'patient_deiden_id'):
    """
    Create NIH demographics table.

    Parameters
    ----------
    df : pd.DataFrame
        DESCRIPTION.
    race_col : str, optional
        DESCRIPTION. The default is 'race'.
    ethnicity_col : str, optional
        DESCRIPTION. The default is 'ethnicity'.
    sex_col : str, optional
        DESCRIPTION. The default is 'sex'.
    patient_id_col : str, optional
        DESCRIPTION. The default is 'patient_deiden_id'.

    Returns
    -------
    TYPE
        DESCRIPTION.

    """
    # make template column index
    out = pd.DataFrame({'Racial Categories': ['Ethnic Category', 'Sex', 'American Indian/Alaska Native', 'Asian',
                                              'Native Hawaiian or Other Pacific Islander', 'Black or African American',
                                              'White', 'More Than One Race', 'Unknnown or Not Reported']}).set_index('Racial Categories')

    # drop duplicate patients
    df = df.groupby(patient_id_col, group_keys=False).agg('first').reset_index()

    # format the categorical variables to match expected output
    df = match_NIH_demographics_format(df=df, race_col=race_col, ethnicity_col=ethnicity_col, sex_col=sex_col)

    # set list of ethnic levels to check
    ethnic_categories: list = ['Not Hispanic or Latino', 'Hispanic or Latino', 'Unknown/Not Reported Ethnicity']

    # set list of sex levels to check
    sex_categories: list = ['Female', 'Male', 'Unknown/Not Reported']

    # fill in columns
    for ethnic_category in ethnic_categories:
        for sex in sex_categories:

            # append the header values
            temp = pd.Series({'Ethnic Category': ethnic_category, 'Sex': sex})

            # fill in the body of the series
            temp = temp.append(df[(df[sex_col] == sex) & (df[ethnicity_col] == ethnic_category)][race_col].value_counts())

            # insert into the dataframe
            out.insert(loc=out.shape[1], column='{}_{}'.format(ethnic_category, sex), value=temp)

    # fill blanks with zero
    out.fillna(0, inplace=True)

    # add totals
    out.loc[['American Indian/Alaska Native',
             'Asian', 'Native Hawaiian or Other Pacific Islander',
             'Black or African American', 'White',
             'More Than One Race', 'Unknnown or Not Reported'], 'total'] = out.loc[['American Indian/Alaska Native',
                                                                                    'Asian', 'Native Hawaiian or Other Pacific Islander',
                                                                                    'Black or African American', 'White',
                                                                                    'More Than One Race', 'Unknnown or Not Reported'], :].apply(sum, axis=1)

    return pd.concat([out, pd.DataFrame({'total': out.loc[['American Indian/Alaska Native',
                                                           'Asian', 'Native Hawaiian or Other Pacific Islander',
                                                           'Black or African American', 'White',
                                                           'More Than One Race', 'Unknnown or Not Reported'], :].apply(sum)}).transpose()],
                     axis=0, sort=False)


def create_nih_demographics_table_from_db(private_key_dir: str,
                                          encrypted_dict_file_path: str,
                                          out_path: str,
                                          encounter_table: str = 'encounters',
                                          database_name: str = 'IdealistClean',
                                          patient_eligibilty_criteria: str = "patient_deiden_id IN (select distinct patient_deiden_id from or_case_schedule_encounters WHERE surgery_start_datetime BETWEEN '2016-01-01' AND '2020-08-01' AND hospital='UF' AND patient_type IN ('INPATIENT', 'OBSERVATION')) AND admit_datetime BETWEEN '2016-01-01' AND '2020-08-01'"):
    """
    Create NIH demographics table from SQL database.

    Parameters
    ----------
    private_key_dir : str
        DESCRIPTION.
    encrypted_dict_file_path : str
        DESCRIPTION.
    out_path : str
        DESCRIPTION.
    database_name : str, optional
        DESCRIPTION. The default is 'IdealistClean'.
    patient_eligibilty_criteria : str, optional
        DESCRIPTION. The default is "patient_deiden_id IN (select distinct patient_deiden_id from or_case_schedule_encounters WHERE surgery_start_datetime BETWEEN '2016-01-01' AND '2020-08-01' AND hospital='UF') AND admit_datetime BETWEEN '2016-01-01' AND '2020-08-01'".

    Returns
    -------
    TYPE
        DESCRIPTION.

    """
    engine = get_conn(encrypted_dict_file_path=encrypted_dict_file_path,
                      private_key_dir=private_key_dir,
                      database=database_name,
                      upload=False)

    query = f"""SELECT
                patient_deiden_id,
                sex,
                ethnicity,
                race
                FROM {encounter_table}
                WHERE
                {patient_eligibilty_criteria};"""

    create_NIH_demographics_table(df=pd.read_sql(query, engine)).to_excel(out_path, index=True)
