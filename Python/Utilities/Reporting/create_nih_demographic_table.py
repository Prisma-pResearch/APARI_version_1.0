# -*- coding: utf-8 -*-
"""
Legacy Module for formatting IDR Data into NIH format.

Created on Wed Oct 30 10:53:57 2019.

@author: ruppert20
"""
import pandas as pd
# import re
# import numpy as np
# from ..Database.connect_to_database import get_SQL_database_connection as get_conn
from ..PreProcessing.standardization_functions import omop_nih_race_ethncity, idr_nih_race_ethncity




def create_NIH_demographics_table(df: pd.DataFrame, race_col: str = 'race_concept_id', ethnicity_col: str = 'ethnicity_concept_id', sex_col: str = 'gender_concept_id', patient_id_col: str = 'person_id'):
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
    df = df.copy(deep=True).groupby(patient_id_col, group_keys=False).agg('first').reset_index()

    # format the categorical variables to match expected output
    df = (omop_nih_race_ethncity if 'concept_id' in race_col else idr_nih_race_ethncity)(df=df, race_col=race_col, ethnicity_col=ethnicity_col, sex_col=sex_col)
    # df = (omop_nih_race_ethncity)(df=df.copy(deep=True), race_col=race_col, ethnicity_col=ethnicity_col, sex_col=sex_col)

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

