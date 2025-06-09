# -*- coding: utf-8 -*-
"""
Created on Sun Dec  6 20:48:28 2020

@author: renyuanfang
"""

def mdrd_fun(age, sex, race, race_correction: bool):
    '''
    This Function applies MDRD GFR formula to return MDRD GFR value for different patients based on their age, sex, and race.

    1. Set GFR value to 75.
    2. If sex is "MALE", set sex_coeff = 1. Else set sex_coeff = 0.742.
    3. If race is "BLACK" or 'African-American', set race_coeff = 1.21. Else set race_coeff = 1.
    3. Apply MDRD GFR formula and calculate MDRD GFR value. Formula shown as below:
       mdrd = ((sex_coeff * race_coeff * 186 * float(age) ** (-0.203)) / gfr) ** (1/1.154)

    Parameters
    ----------
        age: float
            age of the individual
        sex: str
            gender of the individual
        race: str
            race of the indidividual
        race_correction: bool
            ADD DESC, True

    Returns
    -------
    float
        Caluculated mdrd

    Notes
    -----
    '''
    gfr = 75
    if sex == "MALE":
        sex_coeff = 1
    else:
        sex_coeff = 0.742
    if race in ['African-American', "BLACK"]:
        race_coeff = 1
        if race_correction:
            race_coeff = 1.21
    else:
        race_coeff = 1
    output = ((sex_coeff * race_coeff * 186 * float(age) ** (-0.203)) / gfr) ** (1 / 1.154)

    return output


def eGFR_fun(age, sex, race, row_creatinine, race_correction: bool):
    '''
    This Function applies eGFR formula to return eGFR value for different patients based on their age, sex, race, and creatinine value (Lab_result).

    1. If sex is "MALE", set k = 0.9, alpha = -0.411, and sex_coeff = 1. Else set k = 0.7, alpha = -0.329, and sex_coeff = 1.018.
    2. If race is "BLACK", race_coeff = 1.159. Else set race_coeff = 1.
    3. Apply eGFR formula and calculate eGFR value. Formula shown as below:
       eGFR = (141 * min(row_creatinine/k, 1) ** alpha * max(row_creatinine/k, 1) ** (-1.209) * 0.993 ** age * sex_coeff * race_coeff)

    Parameters
    ----------
        age: int
            age of the individual
        sex: str
            gender of the individual
        race: str
            race of the indidividual
        row_creatinine: float
            Creatinine lab result
        race_correction: bool
            ADD DESC, True

    Returns
    -------
    float
        Caluculated eGFR
    '''
    if sex == "MALE":
        k = 0.9
        alpha = -0.411
        sex_coeff = 1
    else:
        k = 0.7
        alpha = -0.329
        sex_coeff = 1.018
    if race in ['African-American', "BLACK"]:
        race_coeff = 1
        if race_correction:
            race_coeff = 1.159
    else:
        race_coeff = 1

    output = 141 * min(row_creatinine / k, 1) ** alpha * max(row_creatinine / k, 1) ** (-1.209) * 0.993 ** age * sex_coeff * race_coeff
    return output


def KeGFR_fun(base_cr, base_eGFR, prev_cr, cur_cr, delta_time_hrs):
    '''
    This function returns the KeGFR value for the input parameters.

    Formula: KeGFR = base_cr * base_eGFR / float((prev_cr + cur_cr) / 2) * (1 - 24 * (cur_cr - prev_cr) / ((cur_cr_datetime - last_kegfr_datetime) * 1.5)) where max_daily_delta_cr = 1.5

    Parameters
    ----------
        base_cr: float
            reference creatinine
        base_eGFR: float
            eGFR based on reference creatinine
        prev_cr: float
            previous creatinine value
        cur_cr: float
            current creatinine value
        delta_time_hrs: float
            duration between current creatinine time and last kegfr calculation time

    Returns
    -------
    float
        keGFR

    Notes
    -----
    '''
    max_daily_delta_cr = 1.5
    KeGFR = base_cr * base_eGFR / float((prev_cr + cur_cr) / 2) * \
            (1 - 24 * (cur_cr - prev_cr) / (delta_time_hrs * max_daily_delta_cr))
    return KeGFR
