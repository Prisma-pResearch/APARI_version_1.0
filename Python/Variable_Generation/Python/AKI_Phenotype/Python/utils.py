# -*- coding: utf-8 -*-
"""
Created on Sun Dec  6 20:48:28 2020

@author: renyuanfang
@editor: Ruppert20 06/02/23
"""

male_aliases: list = ["MALE", 8507, '8507', 'M', 'Male', 'male']

def mdrd_fun(age, sex, race, race_correction: bool = False, version=2):
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
    if ((race_correction == False) and (version == 2)):
        gfr = 75
        if sex in male_aliases:
            sex_coeff = 1
            alpha_1 = -0.302
            alpha_2 = -1.2
            kappa = 0.9
        else:
            sex_coeff = 1.012
            alpha_1 = -0.241
            alpha_2 = -1.2
            kappa = 0.7
        output_1 = (gfr / (sex_coeff * 142 * (0.9938 ** (float(age))) * kappa**(-1.0 * alpha_1))) ** (1 / alpha_1)
        output_2 = (gfr / (sex_coeff * 142 * (0.9938 ** (float(age))) * kappa**(-1.0 * alpha_2))) ** (1 / alpha_2)
        if output_1 >= kappa:
            return output_2
        else:
            return output_1

    gfr = 75
    if sex in male_aliases:
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


def eGFR_fun(age, sex, race, row_creatinine, race_correction: bool, version=2):
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
    if (not race_correction) and (version == 2):
        if sex in male_aliases:
            # males --> equation w/o Cystatin C
            k = 0.9
            a1 = -0.302
            a2 = -1.200
            age_coeff = 0.9938
            sex_coeff = 1

        else:  # elif sex == "FEMALE":
            # females --> equation w/o Cystatin C
            k = 0.7
            a1 = -0.241
            a2 = -1.200
            age_coeff = 0.9938
            sex_coeff = 1.012

        output = 142 * min(row_creatinine / k, 1) ** a1 * max(row_creatinine / k, 1) ** a2 * age_coeff ** age * sex_coeff
        return output

    if sex in male_aliases:
        k = 0.9
        alpha = -0.411
        sex_coeff = 1
    else:
        k = 0.7
        alpha = -0.329
        sex_coeff = 1.018
    if race in ['African-American', "BLACK", 38003598, '38003598', 8516, '8516']:
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
