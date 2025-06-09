# -*- coding: utf-8 -*-
"""
Module for Cleaning Labs.

Created on Sat Dec 11 18:53:09 2021.

@author: ruppert20
"""
import os
import pandas as pd
from ..FileHandling.io import check_load_df
from .data_format_and_manipulation import deduplicate_and_join, create_dict
import re
from unidecode import unidecode
from ..Logging.log_messages import log_print_email_message as logm


def clean_labs(df: pd.DataFrame,
               id_cols: list,
               debug_conversion: bool = False,
               return_labs_only: bool = False,
               **logging_kwargs):
    """
    Clean laboratory data.

    Parameters
    ----------
    df : pd.DataFrame
        Laboratory dataframe
        must contain the following columns:
            *id_cols
            *lab_result or value_source_value
            *lab_unit or unit_source_value or source_unit_concept_id
            *meaurement_concept_id or
    id_cols : list
        List of index columns to be preserved. Examples include [person_id, visit_occurrence_id, visit_detail_id, batch_visit_detail_id, order_num, 'patient_deiden_id', 'merged_enc_id', 'encounter_deiden_id'].
    debug_conversion : bool, optional
        Whether non-convertable rows should be returned with enhanced metadata. The default is False.

    Returns
    -------
    TYPE
        DESCRIPTION.

    """
    # format source data
    df: pd.DataFrame = check_load_df(df, ds_type='pandas', **logging_kwargs,
                                     desired_types={**{x: 'sparse_int' for x in ['unit_source_concept_id', 'unit_concept_id',
                                                                                 'operator_concept_id', 'measurement_concept_id', 'lab_id']},
                                                    **{'value_as_number': 'float', 'value_source_value': 'str'}})

    # check for required columns
    unit_cols: list = ['unit_source_value', 'unit_source_concept_id', 'lab_unit', 'unit_concept_id']
    assert len(df.columns.intersection(unit_cols).tolist()) > 0, f'No Unit Column Detected, ensure that at least one of the following columns is present: {unit_cols}'

    val_cols: list = ['value_source_value', 'lab_result']
    assert len(df.columns.intersection(val_cols).tolist()) > 0, f'No Value Column Detected, ensure that at least one of the following columns is present: {val_cols}'

    lab_type_cols: list = ['meaurement_concept_id', 'loinc_code', 'variable_name', 'stamped_and_inferred_loinc_code']
    loinc_col: str = 'stamped_and_inferred_loinc_code' if 'stamped_and_inferred_loinc_code' in df.columns else 'loinc_code' if 'loinc_code' in df.columns else None
    if isinstance(loinc_col, str) and (str(loinc_col) != 'loinc_code'):
        df.rename(columns={loinc_col: 'loinc_code'}, inplace=True)
        loinc_col: str = 'loinc_code'
    assert len(df.columns.intersection(lab_type_cols).tolist()) > 0, f'Lab Type Column Detected, ensure that at least one of the following columns is present: {lab_type_cols}'

    resource_dir: str = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'Resource_Files')
    logm(message='Formatting Lookup Tables', **logging_kwargs)
    unit_lookup_table = check_load_df(os.path.join(resource_dir, 'lab_lookup.xlsx'), ds_type='pandas', sheet_name='units', **logging_kwargs,
                                      desired_types={x: 'sparse_int' for x in ['source_unit_concept_id']})\
        .dropna(subset=['source_unit_concept_id'])
    unit_lookup_table['temp_merge'] = unit_lookup_table.unit_source_value.astype('category')
    unit_lookup_table.source_numerator_type = unit_lookup_table.source_numerator_type.astype('category')
    unit_lookup_table.source_denominator_part_1_type = unit_lookup_table.source_denominator_part_1_type.astype('category')
    unit_lookup_table.source_denominator_part_2_type = unit_lookup_table.source_denominator_part_2_type.astype('category')
    unit_lookup_table.source_unit_concept_id = unit_lookup_table.source_unit_concept_id.astype('category')
    unit_lookup_table.source_numerator_multiplier = unit_lookup_table.source_numerator_multiplier.astype(float)
    unit_lookup_table.source_denominator_part_1_multiplier = unit_lookup_table.source_denominator_part_1_multiplier.astype(float)
    unit_lookup_table.source_denominator_part_2_multiplier = unit_lookup_table.source_denominator_part_2_multiplier.astype(float)
    unit_lookup_table.rename(columns={'source_unit_concept_id': 'temp_unit_concept_id'}, inplace=True)

    standard_unit_lookup = check_load_df(os.path.join(resource_dir, 'lab_lookup.xlsx'), sheet_name='standard_lab_units_by_loinc',
                                         **logging_kwargs, ds_type='pandas',
                                         desired_types={x: 'sparse_int' for x in ['measurement_concept_id', 'target_unit_concept_id',
                                                                                  ]})\
        .dropna(subset=['lab_type', 'target_unit_concept_id'], how='all')\
        .merge(unit_lookup_table.drop(columns=['unit_source_value', 'assumed', 'temp_merge'])
               .drop_duplicates(subset=['temp_unit_concept_id'])
               .rename(columns={**{'temp_unit_concept_id': 'target_unit_concept_id'}, **{x: x.replace('source', 'target') for x in unit_lookup_table.columns if 'source' in x}}),
               on='target_unit_concept_id',
               how='left')
        
        # left off here need to rename columns from above to get same effect, will need to test about category conversion is they are still applied
    standard_unit_lookup.loinc_code = standard_unit_lookup.loinc_code.astype('category')
    standard_unit_lookup.target_unit_concept_id = standard_unit_lookup.target_unit_concept_id.astype('category')
    standard_unit_lookup.target_numerator_multiplier = standard_unit_lookup.target_numerator_multiplier.astype(float)
    standard_unit_lookup.target_denominator_part_1_multiplier = standard_unit_lookup.target_denominator_part_1_multiplier.astype(float)
    standard_unit_lookup.target_denominator_part_2_multiplier = standard_unit_lookup.target_denominator_part_2_multiplier.astype(float)
    standard_unit_lookup.lab_type = standard_unit_lookup.lab_type.astype('category')
    standard_unit_lookup.eq_mol = standard_unit_lookup.eq_mol.astype(float)
    standard_unit_lookup.molar_mass = standard_unit_lookup.molar_mass.astype(float)
    standard_unit_lookup.loc[:, 'min'] = standard_unit_lookup.loc[:, 'min'].astype(float)
    standard_unit_lookup.loc[:, 'max'] = standard_unit_lookup.loc[:, 'max'].astype(float)

    if 'variable_name' in df.columns:
        standard_unit_lookup = standard_unit_lookup.dropna(subset=['lab_type'])\
            .drop(columns=['loinc_code', 'measurement_concept_id'])\
            .groupby('lab_type', group_keys=False)\
            .agg('first')\
            .reset_index(drop=False)\
            .copy(deep=True)

        # match case to the variable_name
        standard_unit_lookup.lab_type = standard_unit_lookup.lab_type.str.lower()

        # set the merge column for standardizing units
        lab_id_col: str = 'lab_type'

        return_labs_only = True

        df.rename(columns={'variable_name': 'lab_type'}, inplace=True)

    # UF-Only Impute Loincs and rename columns to omop_standard
    elif (len(df.columns.intersection(['lab_id', loinc_col])) == 2):
        logm(message='Impute Loincs based on Lab ID', **logging_kwargs)
        # step 1 label missing loincs using lab ids and convert loincs, lab_ids, and procedure codes to categories to improve performance
        df.lab_id = df.lab_id.astype('category')
        logm(message='Filling in missing LOINCS via Lab IDs', **logging_kwargs)
        missing_2160_mask = df.lab_id.isin(['969', '1526296', '1510379', '3412', '3156']) & df.loinc_code.isnull()
        if missing_2160_mask.any():
            df.loc[missing_2160_mask, 'loinc_code'] = '2160-0'
        del missing_2160_mask

        # assign missing POC creatinine loincs
        missing_poc_mask = (df.lab_id == '3028') & df.loinc_code.isnull()
        if missing_poc_mask.any():
            df.loc[missing_poc_mask, 'loinc_code'] = '38483-4'
        del missing_poc_mask
        df.loinc_code = df.loinc_code.astype('category')

        # seperate out notes and other long results
        logm(message='Processing Notes', **logging_kwargs)
        df, notes = _process_notes(df=df.copy(), id_cols=id_cols)
        df.rename(columns={'lab_unit': 'unit_source_value', 'lab_result': 'value_source_value'}, inplace=True)

        # set the merge column for standardizing units
        lab_id_col: str = 'loinc_code'
        standard_unit_lookup.drop(columns=['measurement_concept_id'], inplace=True)
    else:
        return_labs_only = True
        # set the merge column for standardizing units
        lab_id_col: str = 'measurement_concept_id'

    # attemp to merge unsing unit_concept_id
    if 'unit_concept_id' in df.columns:
        df = df.merge(unit_lookup_table[['temp_unit_concept_id']].drop_duplicates(),
                      right_on='temp_unit_concept_id',
                      left_on='unit_concept_id',
                      how='left')
        matched_idx: pd.Series = df.temp_unit_concept_id.notnull()
        if matched_idx.any():
            df.loc[matched_idx, 'use_source_van'] = 1
        else:
            df['use_source_van'] = None
        del matched_idx
    else:
        df['temp_unit_concept_id'] = None
        df['use_source_van'] = None

    # attempt merge using 'source_unit_concept_id'
    if 'source_unit_concept_id' in df.columns:
        df = df.merge(unit_lookup_table[['temp_unit_concept_id']].drop_duplicates(),
                      right_on='temp_unit_concept_id',
                      left_on='source_unit_concept_id',
                      how='left')
        fillable_idx: pd.Series = df.temp_unit_concept_id_x.isnull() & df.temp_unit_concept_id_y.notnull()
        if fillable_idx.any():
            df.loc[fillable_idx, 'temp_unit_concept_id_x'] = df.loc[fillable_idx, 'temp_unit_concept_id_y'].values
        df.rename(columns={'temp_unit_concept_id_x': 'temp_unit_concept_id'}, inplace=True)
        df.drop(columns=['temp_unit_concept_id_y'], inplce=True)
        del fillable_idx

    # attempt to fill any missing temp_unit_concept_id using unit_source_value
    if 'unit_source_value' in df.columns:
        df.loc[:, 'unit_source_value'] = df.loc[:, 'unit_source_value'].fillna('').astype(str).apply(unidecode).str.strip().replace({'': None}).values
        df['temp_merge'] = df.unit_source_value.str.upper().astype('category')
        df.unit_source_value = df.unit_source_value.astype('category')
        df = df.merge(unit_lookup_table[['temp_unit_concept_id', 'temp_merge']]
                      .drop_duplicates(subset=['temp_merge']),
                      on='temp_merge',
                      how='left')

    # if both a 'source_unit_concept_id' and 'unit_source_value' exist, prefer the source concept id then use the unit source value version
    if 'temp_unit_concept_id_x' in df.columns:
        fillable: pd.Series = df.temp_unit_concept_id_x.isnull() & df.temp_unit_concept_id_y.notnull()
        if fillable.any():
            df.loc[fillable, 'temp_unit_concept_id_x'] = df.loc[fillable, 'temp_unit_concept_id_y'].values
        del fillable
        df.drop(columns=['temp_unit_concept_id_y'], errors='ignore', inplace=True)
        df.rename(columns={'temp_unit_concept_id_x': 'temp_unit_concept_id'}, inplace=True)

    df = df.merge(unit_lookup_table[['temp_unit_concept_id', 'source_unit_name',
                                     'source_numerator_type', 'source_numerator_multiplier',
                                     'source_denominator_part_1_type', 'source_denominator_part_1_multiplier',
                                     'source_denominator_part_2_type', 'source_denominator_part_2_multiplier',
                                     'source_numerator_substance', 'source_denominator_part_1_substance',
                                     'source_denominator_part_2_substance']].drop_duplicates(subset=['temp_unit_concept_id']),
                  on='temp_unit_concept_id',
                  how='left')
    # merge lab standards
    df = df.merge(standard_unit_lookup[standard_unit_lookup.columns.intersection(['loinc_code',
                                                                                  'lab_type',
                                                                                  'measurement_concept_id']).tolist() + ['eq_mol', 'molar_mass', 'min', 'max',
                                                                                                                         'target_unit_concept_id', 'target_numerator_type', 'target_numerator_multiplier',
                                                                                                                         'target_denominator_part_1_type', 'target_denominator_part_1_multiplier',
                                                                                                                         'target_denominator_part_2_type', 'target_denominator_part_2_multiplier',
                                                                                                                         'target_numerator_substance', 'target_denominator_part_1_substance',
                                                                                                                         'target_denominator_part_2_substance', 'generate_binary', 'generate_categorical',
                                                                                                                         'generate_numeric', 'var_abbrev']]
                  .drop_duplicates(subset=[lab_id_col]),
                  on=lab_id_col,
                  how='left')\
        .drop(columns=['temp_merge'], errors='ignore')\
        .reset_index(drop=True)
    del standard_unit_lookup, unit_lookup_table

    # check for units not in dict
    if 'unit_source_value' in df.columns:
        missing_source_standard = df.temp_unit_concept_id.isnull() & df.unit_source_value.notnull()
        if any(missing_source_standard):
            probs: list = []
            for x in df.loc[missing_source_standard, "unit_source_value"].unique().tolist():
                try:
                    float(x)
                except ValueError:
                    probs.append(x)
            if len(probs) > 0:
                logm(f'{len(probs)} source units did not have a standard. Please add the following units to the standard lookup table {probs}',
                     warning=True, **logging_kwargs)
        del missing_source_standard

    df['conversion_ratio'] = None

    logm(message='Standardizing lab Units', **logging_kwargs)
    # at target_mask
    already_target_mask = (df.target_unit_concept_id.astype(float) == df.temp_unit_concept_id.astype(float)) | df.target_unit_concept_id.isnull()
    if already_target_mask.any():
        df.loc[already_target_mask, 'conversion_ratio'] = 1
    del already_target_mask

    # flag missing units and assume unit based on majority
    df['imputed_unit'] = None
    if 'unit_concept_id' not in df.columns:
        df['unit_concept_id'] = None

    probably_on_target_mask = (df.target_unit_concept_id.notnull()
                               & (df.temp_unit_concept_id.isnull()
                                  | (df.temp_unit_concept_id.astype(float) == -1))
                               & df.unit_concept_id.notnull())
    if probably_on_target_mask.any():
        df.loc[probably_on_target_mask, 'conversion_ratio'] = 1
        df.loc[probably_on_target_mask, 'imputed_unit'] = '1'
    del probably_on_target_mask
    df.imputed_unit.fillna('0', inplace=True)

    # meq_molar molar conversion
    molar_meq_eq_mask: pd.Series = (df.eq_mol.notnull()
                                    & df.conversion_ratio.isnull()
                                    & df.source_numerator_type.isin(['equivalent'])
                                    & df.target_numerator_type.isin(['mole']))
    if molar_meq_eq_mask.any():
        df.loc[molar_meq_eq_mask, 'source_numerator_type'] = 'mole'
        df.loc[molar_meq_eq_mask, 'source_numerator_multiplier'] = (df.loc[molar_meq_eq_mask, 'source_numerator_multiplier']
                                                                    * df.loc[molar_meq_eq_mask, 'eq_mol']).values
    del molar_meq_eq_mask

    # convert moles to mass
    molar_mass_mask: pd.Series = (df.molar_mass.notnull()
                                  & df.conversion_ratio.isnull()
                                  & df.source_numerator_type.isin(['mole'])
                                  & df.target_numerator_type.isin(['mass']))
    if molar_mass_mask.any():
        df.loc[molar_mass_mask, 'source_numerator_type'] = 'mass'
        df.loc[molar_mass_mask, 'source_numerator_multiplier'] = (df.loc[molar_mass_mask, 'source_numerator_multiplier']
                                                                  * df.loc[molar_mass_mask, 'molar_mass']).values
    del molar_mass_mask

    # convert mass to moles
    mass_molar_mask: pd.Series = (df.molar_mass.notnull()
                                  & df.conversion_ratio.isnull()
                                  & df.source_numerator_type.isin(['mass'])
                                  & df.target_numerator_type.isin(['mole']))
    if mass_molar_mask.any():
        df.loc[mass_molar_mask, 'source_numerator_type'] = 'mole'
        df.loc[mass_molar_mask, 'source_numerator_multiplier'] = (df.loc[mass_molar_mask, 'source_numerator_multiplier']
                                                                  / df.loc[mass_molar_mask, 'molar_mass']).values
    del mass_molar_mask

    # mmol/g creat -> mmol/mol Cr
    gram_mol_creat: pd.Series = (df.temp_unit_concept_id.astype(float) == 9576) & (df.target_unit_concept_id.astype(float) == 9019)
    if gram_mol_creat.any():
        df.loc[gram_mol_creat, 'conversion_ratio'] = 613.995279
    del gram_mol_creat

    # flag directly convertable units
    directly_convertable_mask = ((df.target_numerator_type.astype(str) == df.source_numerator_type.astype(str))
                                 & df.conversion_ratio.isnull()
                                 & df.target_numerator_type.notnull()
                                 & (df.target_denominator_part_1_type.astype(str).fillna('-999') == df.source_denominator_part_1_type.astype(str).fillna('-999'))
                                 & (df.target_denominator_part_2_type.astype(str).fillna('-999') == df.source_denominator_part_2_type.astype(str).fillna('-999')))
    if directly_convertable_mask.any():
        df.loc[directly_convertable_mask, 'conversion_ratio'] = ((df.loc[directly_convertable_mask,
                                                                         'source_numerator_multiplier'].fillna(1)
                                                                  / df.loc[directly_convertable_mask,
                                                                           'target_numerator_multiplier'].fillna(1))
                                                                 / (df.loc[directly_convertable_mask,
                                                                           'source_denominator_part_1_multiplier'].fillna(1)
                                                                    / df.loc[directly_convertable_mask,
                                                                             'target_denominator_part_1_multiplier'].fillna(1))
                                                                 / (df.loc[directly_convertable_mask,
                                                                           'source_denominator_part_2_multiplier'].fillna(1)
                                                                    / df.loc[directly_convertable_mask,
                                                                             'target_denominator_part_2_multiplier'].fillna(1))).values
    del directly_convertable_mask

    # imputed volume for /HPF and thousand
    hpf_source_mask = df.temp_unit_concept_id.astype(float).isin([8786, 8566])
    if hpf_source_mask.any():
        df.loc[hpf_source_mask, 'source_denominator_part_1_type'] = 'volume'
        df.loc[hpf_source_mask, 'source_denominator_part_1_multiplier'] = 1E-6
    del hpf_source_mask

    hpf_target_mask = df.target_unit_concept_id.astype(float).isin([8786, 8566])
    if hpf_target_mask.any():
        df.loc[hpf_target_mask, 'target_denominator_part_1_type'] = 'volume'
        df.loc[hpf_target_mask, 'target_denominator_part_1_multiplier'] = 1E-6
    del hpf_target_mask

    hpf_convertable_mask = ((df.target_numerator_type.astype(str) == df.source_numerator_type.astype(str))
                            & df.conversion_ratio.isnull()
                            & df.target_numerator_type.notnull()
                            & (df.target_denominator_part_1_type.astype(str).fillna('-999') == df.source_denominator_part_1_type.astype(str).fillna('-999'))
                            & (df.target_denominator_part_2_type.astype(str).fillna('-999') == df.source_denominator_part_2_type.astype(str).fillna('-999')))
    if hpf_convertable_mask.any():
        df.loc[hpf_convertable_mask, 'conversion_ratio'] = ((df.loc[hpf_convertable_mask,
                                                                    'source_numerator_multiplier'].fillna(1)
                                                             / df.loc[hpf_convertable_mask,
                                                                      'target_numerator_multiplier'].fillna(1))
                                                            / (df.loc[hpf_convertable_mask,
                                                                      'source_denominator_part_1_multiplier'].fillna(1)
                                                               / df.loc[hpf_convertable_mask,
                                                                        'target_denominator_part_1_multiplier'].fillna(1))).values
    del hpf_convertable_mask

    percent_percent_mask = (df.target_numerator_type.isin(['percent'])
                            & df.source_numerator_type.isin(['percent'])
                            & df.conversion_ratio.isnull())
    if percent_percent_mask.any():
        df.loc[percent_percent_mask, 'conversion_ratio'] = 1
    del percent_percent_mask

    # INR equivalent
    inr_mask: pd.Series = (df.lab_type.isin(['INR']) & df.conversion_ratio.isnull())
    if inr_mask.any():
        df.loc[inr_mask, 'conversion_ratio'] = 1
    del inr_mask

    # mark calc and ratio as equivalent
    calc_ratio: pd.Series = ((df.temp_unit_concept_id.astype(float) == 8596)
                             & (df.target_unit_concept_id.astype(float) == 8523)
                             & df.conversion_ratio.isnull())
    if calc_ratio.any():
        df.loc[calc_ratio, 'conversion_ratio'] = 1
    del calc_ratio

    # percent conversion for likely mislabeled units
    percent_problem_mask: pd.Series = (df.conversion_ratio.isnull()
                                       & df.source_numerator_type.isin(['percent'])
                                       & df.lab_type.isin(['MCHC', 'Eosinophils']))
    if percent_problem_mask.any():
        df.loc[percent_problem_mask, 'conversion_ratio'] = 1
        df.loc[percent_problem_mask, 'imputed_unit'] = '1'
    del percent_problem_mask

    # non-standardized unit mask
    nonstandard_mask: pd.Series = df.conversion_ratio.isnull()
    df['non_standard_unit'] = 0

    if debug_conversion:
        return df.loc[nonstandard_mask,
                      ['loinc_code', 'lab_type', 'unit_source_value', 'temp_unit_concept_id',
                       'target_unit_name', 'target_unit_concept_id', 'source_numerator_type', 'target_numerator_type',
                       'source_denominator_part_1_type', 'target_denominator_part_1_type', 'source_denominator_part_2_type',
                       'target_denominator_part_2_type', 'loinc_name', 'source_numerator_multiplier',
                       'source_denominator_part_1_multiplier', 'source_denominator_part_2_multiplier',
                       'source_numerator_substance', 'source_denominator_part_1_substance', 'source_denominator_part_2_substance',
                       'eq_mol', 'molar_mass', 'target_numerator_multiplier', 'target_denominator_part_1_multiplier',
                       'target_denominator_part_2_multiplier', 'target_numerator_substance', 'target_denominator_part_1_substance',
                       'target_denominator_part_2_substance']].sort_values('lab_type')
    if nonstandard_mask.any():
        df.loc[nonstandard_mask, 'non_standard_unit'] = 1
        # df.loc[nonstandard_mask, 'target_unit_concept_id'] = df.loc[nonstandard_mask, 'temp_unit_concept_id'].values
    del nonstandard_mask

    # step 3 extract numbers
    if 'unit_concept_id' in df.columns:
        fillable_idx: pd.Series = df.use_source_van.notnull()  # we can only use rows where the unit conept id is present since there may have been a conversion from the value_source_value
        if fillable_idx.any() and ('value_as_number' in df.columns):
            df.loc[fillable_idx, 'temp_value_as_number'] = df.loc[fillable_idx, 'value_as_number'].values
        else:
            df['temp_value_as_number'] = None
        del fillable_idx
    df.drop(columns=['use_source_van'], inplace=True)

    # For all other rows, we use first number extracted from the value source_value
    missing_vn: pd.Series = df.temp_value_as_number.isnull()
    if missing_vn.any():
        df.loc[missing_vn, 'temp_value_as_number'] = pd.to_numeric(df.loc[missing_vn, 'value_source_value'].apply(_extract_num, return_pos=0), errors='coerce').values
    del missing_vn

    # step 4 convert from source to standard unit
    is_number: pd.Series = df.temp_value_as_number.notnull()
    df.loc[is_number, 'temp_value_as_number'] = df.loc[is_number, 'temp_value_as_number'] * df.loc[is_number, 'conversion_ratio'].fillna(1)
    del is_number

    logm(message='Extracting operators and text concepts', **logging_kwargs)

    # step 5 extract operators
    df['operator_source_value'] = df.value_source_value.apply(_extract_operator, return_pos=0).values

    # step 6 extract text concepts
    if 'value_as_concept' in df.columns:
        df.value_as_concept_id.replace({0: None, '0': None}, inplace=True)
    else:
        df['value_as_concept'] = None
    large_mask = (df.value_source_value.str.contains(r'\blar[^sy]|\b3\+|\b4\+', case=False, na=False, regex=True)
                  & df.value_as_concept.isnull())
    if any(large_mask):
        df.loc[large_mask, 'value_as_concept'] = "Large"
    del large_mask

    # handle moderate
    mod_mask = (df.value_source_value.str.contains(r'\bmod[^y]|2\+', case=False, na=False, regex=True)
                & df.value_as_concept.isnull())
    if any(mod_mask):
        df.loc[mod_mask, 'value_as_concept'] = "Moderate"
    del mod_mask

    # handle small
    small_mask = (df.value_source_value.str.contains(r'\bsm[^eiu]|\btr$|^trac[^hu]|1\+', case=False, na=False, regex=True)
                  & df.value_as_concept.isnull())
    if any(small_mask):
        df.loc[small_mask, 'value_as_concept'] = 'Small'
    del small_mask

    # handle normals
    normal_mask = (df.value_source_value.str.contains(r'^norm|\bnml|noraml|nomral|\bnrom|\bnoremal|\bnomal|\bno\sabnorm', case=False, na=False, regex=True)
                   & df.value_as_concept.isnull())
    if any(normal_mask):
        df.loc[normal_mask, 'value_as_concept'] = 'Normal'
    del normal_mask

    # handle negatives
    negative_mask = (df.value_source_value.str.contains(r'\bneg|\bneeg|\sneg|\bnreg|\bnehgative|\bneagative|^nedg$|^neb$|^ned$|/bneative|N\sE\sG\sA\sT\sI\sV\sE|^\-$', case=False, na=False, regex=True)
                     & df.value_as_concept.isnull())
    if any(negative_mask):
        df.loc[negative_mask, 'value_as_concept'] = "Negative"
    del negative_mask

    # handle positives
    negative_mask = (df.value_source_value.str.contains(r'\bpositive|^\+$|^pos$', case=False, na=False, regex=True)
                     & df.value_as_concept.isnull())
    if any(negative_mask):
        df.loc[negative_mask, 'value_as_concept'] = "Positive"
    del negative_mask

    # handle numeric to concept extractions
    glurn_mask: pd.Series = (df.lab_type == 'GLUCOSE_UR') & df.temp_value_as_number.notnull() & df.value_as_concept.isnull()
    if glurn_mask.any():
        small_glurn = glurn_mask & (df.temp_value_as_number <= 499)
        large_glurn = glurn_mask & (df.temp_value_as_number >= 1000)
        moderate_glurn = glurn_mask & (df.temp_value_as_number < 1000) & (df.temp_value_as_number >= 500)
        if small_glurn.any():
            df.loc[small_glurn, 'value_as_concept'] = 'Small'
        del small_glurn
        if large_glurn.any():
            df.loc[large_glurn, 'value_as_concept'] = 'Large'
        del large_glurn
        if moderate_glurn.any():
            df.loc[moderate_glurn, 'value_as_concept'] = "Moderate"
        del moderate_glurn
    del glurn_mask

    uap_mask: pd.Series = (df.lab_type == 'UAP_cat') & df.temp_value_as_number.notnull() & df.value_as_concept.isnull()
    if uap_mask.any():
        small_uap = uap_mask & (df.temp_value_as_number < 30)
        large_uap = uap_mask & (df.temp_value_as_number >= 300)
        moderate_uap = uap_mask & (df.temp_value_as_number < 300) & (df.temp_value_as_number >= 30)
        if small_uap.any():
            df.loc[small_uap, 'value_as_concept'] = 'Small'
        del small_uap
        if large_uap.any():
            df.loc[large_uap, 'value_as_concept'] = 'Large'
        del large_uap
        if moderate_uap.any():
            df.loc[moderate_uap, 'value_as_concept'] = "Moderate"
        del moderate_uap
    del uap_mask

    rbcur_mask: pd.Series = (df.lab_type == 'RBC_UR') & df.temp_value_as_number.notnull() & df.value_as_concept.isnull()
    if rbcur_mask.any():
        negative_rbcur = rbcur_mask & (df.temp_value_as_number <= 4)
        small_rbcur = rbcur_mask & (df.temp_value_as_number > 4) & (df.temp_value_as_number <= 30)
        large_rbcur = rbcur_mask & (df.temp_value_as_number > 50)
        moderate_rbcur = rbcur_mask & (df.temp_value_as_number <= 50) & (df.temp_value_as_number > 30)
        if small_rbcur.any():
            df.loc[small_rbcur, 'value_as_concept'] = 'Small'
        del small_rbcur
        if large_rbcur.any():
            df.loc[large_rbcur, 'value_as_concept'] = 'Large'
        del large_rbcur
        if moderate_rbcur.any():
            df.loc[moderate_rbcur, 'value_as_concept'] = "Moderate"
        del moderate_rbcur
        if negative_rbcur.any():
            df.loc[negative_rbcur, 'value_as_concept'] = "Negative"
        del negative_rbcur
    del rbcur_mask

    logm(message='Flagging out of range values', **logging_kwargs)

    # step 7: flag out of range values
    df['out_of_range_flag'] = 0
    oor: pd.Series = (df.temp_value_as_number.notnull()
                      & df['max'].notnull()
                      & df['min'].notnull()
                      & ((df.temp_value_as_number > df['max']) | (df.temp_value_as_number < df['min'])))
    if oor.any():
        df.loc[oor, 'out_of_range_flag'] = 1
    del oor

    # use original numeric values for non-standard units if they exist
    if ('value_as_number' in df.columns) and ('unit_concept_id' in df.columns):
        non_standard_numeric: pd.Series = df.value_as_number.notnull() & (df.non_standard_unit == 1) & (df.unit_concept_id != 0)
        df.loc[non_standard_numeric, 'temp_value_as_number'] = df.loc[non_standard_numeric, 'value_as_number'].values
        df.loc[non_standard_numeric, 'target_unit_concept_id'] = df.loc[non_standard_numeric, 'unit_concept_id'].values
        del non_standard_numeric

    df['temp_value_as_concept_id'] = pd.to_numeric(df.value_as_concept.replace({'Large': 45878584, 'Small': 45881797,
                                                                                'Moderate': 45877983, 'Negative': 45878583,
                                                                               'Positive': 45884084, 'Normal': 45884153}), errors='coerce')

    concept_mask: pd.Series = df.temp_value_as_concept_id.notnull()
    df.loc[concept_mask, 'value_as_concept_id'] = df.loc[concept_mask, 'temp_value_as_concept_id'].values
    del concept_mask

    # format columns before returning them
    df.drop(columns=['value_as_number', 'unit_concept_id', 'unit_source_concept_id'], errors='ignore', inplace=True)
    df.rename(columns={'temp_value_as_number': 'value_as_number',
                       'target_unit_concept_id': 'unit_concept_id',
                       'temp_unit_concept_id': 'unit_source_concept_id',
                       'inferred_specimen_datetime': 'measurement_datetime',
                       'variable_name_x': 'source_variable_name',
                       'variable_name_y': 'variable_name',
                       'normal_low': 'range_low',
                       'normal_high': 'range_high'},
              inplace=True)
    df = df[[x for x in (id_cols + ['measurement_datetime', 'loinc_code', 'value_as_number', 'value_as_concept_id',
                                               'operator_source_value', 'value_as_concept', 'value_source_value', 'unit_source_value', 'source_variable_name',
                                               'range_low', 'range_high', 'result_datetime', 'order_datetime', 'operator_concept_id', 'variable_name',
                                               'temp_unit_concept_id', 'lab_type', 'unit_concept_id', 'lab_id', 'imputed_unit', 'unit_source_concept_id',
                                               'out_of_range_flag', 'non_standard_unit', 'generate_binary', 'generate_categorical', 'generate_numeric', 'var_abbrev',
                                               'intraop_y_n', 'idr_intraop_y_n', 'ic3_lab_id']) if x in df.columns]]

    if return_labs_only:
        return df
    elif isinstance(notes, pd.DataFrame):
        return df, notes
    else:
        return df, pd.DataFrame(columns=id_cols)


def _extract_num(input_str: str, return_pos: int = 0, abs_value: bool = False, force_negative: bool = False) -> str:
    # check if blank
    if pd.isnull(input_str):
        return None
    else:
        input_str = str(input_str)

    # force absolute value if there is a letter preceeding a dash
    if bool(re.search(r'[A-z]\-[0-9]', input_str)):
        abs_value: bool = True
    # recognize dates or fractions and return None
    elif bool(re.search(r'[0-9]{4}\-[0-9]{2}\-[0-9]{2}|[0-9]/[0-9]', input_str)):
        return None

    # force negtive if it is written in text
    if bool(re.search(r'NEG[0-9]|NEG\s[0-9]|NEGative[0-9]|NEGative\s[0-9]', input_str, re.IGNORECASE)):
        force_negative: bool = True

    if abs_value:
        nums = re.findall(r'[0-9]+\.[0-9]+|[0-9][0-9,.]+[0-9]+|[0-9]+', input_str)  # with negative
    else:
        nums = re.findall(r'-[0-9]+\.[0-9]+|-[0-9][0-9,.]+[0-9]+|-[0-9]+|[0-9]+\.[0-9]+|[0-9][0-9,.]+[0-9]+|[0-9]+', input_str)  # with negative

    if len(nums) == 0:
        return None

    elif bool(re.search(r'[0-9]\-[0-9]', input_str)):
        if len(nums) == 2:
            try:
                return (_format_number(nums[0]) + abs(_format_number(nums[1]))) / 2
            except:
                pass
        return None
    elif len(nums) > 3:
        return None
    else:
        return _format_number(nums[return_pos]) * (-1 if (force_negative and (len(nums) == 1)) else 1)


def _format_number(input_str: str):
    input_str: str = input_str.replace(',', '').replace('...', '.').replace('..', '.')
    try:
        return float(input_str)
    except:
        return input_str


def _extract_operator(input_str: str, return_pos: int = 0) -> str:
    if pd.isnull(input_str):
        return None
    nums = re.findall(r'[<>=≥≤]+', str(input_str))

    if len(nums) == 0:
        return None

    return nums[return_pos]


unit_dict: dict = {'KG': {'multiplier': int(1E3), 'type': 'mass'},
                   'KILOGRAM': {'multiplier': int(1E3), 'type': 'mass'},
                   'LB': {'multiplier': 453.592, 'type': 'mass'},
                   'POUND': {'multiplier': 453.592, 'type': 'mass'},
                   'G': {'multiplier': 1, 'type': 'mass'},
                   'GM': {'multiplier': 1, 'type': 'mass'},
                   'GRAM': {'multiplier': 1, 'type': 'mass'},
                   'GRAMS': {'multiplier': 1, 'type': 'mass'},
                   'MG': {'multiplier': 1E-3, 'type': 'mass'},
                   'MILLIGRAM': {'multiplier': 1E-3, 'type': 'mass'},
                   'MILLIGRAMS': {'multiplier': 1E-3, 'type': 'mass'},
                   'MCG': {'multiplier': 1E-6, 'type': 'mass'},
                   'MICROGRAM': {'multiplier': 1E-6, 'type': 'mass'},
                   'MICROGRAMS': {'multiplier': 1E-6, 'type': 'mass'},
                   'UG': {'multiplier': 1E-6, 'type': 'mass'},
                   'NG': {'multiplier': 1E-9, 'type': 'mass'},
                   'NANOGRAM': {'multiplier': 1E-9, 'type': 'mass'},
                   'NANOGRAMS': {'multiplier': 1E-9, 'type': 'mass'},
                   'PG': {'multiplier': 1E-12, 'type': 'mass'},
                   'PICOGRAM': {'multiplier': 1E-12, 'type': 'mass'},
                   'PICOGRAMS': {'multiplier': 1E-12, 'type': 'mass'},
                   'YR': {'multiplier': float(3600 * 7 * 24 * 364.25), 'type': 'time'},
                   'YEAR': {'multiplier': float(3600 * 7 * 24 * 364.25), 'type': 'time'},
                   'WK': {'multiplier': int(3600 * 7 * 24), 'type': 'time'},
                   'WEEK': {'multiplier': int(3600 * 7 * 24), 'type': 'time'},
                   'WEEKS': {'multiplier': int(3600 * 7 * 24), 'type': 'time'},
                   'DAY': {'multiplier': int(3600 * 24), 'type': 'time'},
                   'DAYS': {'multiplier': int(3600 * 24), 'type': 'time'},
                   'H': {'multiplier': 3600, 'type': 'time'},
                   'HR': {'multiplier': 3600, 'type': 'time'},
                   'HOUR': {'multiplier': 3600, 'type': 'time'},
                   'HOURS': {'multiplier': 3600, 'type': 'time'},
                   'MIN': {'multiplier': 60, 'type': 'time'},
                   'MINUTE': {'multiplier': 60, 'type': 'time'},
                   'MINUTES': {'multiplier': 60, 'type': 'time'},
                   'S': {'multiplier': 1, 'type': 'time'},
                   'SEC': {'multiplier': 1, 'type': 'time'},
                   'SECOND': {'multiplier': 1, 'type': 'time'},
                   'SECONDS': {'multiplier': 1, 'type': 'time'},
                   'U': {'multiplier': 1, 'type': 'unit'},
                   'IU': {'multiplier': 1, 'type': 'unit'},
                   'EU': {'multiplier': 1, 'type': 'unit'},
                   'UT': {'multiplier': 1, 'type': 'unit'},
                   'UNT': {'multiplier': 1, 'type': 'unit'},
                   'UNIT': {'multiplier': 1, 'type': 'unit'},
                   'UNITS': {'multiplier': 1, 'type': 'unit'},
                   'MEQ': {'multiplier': 1E-3, 'type': 'equivalent'},
                   'EQ': {'multiplier': 1, 'type': 'equivalent'},
                   'MILLIEQUIVALENT': {'multiplier': 1E-3, 'type': 'equivalent'},
                   'MILLIEQUIVALENTS': {'multiplier': 1E-3, 'type': 'equivalent'},
                   'MBQ': {'multiplier': float(1 / 37), 'type': 'baquerette'},
                   'MCI': {'multiplier': 1E-3, 'type': 'curie'},
                   'MOL': {'multiplier': 1, 'type': 'mole'},
                   'MOLS': {'multiplier': 1, 'type': 'mole'},
                   'MMOL': {'multiplier': 1E-3, 'type': 'mole'},
                   'MMOLE': {'multiplier': 1E-3, 'type': 'mole'},
                   'MILLIMOLE': {'multiplier': 1E-3, 'type': 'mole'},
                   'MILLIMOLES': {'multiplier': 1E-3, 'type': 'mole'},
                   'UMOL': {'multiplier': 1E-6, 'type': 'mole'},
                   'MICROMOLE': {'multiplier': 1E-6, 'type': 'mole'},
                   'MICROMOLES': {'multiplier': 1E-6, 'type': 'mole'},
                   'NMOL': {'multiplier': 1E-9, 'type': 'mole'},
                   'NANOMOLE': {'multiplier': 1E-9, 'type': 'mole'},
                   'NANOMOLES': {'multiplier': 1E-9, 'type': 'mole'},
                   'PMOL': {'multiplier': 1E-12, 'type': 'mole'},
                   'PICOMOLE': {'multiplier': 1E-12, 'type': 'mole'},
                   'PICOMOLES': {'multiplier': 1E-12, 'type': 'mole'},
                   'L': {'multiplier': 1, 'type': 'volume'},
                   'LITER': {'multiplier': 1, 'type': 'volume'},
                   'LITERS': {'multiplier': 1, 'type': 'volume'},
                   'DL': {'multiplier': 1E-1, 'type': 'volume'},
                   'DECILITER': {'multiplier': 1E-1, 'type': 'volume'},
                   'DECILITERS': {'multiplier': 1E-1, 'type': 'volume'},
                   'ML': {'multiplier': 1E-3, 'type': 'volume'},
                   'MILLILITER': {'multiplier': 1E-3, 'type': 'volume'},
                   'MILLILITERS': {'multiplier': 1E-3, 'type': 'volume'},
                   'UL': {'multiplier': 1E-6, 'type': 'volume'},
                   'MICROLITER': {'multiplier': 1E-6, 'type': 'volume'},
                   'MICROLITERS': {'multiplier': 1E-6, 'type': 'volume'},
                   'NL': {'multiplier': 1E-9, 'type': 'volume'},
                   'NANOLITER': {'multiplier': 1E-9, 'type': 'volume'},
                   'NANOLITERS': {'multiplier': 1E-9, 'type': 'volume'},
                   'PL': {'multiplier': 1E-12, 'type': 'volume'},
                   'PICOLITER': {'multiplier': 1E-12, 'type': 'volume'},
                   'PICOLITERS': {'multiplier': 1E-12, 'type': 'volume'},
                   'FL': {'multiplier': 1E-15, 'type': 'volume'},
                   'FEMTOLITER': {'multiplier': 1E-15, 'type': 'volume'},
                   'FEMTOLITERS': {'multiplier': 1E-15, 'type': 'volume'},
                   'SQUARE_METER': {'multiplier': 1, 'type': 'area'},
                   'SQUARE_CENTIMETER': {'multiplier': 1E-4, 'type': 'area'},
                   'SQUARE_MILLIMETER': {'multiplier': 1E-6, 'type': 'area'},
                   'CUBIC_METER': {'multiplier': int(1E3), 'type': 'volume'},
                   'CUBIC_CENTIMETER': {'multiplier': 1E-3, 'type': 'volume'},
                   'CUBIC_MILLIMETER': {'multiplier': 1E-6, 'type': 'volume'},
                   'CUBIC_MICROMETER': {'multiplier': 1E-15, 'type': 'volume'},
                   'DROP': {'multiplier': 1E-3 * float(1 / 20), 'type': 'volume'},
                   'DROPS': {'multiplier': 1E-3 * float(1 / 20), 'type': 'volume'},
                   'CAPSULE': {'multiplier': 1, 'type': 'unit'},
                   'TABLET': {'multiplier': 1, 'type': 'unit'},
                   'BU_US': {'multiplier': 1, 'type': 'unit'},
                   'LOZENGE': {'multiplier': 1, 'type': 'unit'},
                   'THOUSAND': {'multiplier': int(1E3), 'type': 'count'},
                   'MILLION': {'multiplier': int(1E6), 'type': 'count'},
                   'BILLION': {'multiplier': int(1E6), 'type': 'count'},
                   'HPF': {'multiplier': 1, 'type': 'count'},
                   'BEATS': {'multiplier': 1, 'type': 'count'},
                   'KM': {'multiplier': int(1E3), 'type': 'distance'},
                   'KILOMETER': {'multiplier': int(1E3), 'type': 'distance'},
                   'KILOMETERS': {'multiplier': int(1E3), 'type': 'distance'},
                   'M': {'multiplier': 1, 'type': 'distance'},
                   'METER': {'multiplier': 1, 'type': 'distance'},
                   'METERS': {'multiplier': 1, 'type': 'distance'},
                   'CM': {'multiplier': 1E-2, 'type': 'distance'},
                   'CENTIMETER': {'multiplier': 1E-2, 'type': 'distance'},
                   'CENTIMETERS': {'multiplier': 1E-2, 'type': 'distance'},
                   'MM': {'multiplier': 1E-3, 'type': 'distance'},
                   'MILLIMETER': {'multiplier': 1E-3, 'type': 'distance'},
                   'MILLIMETERS': {'multiplier': 1E-3, 'type': 'distance'},
                   'UM': {'multiplier': 1E-6, 'type': 'distance'},
                   'MICROMETER': {'multiplier': 1E-6, 'type': 'distance'},
                   'MICROMETERS': {'multiplier': 1E-6, 'type': 'distance'},
                   'IN_US': {'multiplier': 0.0254, 'type': 'distance'},
                   'FT_US': {'multiplier': 0.3048, 'type': 'distance'},
                   'UA': {'multiplier': 1, 'type': 'unit'},
                   'FIU': {'multiplier': 1, 'type': 'unit'},
                   'CU': {'multiplier': 1, 'type': 'unit'},
                   'CEL': {'multiplier': 1, 'type': 'temperature'},
                   'PPB': {'multiplier': int(1E9), 'type': 'partsper'},
                   'PPM': {'multiplier': int(1E6), 'type': 'partsper'},
                   'DEGF': {'multiplier': -999, 'type': 'temperature'},
                   'MS': {'multiplier': 1000, 'type': 'time'},
                   'MOSM': {'multiplier': 1E-3, 'type': 'osmols'},
                   'OSM': {'multiplier': 1, 'type': 'osmols'},
                   '%': {'multiplier': 1, 'type': 'percent'}}


def _parse_unit(row: pd.Series) -> pd.Series:
    step_1: str = re.sub(r'\b10\*', '1E',
                         re.sub(r'^10\*$', '10',
                                re.sub(r'^a$', 'YEAR',
                                       str(row.unit_code).upper().replace("'", ' '), flags=re.IGNORECASE)))\
        .replace('MM3', 'CUBIC_MILLIMETER').replace('MM2', 'SQUARE_MILLIMETER')\
        .replace('CM3', 'CUBIC_CENTIMETER').replace('CM2', 'SQUARE_CENTIMETER')\
        .replace('M3', 'CUBIC_METER').replace('M2', 'SQUARE_METER')\
        .split('/')

    out_dict: dict = {}

    for i, part in enumerate([x.strip() for x in step_1]):
        # break
        prefix: str = 'numerator_' if i == 0 else f'denominator_part_{i}_'

        # check if a substance is specified
        substance = re.search(r'{([^}]+)}', part)
        if bool(substance):
            out_dict[prefix + 'substance'] = substance.groups(0)[0]
        else:
            out_dict[prefix + 'substance'] = None

        multiplier = re.search(r'[0-9]+\.[0-9]+E-[0-9]+|[0-9]+E-[0-9]+|[0-9]+\.[0-9]+E[0-9]+|[0-9]+E[0-9]+|[0-9]+\.[0-9]+|[0-9]+', part)
        if bool(multiplier):
            out_dict[prefix + 'multiplier'] = float(multiplier.group(0))
            if len(part) == multiplier.end():
                out_dict[prefix + 'type'] = 'count'
                continue
        else:
            out_dict[prefix + 'multiplier'] = 1

        if part in ['{CELLS}', '{COPIES}', 'LG({COPIES})', '', 'LG({CELLS})']:
            out_dict[prefix + 'type'] = 'count'
            continue

        part = re.sub(r'[0-9]', '', part).replace('..', ' ').replace('.', ' ')

        try:
            units = pd.Series(re.findall(r'((\b|^)' + r'(\b|$|\s))|((\b|^)'.join([r'\%' if x == '%' else x for x in list(unit_dict.keys())]
                                                                                 ) + r'(\b|$|\s))', part.replace('{', ' '))[0]).replace({'': None, ' ': None}).dropna().str.strip()

            for j, unit in enumerate(units):
                out_dict[prefix + ('type' if j == 0 else f'type_{j}')] = unit_dict.get(unit, {}).get('type')
                out_dict[prefix + ('multiplier' if j == 0 else f'multiplier_{j}')] = unit_dict.get(unit, {}).get('multiplier', 1) * out_dict[prefix + 'multiplier']
        except IndexError:

            unit = re.search(r'((\b|^)' + r'(\b|$|\s))|((\b|^)'.join([r'\%' if x == '%' else x for x in list(unit_dict.keys())])
                             + r'(\b|$|\s))', 'billion per milliliter'.replace('{', ' '), re.IGNORECASE)
            if bool(unit):
                out_dict[prefix + 'type'] = unit_dict.get(unit.group(0).strip(), {}).get('type')
                out_dict[prefix + 'multiplier'] = unit_dict.get(unit.group(0).strip(), {}).get('multiplier', 1) * out_dict[prefix + 'multiplier']
            else:
                out_dict[prefix + 'type'] = 'count' if bool(re.search(r'^PER\s', row.unit_name, flags=re.IGNORECASE)) else 'UNKOWN'

    return pd.Series(out_dict)


def _process_notes(df: pd.DataFrame, id_cols: list) -> tuple:
    # seperate out comments
    df.rename(columns={'inferred_specimen_datetime': 'measurement_datetime'}, inplace=True)
    notes_mask = df.lab_name.str.contains('comment', case=False, na=False) | (df.lab_result.str.len() > 100)

    if notes_mask.any():
        notes: pd.DataFrame = df.loc[notes_mask,
                                     df.columns.intersection(id_cols + ['result_datetime',
                                                                        'measurement_datetime',
                                                                        'loinc_code', 'ic3_lab_id',
                                                                        'lab_result', 'lab_name', 'source_row', 'source_file'])]\
            .copy().dropna(subset=['lab_result', 'measurement_datetime'])
        # df = df[~notes_mask]

        # remove common uninformative notes
        notes_to_remove_mask = (notes.lab_result.str.strip().str.lower()
                                .isin(['results that are below the cutoff limits have not been confirmed by an alternate method and are intended for medical use only. cutoff limits are as follows: barbiturate and benzodiazepine: 200 ng/ml. cocaine metabolite, opiates, methadone, and oxycodone:',
                                       'egfr results >60 ml/min/1.73 m2 may be insufficiently accurate to base ther   apeutic decisions.',
                                      'the estimated glomerular filtration rate (egfr) is based on the mdrd study equation. the equation has not been tested in children, the elderly >75 years, pregnant women, patients with serious comorbid conditions, or persons with extremes of body size,',
                                       'testing was performed by the operating room staff. the attached results therefore cannot be independently verified by core laboratory technologists. critical result reporting does not apply since all results are monitored by or.',
                                       'results that are below the detection limit have not been confirmed by an alternate method and are intended for medical use only.',
                                       'results that are below the cutoff limits have not been confirmed by an alternate method and are intended for medical use only. cutoff limits are as follows: barbiturate and benzodiazepine: 200 ng/ml. cocaine metabolite, opiates, methadone, and oxycodone: 300 ng/ml. cannabinoid: 50 ng/ml. amphetamines: 1000 ng/ml.',
                                       'these results have not been confirmed by an alternate method and are intend   ed for medical use only.',
                                       'microscopic analysis not done on urines with negative biochemical tests',
                                       'digital differential not available. refer to automated or manual differential results below.',
                                       'these results have not been confirmed by an alternate method and are intended for medical use only.',
                                       'duplicate order',
                                       'wrong priority',
                                       'test(s) intended for medical use only.',
                                       'bnp is a polypeptide secreted by the left ventricle in response to increase   d left ventricular strain. bnp values exhibit significant intraindividual h   eterogeneity. values less than a 2-fold change are within the range of biol   ogic variation. 20%'])
                                | notes.lab_result.str.contains(r'The\sestimated\sglomerular\sfiltration\srate\s\(eGFR\)\sis\sbased\son\sthe\sMDRD\sstudy\sequation|Results\sthat\sare\sbelow\sthe\scutoff\slimits\shave\snot\sbeen\sconfirmed', case=False, na=False, regex=True))

        notes = notes[~notes_to_remove_mask]

        # stack multiple comments into single row
        notes.loinc_code = notes.loinc_code.astype(str).replace({'nan': None})
        notes.loinc_code.fillna('xxxmissingxxx', inplace=True)
        grouing_cols = notes.columns.intersection(id_cols + ['measurement_datetime', 'loinc_code', 'lab_name', 'ic3_lab_id']).tolist()
        notes = notes.fillna('xxxmissingxxx')\
            .groupby(grouing_cols, group_keys=False)\
            .agg(create_dict(col_action_dict={'first': notes.columns.tolist(),
                                              'grouping': grouing_cols,
                                              deduplicate_and_join: ['lab_result']}))\
            .reset_index()\
            .replace({'xxxmissingxxx': None})

        # Ensure all notes have a datetime
        missing_result_dt_mask = notes.result_datetime.isnull()
        if missing_result_dt_mask.any():
            notes.loc[missing_result_dt_mask, 'result_datetime'] = notes.loc[missing_result_dt_mask, 'measurement_datetime'].values
        del missing_result_dt_mask

        # label name
        # notes['note_title'] = (notes.lab_name.fillna('').str.strip()
        #                        + '|' + notes.proc_code.fillna('').str.strip()
        #                        + '|' + notes.loinc_code.fillna('').str.strip()).values
        notes['note_title'] = 'Lab Result: ' + notes.lab_name.fillna('').str.strip() + ': ' + notes.ic3_lab_id.astype(str)

        return df[~notes_mask].copy(), notes.drop(columns=['measurement_datetime', 'lab_name', 'proc_code',
                                                           'loinc_code'], errors='ignore')\
            .rename(columns={'result_datetime': 'note_datetime'})

    return df, None
