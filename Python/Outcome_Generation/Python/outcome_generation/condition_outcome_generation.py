# -*- coding: utf-8 -*-
"""
Created on Wed Jun  7 17:22:06 2023

@author: ruppert20
"""
import re
import pandas as pd
from typing import List, Dict, Union
from ..Utilities.FileHandling.io import check_load_df
from ..Utilities.PreProcessing.data_format_and_manipulation import ensure_columns
# from ..Utilities.General.func_utils import debug_inputs


def condition_outcome_generation(base_df: pd.DataFrame, condition_df: pd.DataFrame, outcomes: Dict[str, List[str]], visit_detail_type: Union[None, str]) -> pd.DataFrame:
    """
    Generate condition outcomes for visits and visit_details.

    Parameters
    ----------
    base_df : pd.DataFrame
        Reference dataframe with visit and visit_detail information.
    condition_df : pd.DataFrame
        Dataframe of condition_occurrences.
    outcomes : Dict[str, List[str]]
        Dictionary with the key being the outcome group and the value being a list of component outcomes.
    visit_detail_type : Union[None, str]
        the type of the visit_detail either "icu", "surgery" or None.

    Returns
    -------
    visit_outcomes : pd.DataFrame
        Condition outcomes

    """
    assert pd.isnull(visit_detail_type) or (str(visit_detail_type) in ['icu', 'surgery']), f'Invalid visit detail type: {visit_detail_type}, please choose one of the following: ["icu", "surgery"]'
    df = check_load_df(
    base_df[['subject_id', 'visit_occurrence_id']].copy(deep=True)
    .merge(condition_df, on='subject_id', how='left'),
    desired_types={'condition_concept_id': 'sparse_int', 'condition_start_date': 'date', 'poa': 'float'}
)

    # ✅ Ensure 'variable_name' exists after merging
    if 'variable_name' not in df.columns:
        print("⚠️ Warning: 'variable_name' is missing after merging. Check condition_df or base_df!")
        print(f"Columns in df: {df.columns.tolist()}")  # Debugging
        raise KeyError("Missing 'variable_name' in merged DataFrame")
    
    # ✅ Avoid dropping 'variable_name' before grouping
    visit_outcomes = pd.pivot(
        df.groupby(['subject_id', 'visit_occurrence_id', 'variable_name'])
        .apply(_agg_visit_conditions)
        .replace({'': None})
        .apply(lambda row: pd.Series({'poa': row.poa,
                                      'poa_flag': 1 if pd.notnull(row.poa) else 0,
                                      'admit_discharge': row.new,
                                      'admit_discharge_flag': 1 if pd.notnull(row.new) else 0,
                                      'admit_discharge_cat_not_poa_flag': 1 if pd.isnull(row.poa) and pd.notnull(row.new) else 0,
                                      'admit_discharge_any_flag': 1 if pd.notnull(row.new) or pd.notnull(row.poa) else 0}), axis=1)
        .reset_index(drop=False),  
        index=['subject_id', 'visit_occurrence_id'], 
        columns=['variable_name']
    )

    # visit_outcomes = pd.pivot(check_load_df(base_df[['subject_id', 'visit_occurrence_id']].copy(deep=True)
    #                                         .merge(condition_df,
    #                                                on='subject_id',
    #                                                how='left'),
    #                                         desired_types={'condition_concept_id': 'sparse_int', 'condition_start_date': 'date', 'poa': 'float'})
    #                           .groupby(['subject_id', 'visit_occurrence_id', 'variable_name'])
    #                           .apply(_agg_visit_conditions).replace({'': None})
    #                           .apply(lambda row: pd.Series({'poa': row.poa,
    #                                                         'poa_flag': 1 if pd.notnull(row.poa) else 0,
    #                                                         'admit_discharge': row.new,
    #                                                         'admit_discharge_flag': 1 if pd.notnull(row.new) else 0,
    #                                                         'admit_discharge_cat_not_poa_flag': 1 if pd.isnull(row.poa) and pd.notnull(row.new) else 0,
    #                                                         'admit_discharge_any_flag': 1 if pd.notnull(row.new) or pd.notnull(row.poa) else 0}), axis=1)
    #                           .reset_index(drop=False),
    #                           index=['subject_id', 'visit_occurrence_id'], columns=['variable_name'])

    if visit_outcomes.shape[0] > 0:

        # flatten the multi-index
        visit_outcomes.columns = visit_outcomes.columns.to_frame().reset_index(drop=True).apply(lambda x: f'{x.iloc[1]}_{x.iloc[0]}'.lower(), axis=1).tolist()

    # add any missing levels
    visit_outcomes = ensure_columns(visit_outcomes, cols=[item for sublist in [[f'{v}_poa', f'{v}_poa_flag', f'{v}_admit_discharge', f'{v}_admit_discharge_flag', f'{v}_admit_discharge_cat_not_poa_flag', f'{v}_admit_discharge_any_flag'] for v in [element for sublist1 in outcomes.values() for element in sublist1]] for item in sublist])

    # make the group flag adjudications
    visit_outcomes = _group_conditions(visit_outcomes, outcome_type='visit_occurrence', outcomes=outcomes).reset_index(drop=False)
    
   
    if isinstance(visit_detail_type, str):
        visit_detail_outcomes = pd.pivot(check_load_df(base_df[['subject_id', 'visit_occurrence_id', 'visit_detail_id', f'{visit_detail_type}_end_datetime']].copy(deep=True)
                                                        .merge(condition_df,
                                                        on='subject_id',
                                                        how='left'),
                                                        desired_types={'condition_concept_id': 'sparse_int', 'condition_start_date': 'date', f'{visit_detail_type}_end_datetime': 'datetime'})
                                          .query(f'condition_start_date > {visit_detail_type}_end_datetime')
                                          .groupby(['subject_id', 'visit_occurrence_id', 'visit_detail_id', 'variable_name'])
                                          .apply(_agg_visit_detail_conditions, visit_detail_type=visit_detail_type).replace({'': None})
                                          .rename(f'{visit_detail_type}_discharge')                                     
                                          .apply(lambda x: pd.Series({f'{visit_detail_type}_discharge': x,
                                                                       f'{visit_detail_type}_discharge_flag': 1 if pd.notnull(x) else 0}))                                    
                                          .reset_index(drop=False),
                                          index=['subject_id', 'visit_occurrence_id', 'visit_detail_id'], columns=['variable_name'])
        
        if visit_detail_outcomes.shape[0] > 0:

            # flatten the multi-index
            visit_detail_outcomes.columns = visit_detail_outcomes.columns.to_frame().reset_index(drop=True).apply(lambda x: f'{x.iloc[1]}_{x.iloc[0]}'.lower(), axis=1).tolist()

        # add any missing levels
        visit_detail_outcomes = ensure_columns(visit_detail_outcomes, cols=[item for sublist in [[f'{v}_{visit_detail_type}_discharge', f'{v}_{visit_detail_type}_discharge_flag'] for v in [element for sublist1 in outcomes.values() for element in sublist1]] for item in sublist])

        # make the group flag adjudications
        visit_detail_outcomes = _group_conditions(visit_detail_outcomes, outcome_type=visit_detail_type, outcomes=outcomes).reset_index(drop=False)

        visit_outcomes = visit_outcomes.merge(visit_detail_outcomes, how='left', on=['subject_id', 'visit_occurrence_id'])

    # fill empty flag columns with zero
    for c in [x for x in visit_outcomes.columns if bool(re.search(r'_flag', x))]:
        visit_outcomes[c].fillna(0, inplace=True)

    return visit_outcomes


def _group_conditions(df: pd.DataFrame, outcomes: Dict[str, list], outcome_type: str) -> pd.DataFrame:
    for k, v in outcomes.items():
        if outcome_type in ['icu', 'surgery']:
            df[f'{k}_{outcome_type}_discharge_flag_overall'] = df[[f'{y}_{outcome_type}_discharge_flag' for y in v]].sum(axis=1, skipna=True)
        else:
            for x in ['poa_flag', 'admit_discharge_flag', 'admit_discharge_cat_not_poa_flag', 'admit_discharge_any_flag']:

                df[f'{k}_{x}_overall'] = df[[f'{y}_{x}' for y in v]].sum(axis=1, skipna=True)

    return df


def _agg_visit_conditions(dfg: pd.DataFrame) -> pd.Series:
    return dfg.groupby('condition_concept_id', as_index=False)\
        .agg({'condition_start_date': 'min', 'poa': 'max', 'condition_concept_id': 'first'})\
        .apply(lambda row: pd.Series({'poa': None if pd.isnull(row.poa) else f'[{row.condition_concept_id}: {row.condition_start_date}]', 'new': None if pd.notnull(row.poa) else f'[{row.condition_concept_id}: {row.condition_start_date}]'}), axis=1)\
        .apply(_stack, axis=0)


def _agg_visit_detail_conditions(dfg: pd.DataFrame, visit_detail_type: str) -> pd.Series:
    return _stack(dfg.groupby('condition_concept_id', as_index=False)
                  .agg({'condition_start_date': 'min', 'condition_concept_id': 'first'})
                  .apply(lambda row: f'[{row.condition_concept_id}: {row.condition_start_date}]', axis=1))


def _stack(series: pd.Series):

    return ','.join(series.dropna())
