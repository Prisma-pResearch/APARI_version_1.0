# -*- coding: utf-8 -*-
"""
Created on Mon Apr 26 16:30:15 2021.

@author: ruppert20
"""
import pandas as pd
from typing import Dict
from .Utilities.PreProcessing.data_format_and_manipulation import coalesce
from .Utilities.FileHandling.io import check_load_df
from .Utilities.Logging.log_messages import log_print_email_message as logm
# from Utils.func_utils import debug_inputs


def calculate_charlson_elixhauser_comorbidity_indicies_v2(df: pd.DataFrame, condition_df: pd.DataFrame, scoring_df: pd.DataFrame, reference_points: Dict[str, str] = {'poa': 'visit_start_datetime'}, **logging_kwargs) -> pd.DataFrame:
    
    # add indicator
    condition_df['indicator'] = 1
    
    # adjudicate final name for each sub_component to show in the output
    scoring_df['final_name'] = scoring_df.apply(lambda row: coalesce(row.var_gen_name, row['name']), axis=1)
    
    for l, dc in reference_points.items():
        
        logm(message=f'Generating {l} comorbidity indicies', **logging_kwargs)
        
        # merge condtion df to base df while checking datatypes, temporal filter, and variable filter
        temp = check_load_df(df[['subject_id', dc]].copy(deep=True)\
                                      .merge(condition_df[['subject_id', 'condition_start_date', 'variable_name', 'poa', 'indicator', 'condition_concept_id']],
                                             on='subject_id',
                                             how='left'),
                                      desired_types={'condition_start_date': 'date', dc: 'date', 'poa': 'sparse_int'})\
        .query(f'(condition_start_date < {dc}) | ((condition_start_date <= {dc}) & (poa == "1"))', engine='python')
        
        udn: pd.Series = temp.groupby('subject_id')['condition_concept_id'].nunique().rename('udn').reset_index(drop=False)
        
        temp = temp.drop(columns=[dc, 'condition_start_date', 'poa', 'condition_concept_id'])\
        .groupby(['subject_id', 'variable_name'], as_index=False).first()\
        .merge(scoring_df[['final_name', 'weight', 'lookup_table_name', 'score_name']].rename(columns={'lookup_table_name': 'variable_name'}),
               on='variable_name',
               how='inner')

        # determine final score by taking into consideration the weight
        temp['score'] = None if temp.shape[0] == 0 else temp.apply(lambda row: int(coalesce(row.weight, row.indicator)), axis=1)
        
        # remove mutually exclusive indicators/scores
        for p, r in {'cci_diabwc': 'diabetes',
                     'imcancer': 'icancer',
                     'cci_msld': 'cci_mld'}.items():
            subjects_w_p: pd.Series = temp.loc[(temp.final_name == p) & (temp.score > 0), 'subject_id']
            drop_idx: pd.Series = (temp.final_name == r) & (temp.score > 0) & temp.subject_id.isin(subjects_w_p)
            if drop_idx.any():
                temp = temp[~drop_idx]
        
        # calculate the scores
        score_df = temp[['subject_id', 'score_name', 'score']].copy(deep=True) if temp.shape[0] == 0 else temp.groupby(['subject_id', 'score_name'])['score'].sum().reset_index(drop=False)
        
        # ensure all subjects are represented and pivot to make wide df
        temp_p = df[['subject_id']].merge(udn,
                                          how='left',
                                          on='subject_id')\
            .merge(pd.pivot(temp, index='subject_id', columns='final_name', values='indicator')\
                   .merge(pd.pivot(score_df, index='subject_id', columns='score_name', values='score'),
                          how='left',
                          left_index=True,
                          right_index=True),
                   left_on='subject_id',
                   right_index=True,
                   how='left')\
            .set_index('subject_id')
            
        # ensure all scores and components are represented
        for c in scoring_df.final_name.unique().tolist() + scoring_df.score_name.unique().tolist() + ['udn']:
            if c not in temp_p.columns:
                temp_p[c] = 0 if temp_p.shape[0] > 0 else None
            else:
                temp_p[c].fillna(0, inplace=True)
  
        # set aggregate indicators
        for a, ids in {'liverd': ['cci_mld', 'cci_msld'],
                       'alc_drug': ['eci_alcohol', 'eci_drug'],
                       'anemia': ['eci_blane', 'eci_dane']}.items():
        
            temp_p[a] = temp_p[ids].apply(max, axis=1) if temp_p.shape[0] > 0 else None
       
                
        # add label for temporal filter
        temp_p.columns = [f'{l}_{x.lower()}' for x in temp_p.columns]
        
        # append results
        df = df.merge(temp_p, how='left', left_on='subject_id', right_index=True)
    
    logm(message='Variable Generation Complete', **logging_kwargs)
    
    return df
    

