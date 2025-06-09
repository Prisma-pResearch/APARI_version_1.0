# -*- coding: utf-8 -*-
"""
Created on Thu Jun 15 10:13:05 2023

@author: ruppert20
"""
import pandas as pd
from typing import Dict, List
from .Utilities.FileHandling.io import check_load_df


def meds_generation_v2(df: pd.DataFrame,
                       meds_df: pd.DataFrame, 
                       intervals: Dict[str, str] = {'pre_admission': 'visit_start_datetime', 'pre_surgery': 'surgery_start_datetime'},
                       meds: List[str] = ['asprin', 'statins', 'AMINOGLYCOSIDES', 'ACEIs_ARBs', 'diuretics', 'nsaids', 'pressors_inotropes',
                                          'OPIOIDS', 'vancomycin', 'beta_blockers', 'antiemetics', 'bicarbonates']) -> pd.DataFrame:
    
    for l, dc in intervals.items():
        
        temp = check_load_df(df[['subject_id', dc]].copy(deep=True)\
                                      .merge(meds_df[['subject_id', 'drug_exposure_start_datetime', 'drug_exposure_end_datetime', 'drug_concept_id', 'variable_name']].query(f'variable_name.isin({meds})', engine='python'),
                                             on='subject_id',
                                             how='left'),
                                      desired_types={'drug_concept_id': 'sparse_int', 'drug_exposure_start_datetime': 'datetime', dc: 'datetime', 'drug_exposure_end_datetime': 'datetime'})
        # filter temporally and get count of unique meds in each category
        temp2 = pd.pivot(temp.loc[(temp.drug_exposure_start_datetime < temp[dc]) & (temp.drug_exposure_end_datetime >= (temp[dc] - pd.to_timedelta('365 Days'))), ['subject_id', 'variable_name', 'drug_concept_id']]\
        .groupby(['subject_id', 'variable_name'])['drug_concept_id'].nunique().reset_index(drop=False),
        index='subject_id', columns='variable_name', values='drug_concept_id')
            
        # ensure all subjects are represented
        temp2 = df[['subject_id']].merge(temp2, how='left', left_on='subject_id', right_index=True).set_index('subject_id')
        
        # ensure all meds are represented and missing values are filled with zero
        for m in meds:
            if m not in temp2.columns:
                temp2[m] = 0 if temp2.shape[0] > 0 else None
            else:
                temp2[m].fillna(0, inplace=True)
                
        # format names based on interval before merge
        temp2.columns = [f'{l}_{x.lower()}' for x in temp2.columns]
        
        # update base
        df = df.merge(temp2, how='left', left_on='subject_id', right_index=True)
        
    return df
        
        
                
        

