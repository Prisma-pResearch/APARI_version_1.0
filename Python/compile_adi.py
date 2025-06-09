# -*- coding: utf-8 -*-
"""
Created on Fri Jun 16 09:41:58 2023

@author: ruppert20
"""
import pandas as pd
from .Utilities.FileHandling.io import check_load_df

def _compile_adi(source_dir: str) -> pd.DataFrame:
    adi_df: pd.DataFrame = check_load_df('', patterns=['*_ADI_9 Digit Zip Code_v3.1.txt'], regex=False, directory=source_dir,
                                         usecols=["ZIPID","fip_sx","ADI_NATRANK","ADI_STATERNK", "fips", "fip_s", "FIPS.x", "FIPS"], use_col_intersection=True)\
        .rename(columns={'fip_s': 'fips', 'fip_sx': 'fips', 'zipid': 'nine_digit_zip', 'adi_staternk': 'adi_staterank'})\
        .dropna(how='any')
        
    adi_df['nine_digit_zip'] = adi_df['nine_digit_zip'].str.extract(r'([0-9]{9})').iloc[:, 0].astype(int).values
    
    adi_df.drop_duplicates(subset=['nine_digit_zip'], inplace=True)
    
    return check_load_df(adi_df, desired_types={x: 'int' for x in ['adi_staterank', 'adi_natrank', 'fips']})


