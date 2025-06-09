# -*- coding: utf-8 -*-
"""
Created on Wed May 10 19:53:11 2023

@author: ruppert20
"""

import check_load_df
import pandas as pd


lookup_table: pd.DataFrame = check_load_df('''SELECT [concept_id]
      ,[concept_code]
      ,[concept_name]
      ,[vocabulary_id]
      ,[domain]
      ,[variable_name]
  FROM [IDEALIST].[dbo].[IC3_Variable_Lookup_Table]''',
  engine=engine)


duplicated = lookup_table[lookup_table.concept_id.duplicated(keep=False)].query('domain != "Drug"').sort_values(['domain', 'concept_id', ])
  
  