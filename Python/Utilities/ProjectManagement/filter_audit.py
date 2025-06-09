# -*- coding: utf-8 -*-
"""
Created on Mon Jan  6 13:44:48 2025

@author: diehu
"""

import pandas as pd

file_path=r'S:\2016_223 IDEALIST\4 PROJECTS\ACTIVE\9 Care Analysis Project Tyler\Dee\Active\APARI_2024_DATA_V2\procedure_occurrence_test\Data\Audit'



apari_list = pd.read_excel(f'{file_path}\\apari_master_variable_filter_list.xlsx')  # Ensure file name is correct
audit_result = pd.read_excel(f'{file_path}\\final_report.xlsx')   # Ensure file name is correct


duplicates = apari_list[apari_list.duplicated()]
apari_list.columns
audit_result.columns


apari_list = apari_list.drop_duplicates()
audit_result = audit_result.drop_duplicates()

filtered_audit_result = audit_result.merge(
    apari_list,
    on=['cdm_table', 'cdm_field_name', 'variable_name'],
    how='inner'
).sort_values(by=['cdm_table',  'variable_name',]).reset_index(drop=True)

# filtered_audit_result_sorted = filtered_audit_result.sort_values(
#     by=['cdm_table',  'variable_name', 'data_type']
# )


filtered_audit_result.to_excel( f'{file_path}\\filtered_final_report_v2.xlsx', index=False)

