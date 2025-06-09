# -*- coding: utf-8 -*-
"""
Created on Mon Nov 23 22:51:24 2020.

@author: renyuanfang
@editor: Ruppert20 06/02/2023
"""
import os
import pandas as pd
from .Utilities.FileHandling.io import check_load_df, save_data
from .Utilities.PreProcessing.data_format_and_manipulation import ensure_columns

# aki_codes = ['584', '584.5', '584.6', '584.7', '584.8', '584.9', '997.5', 'N17', 'N17.0',
#              'N17.1', 'N17.2', 'N17.8', 'N17.9', 'N28.9', '593.9']

# ckd_codes = ['N01.8', 'N01.9', 'N01.4', 'N01.5', 'N01.6', 'N01.7', 'N01.0', 'N01.1', 'N01.2',
#              'N01.3', 'E08.2', '582.89', 'E13.2', 'Q61.9', '582.81', 'N07.8', 'N07.9', 'N07.2',
#              'N07.3', 'N07.0', 'N07.1', 'N07.6', 'N07.7', 'N07.4', 'N07.5', 'E08.21', 'E08.22', 'Q61.8', 'E08.29',
#              'N05.8', 'N05.9', 'N05.0', 'N05.1', 'N05.2', 'N05.3', 'N05.4', 'N05.5', 'N05.6', 'N05.7', 'N03.8', 'N03.9',
#              'N03.6', 'N03.7', 'N03.4', 'N03.5', 'N03.2', 'N03.3', 'N03.0', 'N03.1', '586', '403.10', '403.11', 'I13.10',
#              'I13.11', '581.3', '581.2', '581.1', '581.0', '403.01', '581.9', '581.8', '403.00', 'E11.29', 'N25.89',
#              'E11.22', 'E11.21', '583.6', 'N25.81', 'N05', 'N04', 'N07', 'N06', 'N01', 'N03', '583.1', '583.0', '585',
#              '582', '583.4', '583.7', '581', '583.9', '583.8', 'Q61.3', 'Q61.2', 'Q61.5', 'Q61.4', 'I12.9', '588.8',
#              '588.9', 'I12.0', 'N28.0', '585.9', '585.4', '585.3', '585.2', '585.1', '403.91', 'N18.9', '250.43',
#              '250.42', '250.41', '250.40', '404.00', '404.01', '404.02', '404.03', 'N18.2', 'N28', '404.11', '582.4',
#              '582.5', 'N18.1', '582.0', '582.1', '582.2', '582.8', '582.9', 'E10.21', 'E10.22', '583.81', '404.13',
#              '404.12', '588.89', '404.10', 'E10.29', '583.89', '588.81', '581.89', 'I13.2', '581.81', 'I13.0', 'I13.1',
#              'N25', '404.93', '404.92', '404.91', '404.90', 'E09.29', 'E09.22', 'N18.3', 'E09.21', 'N18.4', 'N18.5',
#              'E09.2', 'E11.2', 'N06.7', 'N06.6', 'N06.5', 'N06.4', 'N06.3', 'N06.2', 'N06.1', 'N06.0', 'N02.9', 'N06.9',
#              'N06.8', 'N04.5', 'N04.4', 'N04.7', 'N04.6', 'N04.1', 'N04.0', 'N04.3', 'N04.2', '753.13', 'E13.22',
#              'N04.9', 'N04.8', 'E13.29', '583.2', 'E10.2', 'Q61', 'E13.21', '583', 'N02.8', '403.90',
#              'N25.8', 'N25.9', 'N02.3', 'N02.2', 'N02.1', 'N02.0', 'N02.7', 'N02.6', 'N02.5', 'N02.4']

# dialysis_codes = ['V45.12', 'V56.0', 'V56.8', 'V56.1', 'V56.2', 'V56.32', 'V45.1', 'V45.11', '996.56',
#                   '996.68', '792.5', '39.95', '54.98', 'Z91.15', 'Z49.31', 'Z49.32', 'Z49.01', 'Z49.02',
#                   'Z49.32', 'T85.71XA', 'T85.611A', 'T85.621A', 'R88.0', 'T85.631A', 'T85.71XA', 'T85.71XS',
#                   'Z99.2', 'T85.71XD', 'Z99.2', '5A1D00Z ', '5A1D60Z', '3E1M39Z', '90935', '90937',
#                   '90945', '90947', '90999', '5A1D70Z', '5A1D80Z', '5A1D90Z']

# transplant_codes = ['V42.0', '996.81', '55.6', '55.61', '55.69', 'Z94.0', 'T86.10', 'T86.11',
#                     'T86.12', 'T86.13', 'T86.19', '0TS00ZZ', '0TS10ZZ', '0TY00Z0', '0TY00Z1',
#                     '0TY10Z0', '0TY10Z1', '50360', '50365', '50380']

# esrd_basic_codes = ['585.6', 'V45.1', 'V45.11', 'V45.12', 'N18.6', 'Z91.15', 'Z99.2']
# esrd_dialysis_codes = ['585.5', 'N18.5']
# esrd_dialysis_aki_codes = ['N01.8', 'N01.9', 'N01.4', 'N01.5', 'N01.6', 'N01.7', 'N01.0', 'N01.1', 'N01.2', 'N01.3',
#                            'E08.2', '582.89', 'E13.2', 'Q61.9', '582.81', 'N07.8', 'N07.9', 'N07.2', 'N07.3', 'N07.0',
#                            'N07.1', 'N07.6', 'N07.7', 'N07.4', 'N07.5', 'E08.21', 'E08.22', 'Q61.8', 'E08.29', 'N05.8',
#                            'N05.9', 'N05.0', 'N05.1', 'N05.2', 'N05.3', 'N05.4', 'N05.5', 'N05.6', 'N05.7', 'N03.8',
#                            'N03.9', 'N03.6', 'N03.7', 'N03.4', 'N03.5', 'N03.2', 'N03.3', 'N03.0', 'N03.1', '586',
#                            '403.10', '403.11', 'I13.10', 'I13.11', '581.3', '581.2', '581.1', '581.0', '403.01', '581.9',
#                            '581.8', '403.00', 'E11.29', 'N25.89', 'E11.22', 'E11.21', '583.6', 'N25.81', 'N05', 'N04',
#                            'N07', 'N06', 'N01', 'N03', '583.1', '583.0', '585', '582', '583.4', '583.7', '581', '583.9',
#                            '583.8', 'Q61.3', 'Q61.2', 'Q61.5', 'Q61.4', 'I12.9', '588.8', '588.9', 'I12.0', 'N28.0',
#                            '585.9', '585.4', '585.3', '585.2', '585.1', '403.91', 'N18.9', '250.43', '250.42',
#                            '250.41', '250.40', '404.00', '404.01', '404.02', '404.03', 'N18.2', 'N28', '404.11',
#                            '582.4', '582.5', 'N18.1', '582.0', '582.1', '582.2', '582.8', '582.9', 'E10.21', 'E10.22',
#                            '583.81', '404.13', '404.12', '588.89', '404.10', 'E10.29', '583.89', '588.81', '581.89',
#                            'I13.2', '581.81', 'I13.0', 'I13.1', 'N25', '404.93', '404.92', '404.91', '404.90', 'E09.29',
#                            'E09.22', 'N18.3', 'E09.21', 'N18.4', 'N18.5', 'E09.2', 'E11.2', 'N06.7', 'N06.6', 'N06.5',
#                            'N06.4', 'N06.3', 'N06.2', 'N06.1', 'N06.0', 'N02.9', 'N06.9', 'N06.8', 'N04.5', 'N04.4',
#                            'N04.7', 'N04.6', 'N04.1', 'N04.0', 'N04.3', 'N04.2', '753.13', 'E13.22', 'N04.9', 'N04.8',
#                            'E13.29', '583.2', 'E10.2', 'Q61', 'E13.21', '583', 'N02.8', '403.90', 'N25.8',
#                            'N25.9', 'N02.3', 'N02.2', 'N02.1', 'N02.0', 'N02.7', 'N02.6', 'N02.5', 'N02.4']

def p02_create_admin_flags(inmd_dir: str, eid: str, pid: str, batch: int, **logging_kwargs):
    """
    Create patient history flag, specific history disease flags(aki, ckd, dialysis, kidneyTransplant, esrd) based on the patient history code.

    1. Load filtered encounter, diagnosis, and procedure files in the intermediate file directory as dataframes
    2. Concatenate diagnosis and procedure dataframes, inner join with encounter dataframe using pid, and filter to 3 following dataframe
        * history_codes: filter code_date <= admit_date.
        * procedure_codes: filter code_date <= admit_date and domain_id == 'Procedure'.
        * condition_codes: filter code_date <= admit_date and domain_id in ('Condition', 'Observation').
    3. Use the above 3 dataframes' encounter ids to label 'ConditionFlag', 'ProcedureFlag', and 'finalCodeFlag' flag columns with 1 as indicator of code present and 0 as not present.
    4. Add history disease admin flags for aki, dialysis, renal_transplant, esrd, and ckd using encounter, history_codes dataframes and disease code lists. New columns introduced:
        * condition_code_date: latest code date for specific disease from diagnosis file
        * condition_concept_id: specific disease concept_id from diagnosis file
        * procedure_concept_id: specific procedure concept_id from procedure file
        * procedure_code_date: latest code date for specific disease from procedure file
        * admin_flag: admin flag column with 1 as indicator of code present and 0 as not present in patient disease history.
    5. Adjust CKD admin flag based on esrd_admin_flag. If esrd_admin_flag = 1, adjust ckd_admin_flag to 0 and remove all corresponding code and code_date for both procedure and diagnosis
    6. Save final file to intermediate directory in csv format.

    Parameters
    ----------
        inmd_dir: str
            intermediate file directory
        batch: int
            batch number, default value = 0
        eid: str
            encounter id column name
        pid: str
            patient id column name

    Returns
    -------
    None
    """
    encounter = check_load_df(os.path.join(inmd_dir, 'filtered_encounters', 'filtered_encounters_{}.csv'.format(batch)),
                              desired_types={x: 'datetime' for x in ['admit_datetime', 'visit_start_datetime']},
                              pid=pid, eid=eid, dtype=None, **logging_kwargs)\
        .rename(columns={'visit_start_datetime': 'admit_datetime'})

    diagnosis = check_load_df(os.path.join(inmd_dir, 'filtered_diagnosis', 'filtered_diagnosis_{}.csv'.format(batch)),
                              usecols=[pid, 'start_date', 'condition_concept_id', 'variable_name'], parse_dates=['start_date'],
                              pid=pid, eid=eid, dtype=None, **logging_kwargs)\
        .rename(columns = {'start_date': 'code_date',
                           'condition_concept_id': 'concept_id'})
    diagnosis['domain_id'] = 'Condition'

    procedure = check_load_df(os.path.join(inmd_dir, 'filtered_procedure', 'filtered_procedure_{}.csv'.format(batch)),
                              usecols=[pid, 'procedure_date', 'proc_date', 'procedure_concept_id'],
                              desired_types={x: 'datetime' for x in ['proc_date', 'procedure_start_date']},
                              use_col_intersection=True,
                              pid=pid, eid=eid, dtype=None, **logging_kwargs)\
        .rename(columns={'proc_date': 'code_date',
                         'procedure_concept_id': 'concept_id'})
    procedure['domain_id'] = 'Procedure'

    # stack diagnosis and procedure codes
    codes = pd.concat([diagnosis, procedure], ignore_index=True).drop_duplicates().dropna(subset=['code_date'])
    codes['code_date'] = codes['code_date'].dt.date if codes.shape[0] > 0 else None
    encounter['admit_date'] = encounter['admit_datetime'].dt.date if encounter.shape[0] > 0 else None
    history_codes = codes.merge(encounter, on=pid, how='inner')
    history_codes = history_codes[history_codes['code_date'] <= history_codes['admit_date']]

    # create flags for encounters with and without them
    procedure_codes = history_codes[history_codes['domain_id'].isin(['Procedure'])]
    condition_idx: pd.Series = history_codes['domain_id'].isin(['Condition', 'Observation'])
    condition_codes = history_codes[condition_idx]
    if condition_idx.any():
        history_codes.loc[condition_idx, 'domain_id'] = 'Condition'

    encounter['ConditionFlag'] = 0
    encounter['ProcedureFlag'] = 0
    encounter['finalCodeFlag'] = 0
    encounter.loc[encounter[eid].isin(condition_codes[eid]), 'ConditionFlag'] = 1
    encounter.loc[encounter[eid].isin(procedure_codes[eid]), 'ProcedureFlag'] = 1
    encounter.loc[encounter[eid].isin(history_codes[eid]), 'finalCodeFlag'] = 1

    history_codes = history_codes[[eid, 'admit_date', 'code_date', 'concept_id', 'variable_name', 'domain_id']]
    
    #TODO: Confirm <= for esrd, ckd, and dialysis, while < for aki & renal_transplant
    #TODO: Confirm inclusion of non-ckd codes in CKD category such as  "Diabetes with renal manifestations, type II or unspecified type, not stated as uncontrolled", should those still be used?
    hist_summary = pd.pivot(pd.concat([history_codes[(history_codes.variable_name.isin(['aki', 'renal_transplant'])
                                    &
                                    (history_codes.code_date < history_codes.admit_date))],
                              history_codes[history_codes.variable_name.isin(['dialysis', 'ckd', 'esrd'])]], axis=0, sort=False)\
        .sort_values([eid, 'code_date'], ascending=True)\
        .groupby([eid, 'variable_name', 'domain_id'])\
        .tail(1)\
        .drop(columns=['admit_date']), index=eid, columns=['variable_name', 'domain_id'], values=['code_date', 'concept_id'])
            
    if hist_summary.shape[0] > 0:
    
        # flatten the multi-index
        hist_summary.columns = hist_summary.columns.to_frame().reset_index(drop=True).apply(lambda x: f'{x.iloc[1]}_{x.iloc[2]}_{x.iloc[0]}'.lower(), axis=1).tolist()
    else:
        hist_summary = history_codes.drop(columns=['admit_date'])
    
    # add any missing levels
    hist_summary = ensure_columns(hist_summary, cols=[item for sublist in [[f'{v}_{t}_concept_id', f'{v}_{t}_code_date'] for v, t in zip(['aki', 'ckd', 'dialysis', 'renal_transplant', 'esrd']*2, ['condition']*5+['procedure']*5)] for item in sublist])
    
    # add admin flags
    for c in [x for x in hist_summary.columns if 'procedure_concept_id' in x]:
        hist_summary[c.replace('procedure_concept_id', 'admin_flag')] = (hist_summary[c].notnull() | hist_summary[c.replace('procedure_concept_id', 'condition_concept_id')].notnull()).astype(int)
    
    # sort columns by name for better presentation and replace renal_transplant with kidneyTransplant
    hist_summary = hist_summary[sorted(hist_summary.columns.tolist())].rename(columns={x: x.replace('renal_transplant', 'kidneyTransplant') for x in hist_summary.columns.tolist() if 'renal' in x})
    
    # merge back to encounter dataframe
    encounter = encounter.merge(hist_summary, how='left', on=eid)

    # ensure all flags are filled in
    for c in [x for x in encounter.columns if 'admin_flag' in x]:
        encounter[c].fillna(0, inplace=True)

    # remove CKD flag where ESRD flag is already present
    con = encounter['esrd_admin_flag'] == 1
    encounter.loc[con, 'ckd_admin_flag'] = 0
    for x in ['ckd_procedure_concept_id', 'ckd_procedure_code_date', 'ckd_condition_concept_id', 'ckd_condition_code_date']:
        encounter.loc[con, x] = None

    save_data(df=encounter,
              out_path=os.path.join(inmd_dir,
                                    'encounter_admin_flags',
                                    'encounter_admin_flags_{}.csv'.format(batch)),
              index=False, **logging_kwargs)
    
    
# def p02_create_admin_flags(inmd_dir: str, eid: str, pid: str, batch: int, **logging_kwargs):
#     """
#     Create patient history flag, specific history disease flags(aki, ckd, dialysis, kidneyTransplant, esrd) based on the patient history code.

#     1. Load filtered encounter, diagnosis, and procedure files in the intermediate file directory as dataframes
#     2. Concatenate diagnosis and procedure dataframes, inner join with encounter dataframe using pid, and filter to 3 following dataframe
#         * history_codes: filter code_date <= admit_date.
#         * cpt_codes: filter code_date <= admit_date and code_type = CPT.
#         * diagnose_codes: filter code_date <= admit_date and code_type != CPT.
#     3. Use the above 3 dataframes' encounter ids to label 'ICD9_10flagDiagnosis', 'ICD9_10flagProcedures', and 'finalICD910' flag columns with 1 as indicator of code present and 0 as not present.
#     4. Add history disease admin flags for aki, dialysis, kidneyTransplant, and ckd using encounter, history_codes dataframes and disease code lists. New columns introduced:
#         * disease_code_date: latest code date for specific disease from diagnosis file
#         * disease_code: specific disease code from diagnosis file
#         * disease_icd_type: specific disease icd type from diagnosis file
#         * disease_cpt_code: specific disease cpt code from procedure file
#         * disease_cpt_date: latest code date for specific disease from procedure file
#         * disease_admin_flag: admin flag column with 1 as indicator of code present and 0 as not present in patient disease history.
#     5. Use esrd_basic_codes & esrd_dialysis_codes & esrd_dialysis_aki_codes lists and aki_admin_flag & dialysis_admin_flag columns to
#        locate ESRD condition in both diagnosis and procedure files. New columns introduced:
#         * esrd_code: ESRD code from diagnosis file
#         * esrd_code_date: latest code date for ESRD from diagnosis file
#         * esrd_cpt_code: ESRD code from procedure file
#         * esrd_cpt_date: latest code date for ESRD from procedure file
#         * esrd_admin_flag: admin flag column with 1 as indicator of ESRD present and 0 as not present in patient disease history.
#     6. Adjust CKD admin flag based on esrd_admin_flag. If esrd_admin_flag = 1, adjust ckd_admin_flag to 0 and remove all corresponding code and code_date for both procedure and diagnosis
#     7. Save final file to intermediate directory in csv format.

#     Parameters
#     ----------
#         inmd_dir: str
#             intermediate file directory
#         batch: int
#             batch number, default value = 0
#         eid: str
#             encounter id column name
#         pid: str
#             patient id column name

#     Returns
#     -------
#     None
#     """
#     encounter = check_load_df(os.path.join(inmd_dir, 'filtered_encounters', 'filtered_encounters_{}.csv'.format(batch)),
#                               parse_dates=['admit_datetime', 'visit_start_datetime'],
#                               pid=pid, eid=eid, dtype=None, **logging_kwargs)\
#         .rename(columns={'visit_start_datetime': 'admit_datetime'})

#     diagnosis = check_load_df(os.path.join(inmd_dir, 'filtered_diagnosis', 'filtered_diagnosis_{}.csv'.format(batch)),
#                               usecols=[pid, 'start_date', 'diag_code',
#                                        'diag_icd_type'], parse_dates=['start_date'],
#                               pid=pid, eid=eid, dtype=None, **logging_kwargs)\
#         .rename(columns = {'start_date': 'code_date',
#                            'condition_start_date': 'code_date',
#                            'concept_code': 'code',
#                            'vocabulary_id': 'code_type',
#                            'diag_code': 'code',
#                            'diag_icd_type': 'code_type'})

#     procedure = check_load_df(os.path.join(inmd_dir, 'filtered_procedure', 'filtered_procedure_{}.csv'.format(batch)),
#                               usecols=[pid, 'proc_date', 'procedure_start_date', 'proc_code', 'proc_code_type', 'proc_type'],
#                               desired_types={x: 'datetime' for x in ['proc_date', 'procedure_start_date']},
#                               use_col_intersection=True,
#                               pid=pid, eid=eid, dtype=None, **logging_kwargs)\
#         .rename(columns={'proc_date': 'code_date',
#                          'procedure_start_date': 'code_date',
#                          'procedure_date': 'code_date',
#                          'concept_code': 'code',
#                          'vocabulary_id': 'code_type',
#                          'proc_code': 'code',
#                          'proc_type': 'code_type',
#                          'proc_code_type': 'code_type'})

#     codes = pd.concat([diagnosis, procedure], ignore_index=True).drop_duplicates()
#     codes['code_date'] = codes['code_date'].dt.date if codes.shape[0] > 0 else None
#     encounter['admit_date'] = encounter['admit_datetime'].dt.date if encounter.shape[0] > 0 else None
#     history_codes = codes.merge(encounter, on=pid, how='inner')
#     history_codes = history_codes[history_codes['code_date'] <= history_codes['admit_date']]

#     cpt_codes = history_codes[history_codes['code_type'].isin(['CPT', 'CPT4'])]
#     diagnose_codes = history_codes[history_codes['code_type'].isin(['ICD9CM', 'ICD10CM'])]

#     encounter['ICD9_10flagDiagnosis'] = 0
#     encounter['ICD9_10flagProcedures'] = 0
#     encounter['finalICD910'] = 0
#     encounter.loc[encounter[eid].isin(diagnose_codes[eid]), 'ICD9_10flagDiagnosis'] = 1
#     encounter.loc[encounter[eid].isin(cpt_codes[eid]), 'ICD9_10flagProcedures'] = 1
#     encounter.loc[encounter[eid].isin(history_codes[eid]), 'finalICD910'] = 1

#     history_codes = history_codes[[eid, 'code', 'code_date', 'code_type', 'admit_date']]

#     # add history disease flags
#     for disease, code_list in zip(['aki', 'dialysis', 'kidneyTransplant', 'ckd'], [aki_codes, dialysis_codes, transplant_codes, ckd_codes]):
#         temp_codes = history_codes.copy()
#         if disease in ['aki', 'kidneyTransplant']:
#             temp_codes = temp_codes[temp_codes['code_date'] < temp_codes['admit_date']]
#         temp_codes = temp_codes.drop(columns=['admit_date'])

#         temp_codes.loc[:, disease] = temp_codes.loc[:, 'code'].apply(lambda x: 1 if str(x) in code_list else 0)
#         temp_codes = temp_codes[temp_codes[disease] == 1]

#         temp_diagnoses = temp_codes[temp_codes['code_type'] != 'CPT']
#         temp_procedures = temp_codes[temp_codes['code_type'] == 'CPT'].drop(columns=['code_type'])

#         temp_diagnoses = temp_diagnoses.sort_values([eid, 'code_date']).drop_duplicates([eid], keep='last').drop(columns=[disease])
#         temp_procedures = temp_procedures.sort_values([eid, 'code_date']).drop_duplicates([eid], keep='last').drop(columns=[disease])

#         encounter[disease + '_admin_flag'] = 0
#         encounter.loc[encounter[eid].isin(temp_codes[eid]), disease + '_admin_flag'] = 1
#         encounter = encounter.merge(temp_diagnoses, on=eid, how='left')
#         encounter = encounter.rename(columns={'code': disease + '_code', 'code_date': disease + '_code_date',
#                                               'code_type': disease + '_icd_type'})

#         encounter = encounter.merge(temp_procedures, on=eid, how='left')
#         encounter = encounter.rename(columns={'code': disease + '_cpt_code', 'code_date': disease + '_cpt_date'})

#     """esrd flag"""
#     history_codes = history_codes.drop(columns=['admit_date'])
#     temp_diagnoses = history_codes[history_codes['code_type'] != 'CPT']
#     temp_diagnoses = temp_diagnoses.merge(encounter[[eid, 'aki_admin_flag', 'dialysis_admin_flag']], on=eid, how='left')
#     temp_diagnoses.loc[:, 'flag'] = temp_diagnoses.loc[:, 'code'].apply(lambda x: 1 if str(x) in esrd_basic_codes else 0)
#     con = (temp_diagnoses['dialysis_admin_flag'] == 1) & (temp_diagnoses['flag'] == 0)
#     temp_diagnoses.loc[con, 'flag'] = temp_diagnoses.loc[con, 'code'].apply(lambda x: 1 if str(x) in esrd_dialysis_codes else 0)
#     con = (temp_diagnoses['dialysis_admin_flag'] == 1) & (temp_diagnoses['flag'] == 0) & (temp_diagnoses['aki_admin_flag'] == 0)
#     temp_diagnoses.loc[con, 'flag'] = temp_diagnoses.loc[con, 'code'].apply(lambda x: 1 if str(x) in esrd_dialysis_aki_codes else 0)
#     temp_diagnoses = temp_diagnoses[temp_diagnoses['flag'] == 1].drop(columns=['flag', 'aki_admin_flag', 'dialysis_admin_flag'])
#     temp_diagnoses = temp_diagnoses.sort_values([eid, 'code_date']).drop_duplicates([eid], keep='last')

#     temp_procedures = history_codes[history_codes['code_type'] == 'CPT'].drop(columns=['code_type'])
#     temp_procedures = temp_procedures.merge(encounter[[eid, 'aki_admin_flag', 'dialysis_admin_flag']], on=eid, how='left')
#     temp_procedures.loc[:, 'flag'] = temp_procedures.loc[:, 'code'].apply(lambda x: 1 if str(x) in esrd_basic_codes else 0)
#     con = (temp_procedures['dialysis_admin_flag'] == 1) & (temp_procedures['flag'] == 0)
#     temp_procedures.loc[con, 'flag'] = temp_procedures.loc[con, 'code'].apply(lambda x: 1 if str(x) in esrd_dialysis_codes else 0)
#     con = (temp_procedures['dialysis_admin_flag'] == 1) & (temp_procedures['flag'] == 0) & (temp_procedures['aki_admin_flag'] == 0)
#     temp_procedures.loc[con, 'flag'] = temp_procedures.loc[con, 'code'].apply(lambda x: 1 if str(x) in esrd_dialysis_aki_codes else 0)
#     temp_procedures = temp_procedures[temp_procedures['flag'] == 1].drop(columns=['flag', 'aki_admin_flag', 'dialysis_admin_flag'])
#     temp_procedures = temp_procedures.sort_values([eid, 'code_date']).drop_duplicates([eid], keep='last')

#     encounter['esrd_admin_flag'] = 0
#     encounter = encounter.merge(temp_diagnoses, on=eid, how='left')
#     encounter.loc[encounter['code_date'].notnull(), 'esrd_admin_flag'] = 1
#     encounter = encounter.rename(columns={'code': 'esrd_code', 'code_date': 'esrd_code_date',
#                                           'code_type': 'esrd_icd_type'})

#     encounter = encounter.merge(temp_procedures, on=eid, how='left')
#     encounter.loc[encounter['code_date'].notnull(), 'esrd_admin_flag'] = 1
#     encounter = encounter.rename(columns={'code': 'esrd_cpt_code', 'code_date': 'esrd_cpt_date'})

#     """CKD flag"""
#     con = encounter['esrd_admin_flag'] == 1
#     encounter.loc[con, 'ckd_admin_flag'] = 0
#     for x in ['ckd_code', 'ckd_code_date', 'ckd_icd_type', 'ckd_cpt_code', 'ckd_cpt_date']:
#         encounter.loc[con, x] = np.nan

#     save_data(df=encounter,
#               out_path=os.path.join(inmd_dir,
#                                     'encounter_admin_flags',
#                                     'encounter_admin_flags_{}.csv'.format(batch)),
#               index=False, **logging_kwargs)
