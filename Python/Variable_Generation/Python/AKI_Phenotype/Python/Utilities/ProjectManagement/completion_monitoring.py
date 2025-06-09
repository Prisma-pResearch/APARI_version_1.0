# -*- coding: utf-8 -*-
"""
Created on Wed Sep 28 13:36:23 2022.

@author: ruppert20
"""
import pandas as pd
from tqdm import tqdm
import os
from datetime import datetime as dt

# custom modules
from ..FileHandling.io import get_batches_from_directory, detect_file_names, save_data, find_files
from ..Logging.log_messages import log_print_email_message as logm
from ..PreProcessing.data_format_and_manipulation import get_file_name_components
# from ..General.func_utils import debug_inputs


def check_complete(dir_dict: dict,
                   file_type: str = '.csv',
                   batch_list: list = None,
                   search_clean: bool = True,
                   phenotyping_flag: bool = False,
                   split_into_n_batches: int = None,
                   files_list: list = None,
                   raise_exception: bool = True,
                   source_dir_key: str = 'source_dir',
                   dest_dir_key: str = 'final_data',
                   initial_check: bool = False,
                   pre_run_check: bool = False,
                   essential_file_name: str = '^encounters',
                   seperate_procedures_files: bool = True, **logging_kwargs):
    """
    Check if the data processing operation has completed.

    Actions
    -------
    1. compare expected to observed files to see if there were any incomplete files

    Parameters
    ----------
    dir_dict: dict
        dictionary containing at least source_dir and data_dir as keys
    batch_list : list, optional
        list of batches to check. The default is None.
    search_clean : bool, optional
        Wheter to look for clean versions of the file or not. The default is True.
    phenotyping_flag : bool, optional
        Whether to check for expected AKI_CKD Phenotyping output. The default is False.
    split_into_n_batches : int, optional
        number of batches it was split into. The default is None.
    files_list : list, optional
       list of files to check. The default is None.
    raise_exception : bool, optional
        Whether to raise exception. The default is True.
    source_dir_key: str, optional
        The key in the dir_dict for the source directory. The default is 'source_dir'.
    initial_check : bool, optional
        Whether the check will return a dataframe of files to be processed. The default is False.
    pre_run_check : bool, optional
        DESCRIPTION. The default is False.
    essential_file_name : str, optional
        DESCRIPTION. The default is '^encounters'.
    **logging_kwargs : TYPE
        DESCRIPTION.

    Raises
    ------
    Exception
        DESCRIPTION.

    Returns
    -------
    problem_files : pd.dataframe
        problems detected by the check.
    """
    # debug_inputs(function=check_complete, kwargs=locals(), dump_fp='check_complete.p')
    batch_list = get_batches_from_directory(directory=dir_dict.get(source_dir_key),
                                            batches=batch_list, file_name=essential_file_name,
                                            independent_sub_batches=False, file_type=file_type)

    success_m: str = f'{files_list} sucessfully processed'

    if isinstance(files_list, str):
        files_list: list = [files_list]
    elif isinstance(files_list, list):
        pass
    elif (not isinstance(files_list, list)) or (files_list is None):
        files_list: list = detect_file_names(directory=dir_dict.get(source_dir_key), drop_duplicates=True,
                                             pattern=r'_[0-9]+\{}'.format(file_type),
                                             omit_file_names=['crrt', 'admit_discharge_stations', 'stations'])
    else:
        raise Exception(f'Invalid Input. Expecting a list, str, or NoneType, but found a {type(files_list)}')

    def convert_to_clean_names(file_name: str) -> list:
        if initial_check and (file_name in ['blood_pressure', 'respiratory', 'labs', 'meds']):
            baseline: list = [f'{file_name}_to_be_cleaned']
        else:
            baseline: list = []

        if phenotyping_flag:
            return [f'{file_name}_aki_daily',
                    f'{file_name}_aki_summary',
                    f'{file_name}_aki_trajectory',
                    f'{file_name}_ckd_aki_master',
                    f'{file_name}_ckd_noesrd_summary',
                    f'{file_name}_ckd_summary',
                    f'{file_name}_encounter_mort_dischg_place',
                    f'{file_name}_final_aki',
                    f'{file_name}_rrt_summary']
        elif file_name == 'vitals':
            return baseline + ['heart_rate_clean', 'temperature_clean']
        elif file_name == 'labs':
            return baseline + ['labs_clean', 'lab_notes_clean']
        elif file_name == 'meds':
            return baseline + ['meds_clean', 'prescribing_clean']
        elif file_name == 'procedures_icd':
            return baseline + ['procedures_clean']
        elif file_name in ['procedures_cpt', 'billing_accounts']:
            return []

        return baseline + [f'{file_name}_clean']

    if search_clean or phenotyping_flag:
        search_files: list = []
        for f in files_list:
            search_files += convert_to_clean_names(f)
    else:
        search_files: list = files_list

    def process_file_path_into_df(fp: str) -> pd.DataFrame:
        directory, file_name, batch_numbers, file_type, optimized, bs_sep = get_file_name_components(fp)
        df_dict: dict = {'directory': [directory],
                         'file_name': [file_name],
                         'batch_num': [None],
                         'sub_batch': [None],
                         'file_type': [file_type]}
        for i, v in enumerate(batch_numbers):
            if i == 0:
                df_dict['batch_num'] = [v]
            elif i == 1:
                df_dict['sub_batch'] = [v]
            else:
                raise Exception('More than two batch numbers detected')
        return pd.DataFrame(df_dict)

    found_files: list = []
    for f in tqdm(search_files, 'Checking For Incomplete Files'):
        if 'to_be_cleaned' in f:
            try:
                found_files.append(pd.concat([process_file_path_into_df(x) for x in find_files(directory=dir_dict.get('to_be_cleaned'),
                                                                                               patterns=r'^{}.*\{}'.format(f, file_type),
                                                                                               recursive=False, regex=True)],
                                             axis=0))
            except ValueError:
                pass
        else:
            try:
                found_files.append(pd.concat([process_file_path_into_df(x) for x in find_files(directory=dir_dict.get(dest_dir_key),
                                                                                               patterns=r'^{}.*\{}'.format(f, file_type),
                                                                                               recursive=False, regex=True)],
                                             axis=0))
            except ValueError:
                if not pre_run_check:
                    logm(message=f'None of the {f} files were found!', error=True)
                pass

    if len(found_files) > 0:
        found_files: pd.DataFrame = pd.concat(found_files, axis=0)
    else:
        found_files: pd.DataFrame = pd.DataFrame(columns=['directory', 'file_name', 'batch_num', 'sub_batch', 'file_type'])

    if isinstance(split_into_n_batches, int):
        completed: pd.DataFrame = found_files.groupby(['file_name', 'batch_num'], group_keys=False).agg({'file_type': 'count'})\
            .reset_index(drop=False).rename(columns={'file_type': 'count'})
    elif len(get_file_name_components(f+'_'+batch_list[0]).batch_numbers) == 2:
        completed: pd.DataFrame = found_files[['file_name', 'batch_num', 'sub_batch']].copy()
        sep_str: str = get_file_name_components(f+'_'+batch_list[0]).bs_sep
        completed['batch_num'] = (completed['batch_num'].astype(str) + sep_str + completed['sub_batch'].astype(str)).values
        del sep_str
        completed.drop(columns=['sub_batch'], inplace=True)
        completed['count'] = 1
    else:
        completed: pd.DataFrame = found_files[['file_name', 'batch_num']].copy()
        completed['count'] = 1

    completed.loc[:, 'batch_num'] = completed.batch_num.astype(str).values

    if len(batch_list) == 1:
        if batch_list[0] == '':
            completed.loc[:, 'batch_num'] = ''

    for b in batch_list:
        for f in list(set(search_files) - set(completed.query(f'(batch_num  == "{b}") & (file_name in {search_files})').file_name.unique().tolist())):
            completed = completed.append(pd.Series({'file_name': f, 'batch_num': b, 'count': 0}), ignore_index=True)

    expected_count: int = split_into_n_batches if isinstance(split_into_n_batches, int) else 1

    if initial_check:
        completed.file_name.replace({'meds_to_be_cleaned': 'meds_clean',
                                     'prescribing_clean': 'meds_clean',
                                     'labs_to_be_cleaned': 'labs_clean',
                                     'lab_notes_clean': 'labs_clean',
                                     'respiratory_to_be_cleaned': 'respiratory_clean',
                                     'blood_pressure_to_be_cleaned': 'blood_pressure_clean'},
                                    inplace=True)

    completed = completed.groupby(['file_name', 'batch_num'], group_keys=False)['count'].sum().reset_index(drop=False)

    problems: pd.DataFrame = completed.query(f'(count < {expected_count}) & (batch_num in {batch_list})')

    def process_problem_df(df: pd.DataFrame) -> pd.DataFrame:
        out: pd.DataFrame = pd.DataFrame(columns=['file_name', 'batch'])
        for f in problems.file_name.unique().tolist():
            missing_batches: list = list(set(batch_list) - set(completed.query(f'(file_name == "{f}") & (count >= {split_into_n_batches if isinstance(split_into_n_batches, int) else 1})', engine='python').batch_num.dropna().unique().tolist()))
            out = out.append(pd.DataFrame({'file_name': [f.replace('_to_be_cleaned', '')
                                                         .replace('procedures_clean', 'procedures_icd' if seperate_procedures_files else 'procedures_clean')
                                                         .replace('_clean', '')
                                                         .replace('heart_rate', 'vitals')
                                                         .replace('temperature', 'vitals')
                                                         .replace('lab_notes', 'labs')
                                                         .replace('prescribing', 'meds')] * len(missing_batches),
                                           'batch': missing_batches}))
        return out.drop_duplicates()

    if problems.shape[0] > 0:
        if not pre_run_check:
            save_data(found_files, out_path=os.path.join(dir_dict.get('status_files'),
                                                         f'{dt.now().strftime("%Y-%m-%d_%H.%M.%S")}_found_files.csv'),
                      **logging_kwargs)
            
        problems.loc[:, 'deficit'] = expected_count - problems['count'].values
        if pre_run_check:
            logm(f'Proccessing {problems.shape[0]} files', display=True, log_name=logging_kwargs.get('log_name'))
            return process_problem_df(problems).sort_values('batch', ascending=False)

        logm('Prolems Detected with the following files:', error=True)
        logm(problems.to_markdown(index=False), error=True)
        problem_path: str = os.path.join(dir_dict.get('status_files'), f'{dt.now().strftime("%Y-%m-%d_%H.%M.%S")}_problems.csv')
        logm(f'for details check log or {problem_path}', error=True, log_name=logging_kwargs.get('log_name'))
        save_data(problems, out_path=problem_path, **logging_kwargs)
        logm('Process Terminating', raise_exception=raise_exception, log_name=logging_kwargs.get('log_name'))
        return False
    else:
        if pre_run_check:
            return pd.DataFrame(columns=['file_name', 'batch'])

        logm(message=success_m, **logging_kwargs)
        return True
