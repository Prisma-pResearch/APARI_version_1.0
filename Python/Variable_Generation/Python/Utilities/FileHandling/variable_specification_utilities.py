# -*- coding: utf-8 -*-
"""
Module for Handling variables from a variable_specification document.

Created on Wed Jun  7 08:04:22 2023

@author: ruppert20
"""
import os
import pandas as pd
from typing import List, Dict, Union
from .io import check_load_df
from ..PreProcessing.data_format_and_manipulation import coalesce
import warnings


def load_variables_from_var_spec(variables_columns: List[str],
                                 dir_dict: Dict[str, str] = None,
                                 var_spec_key: str = 'variable_file_link',
                                 data_source_key: str = 'source_data',
                                 id_vars: List[str] = ['subject_id', 'person_id', 'visit_occurrence_id', 'visit_detail_id'],
                                 project: Union[str, list] = None,
                                 mute_duplicate_var_warnings: bool = True,
                                 cdm_tables: Union[None, list] = None,
                                 coalesce_fields: dict = None,
                                 allow_empty_returns: bool = False,
                                 filter_variables: bool = True,
                                 aggregation_function_filter: Union[str, None] = None,
                                 paritition_filter: Union[str, int, None] = None,
                                 index_type_filter: Union[str, None] = None,
                                 append_subject_id_type_if_missing: Union[str, None] = None,
                                 **kwargs) -> pd.DataFrame:
    """
    Load Data From Variable Specification File.

    Parameters
    ----------
    variables_columns : List[str]
        The variable_names or column names for data you would like retrieved.
    dir_dict : Dict[str, str], optional
        dictionary with folder paths that contain the folders cotaining the data you would like to load and/or where the linkage file is locatated. The default is None.
    var_spec_key : str, optional
        The file_path or dictionary key to the excel document that links queries with the .csv files. The default is 'variable_file_link'.
    data_source_key : str, optional
        The folder path or dictionary key to the folder containing the downloaded data. The default is 'source_data'.
    id_vars : List[str], optional
        list of id variables to include in the loaded dataframe. The default is ['subject_id', 'person_id', 'visit_occurrence_id', 'visit_detail_id'].
    project : Union[str, list], optional
        The project name filter for the linkage file. The default is None.
    mute_duplicate_var_warnings : bool, optional
        Option to warn when loaded ambiguous variable names. The default is True.
    cdm_tables : Union[None, list], optional
        List of cdm tables from which the data may come from. The default is None.
    coalesce_fields : dict, optional
        Option to coalesce fields from different source files such as measurement and observation into one column. The default is None.
        e.g. {'asa_datetime': ['measurement_datetime', 'observation_datetime']} or {'value': ['value_as_number', 'quantity']}
    allow_empty_returns : bool, optional
        Allows module to return None instead of throwing an error if there were no matching files. The default is False.
    filter_variables : bool, optional
        Allows for the variable_name to be automatically filtered based on the variables_columns parameter when appropriate. The default is True.
    aggregation_function_filter : Union[str, None], optional
        Allows for the filtering of the result based on the aggregation function. The default is None.
        ***NOTE: This must be a string, not a list if used.***
    paritition_filter : Union[str, int, None], optional
        Allows for the filtering of the result based on the parition key: e.g. -1 or 2. The default is None.
        ***NOTE: This must be a string or integer, not a list if used.***
    index_type_filter : Union[str, None], optional
        Allows for the filtering of data based on the index type. The default is None.
        ***NOTE: This must be a string, not a list if used.***
    append_subject_id_type_if_missing: Union[str, None], optional
        The parameter can be used to create a new column with the desired original id column name from the subject id field for use in modules which depend on both the subject id label and the original name for it.
    **kwargs : TYPE
        Keyword arguments to pass onto the check_load_df function.


    Returns
    -------
    output : pd.DataFrame
        DataFrame containg the requested data elements.

    """
    # load the var_spec_df
    assert isinstance(var_spec_key, str), f'The paramter var_spec_key, must be a string; however, a {type(data_source_key)} was passed.'
    if os.path.exists(var_spec_key):
        var_spec_df: pd.DataFrame = check_load_df(var_spec_key, desired_types={'partition_seq': 'sparse_int'})
    else:
        assert isinstance(dir_dict, dict), 'A dir dict is required when the data_source_key is not a filepath to the variable specification document' if pd.isnull(dir_dict) else f'The parameter dir_dict must be of type Dict[str, str]; however, one of type {type(dir_dict)} was found'
        assert isinstance(dir_dict.get(var_spec_key), str), f'The value for the var_spec_key in the dir_dict must be a string; however, a {type(dir_dict.get(var_spec_key))} was found'
        assert os.path.exists(dir_dict.get(var_spec_key)), f'The variable_specification file: {dir_dict.get(var_spec_key)} could not be found!'
        var_spec_df: pd.DataFrame = check_load_df(dir_dict.get(var_spec_key), desired_types={'partition_seq': 'sparse_int'})

    if isinstance(project, (str, list)):
        var_spec_df = var_spec_df[var_spec_df.project.isin([project] if isinstance(project, str) else project)]

    # establish effect name for variables
    var_spec_df['effective_name'] = var_spec_df.apply(lambda row: coalesce(row.result_field_name, row.variable_name, row.cdm_field_name), axis=1)

    # check for variables in multiple rows/files, this is done to handle instance where multiple projects have the same variables, but they do not conflict
    dup_check = var_spec_df[['effective_name', 'file_name']].drop_duplicates()

    if (dup_check.effective_name.duplicated().any()) and (not mute_duplicate_var_warnings):
        warnings.warn(f'There are {dup_check.effective_name.duplicated().sum()} ambiguous names in your variable_specification table. They are {dup_check.effective_name[dup_check.effective_name.duplicated()]}')

    # search for the variable starting with effective name. #TODO: add a more exhaustive search pattern
    found_vars = var_spec_df[var_spec_df.effective_name.isin(variables_columns)].dropna(subset=['file_name'])

    if isinstance(cdm_tables, list):
        found_vars = found_vars[found_vars.cdm_table.isin(cdm_tables)]

    if isinstance(aggregation_function_filter, str):
        if aggregation_function_filter == 'xxxisnullxxx':
            found_vars = found_vars[found_vars.aggregation_function.isnull()]
        else:
            found_vars = found_vars[found_vars.aggregation_function == aggregation_function_filter]

    if isinstance(paritition_filter, (int, str)):
        if paritition_filter == 'xxxisnullxxx':
            found_vars = found_vars[found_vars.partition_seq.isnull()]
        else:
            found_vars = found_vars[found_vars.partition_seq == str(paritition_filter)]

    if isinstance(index_type_filter, (int, str)):
        found_vars = found_vars[found_vars.partition_seq == str(index_type_filter)]

    fields_to_append: List[str] = []

    # check to see if the associated fields are needed to be appended
    for field in found_vars.apply(lambda row: coalesce(row.result_field_name, row.cdm_field_name), axis=1):
        if (field not in variables_columns) and (field not in fields_to_append):
            fields_to_append.append(field)

            if (field == 'value_as_number') and ('unit_concept_id' not in variables_columns) and ('unit_concept_id' not in fields_to_append):
                fields_to_append.append('unit_concept_id')

    # include variable name if necessary
    use_var_name: bool = found_vars.concept_class_id.notnull().any()
    if use_var_name and ('variable_name' not in variables_columns):
        fields_to_append.append('variable_name')

    if found_vars.shape[0] < len(variables_columns):
        warnings.warn(f"Warning...........Unable to located the following variables: {[x for x in variables_columns if x not in found_vars.effective_name]}")

    # check the source folder
    assert isinstance(data_source_key, str), f'The paramter data_source_key, must be a string; however, a {type(data_source_key)} was passed.'
    if os.path.exists(data_source_key):
        assert os.path.isdir(data_source_key), f'The data_source_key is a file_path it must reference a folder; however, a reference to a file was provided. {data_source_key}'
        directory: str = data_source_key
    else:
        assert isinstance(dir_dict.get(data_source_key), str), f'The value for the data_source_key in the dir_dict must be a string; however, a {type(dir_dict.get(data_source_key))} was found'
        assert os.path.exists(dir_dict.get(data_source_key)), f'The data source folder: {dir_dict.get(data_source_key)} could not be found!'
        assert os.path.isdir(dir_dict.get(data_source_key)), f'The value for the key data_source_key in the dir dict must be a reference to a folder; however, a reference to a file was found. {dir_dict.get(data_source_key)}'
        directory: str = dir_dict.get(data_source_key)

    if 'usecols' not in kwargs:
        kwargs['usecols'] = variables_columns + id_vars + fields_to_append
        
    if isinstance(append_subject_id_type_if_missing, str):
        assert append_subject_id_type_if_missing in id_vars, f'Please add {append_subject_id_type_if_missing} to id_vars parameter before proceeding.'
        assert 'subject_id' in id_vars, f'Please add subject_id to id_vars before proceeding.'

    if found_vars.shape[0] == 0:
        if allow_empty_returns:
            return
        else:
            raise Exception('Unable to find files corresponding to the input parameters')

    kwargs['max_workers'] = 1 if kwargs.get('use_dask', False) else kwargs.pop('max_workers', 4)

    try:
        if use_var_name and filter_variables:
            output: pd.DataFrame = pd.concat([check_load_df(r'^{}'.format(x), directory=directory, use_col_intersection=True, **kwargs,
                                                            df_query_str=f'variable_name.isin({variables_columns})') for x in found_vars.file_name.unique()], axis=0)
        else:
            output: pd.DataFrame = pd.concat([check_load_df(r'^{}'.format(x), directory=directory, use_col_intersection=True, **kwargs) for x in found_vars.file_name.unique()], axis=0)
    except ValueError as e:
        print(variables_columns)
        print(found_vars.file_name.unique())
        raise Exception(e)
    
    if isinstance(append_subject_id_type_if_missing, str):
        assert 'subject_id' in output.columns, 'subject_id was missing from the source file'
        if append_subject_id_type_if_missing not in output.columns:
            output[append_subject_id_type_if_missing] = output.subject_id.copy(deep=True)

    if isinstance(coalesce_fields, dict):
        cols_to_drop: List[str] = []
        for k, v in coalesce_fields.items():
            if (output.shape[0] > 0):
                output[k] = output.apply(lambda row: coalesce(*[row[x] for x in v]), axis=1)
            else:
                output[k] = None
            cols_to_drop += v
        output.drop(columns=list(set(cols_to_drop) - set(list(coalesce_fields.keys()))), inplace=True)

    return output
