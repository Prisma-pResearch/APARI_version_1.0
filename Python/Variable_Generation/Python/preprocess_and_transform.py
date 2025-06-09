# -*- coding: utf-8 -*-
"""
Created on Mon Jun  7 14:29:27 2021.

@author: s.miao
"""
import pandas as pd
import os
from .Utilities.PreProcessing.compute_stats import outlier_detection_and_imputation
from .Utilities.Logging.log_messages import log_print_email_message as logm
from .Utilities.FileHandling.io import check_load_df, save_data, get_batches_from_directory, find_files
from .Utilities.ResourceManagement.parallelization_helper import run_function_in_parallel_v2

def harmonize_categories_v2(column: pd.Series, replacement_dict: dict, default_value: any = None, other_value: any = None, **kwargs):
    """
    Standardize Categories according to dictionary and default value.

    Parameters
    ----------
    column : pd.Series
        column to be standardized.
    replacement_dict : dict
        dictionary of values to be replaced by standard.
    default_value : any, optional
        This can be a fixed value or a generator function to replace any values that are not in the replacement dict. The default is None.

    Returns
    -------
    TYPE
        DESCRIPTION.

    """
    
    return column\
        .apply(lambda x: replacement_dict.get(x, (other_value(**kwargs) if (str(type(default_value)) == "<class 'function'>") else other_value) if pd.notnull(x) else default_value(**kwargs) if (str(type(default_value)) == "<class 'function'>") else default_value))
        
def harmonize_categories(column: pd.Series, replacement_dict: dict, default_value: any = None):
    """
    Standardize Categories according to dictionary and default value.

    Parameters
    ----------
    column : pd.Series
        column to be standardized.
    replacement_dict : dict
        dictionary of values to be replaced by standard.
    default_value : any, optional
        This can be a fixed value or a generator function to replace any values that are not in the replacement dict. The default is None.

    Returns
    -------
    TYPE
        DESCRIPTION.

    """
    if 'concept_id' in column.name:
        tp: pd.Series = pd.to_numeric(column, errors='coerce').fillna(0).astype(int)
    else:
        tp: pd.Series = column.str.upper().str.strip()
    return tp\
        .apply(lambda x: replacement_dict.get(x, default_value() if (str(type(default_value)) == "<class 'function'>") else default_value))


def numeric_imputation_v2(input_data: pd.DataFrame, numeric_lookup_table: pd.DataFrame):
    """
    Preprocesse numeric values and imputes outlier values.

    Called in Preprocessing()
    1. Develop list of variables to perform imputation based on z score and percentile: z_imput_list
    2. Develop list of variables to perform imputation which are integer values: round_imput_list
    3. Conduct numerical imputation based on the following criteria:
        a. Convert input feature column to numeric. For any non numeric values, impute with mean value from numeric lookup table.
        b. If input feature is age, adjust age to 103 if any age > 103 value exists.
        c. If input feature is BMI, adjust BMI to median value if any BMI is not in range of 14 to 60.
        d. If input feature in z_imput_list, compute z for value and if z value > 3.5 and value not in
           1-99 percentile in numerical lookup table: impute by drawing a number from a uniform distribution
           Note: using modified z-score and enhance previous z-score calculation if MAD is zero.
        e. If input feature in round_imput_list, round the input data so that the final value is a integer.
    4. If any value in imputed feature are still NaN, raise an error.

    Parameters
    ----------
        dr_data: pandas.Dataframe
            Variable generated data. Input dataframe may include following columns:
                * eGFR
                * ratio_ref_Cr_mdrd (not in OneFL)
                * age
                * cci
                * NEPHROTOXIC
                * NUM_MEDS
                * BMI
                * columns with _min, _max, _var, _avg, _count
        numeric_lookup_table: pandas.Dataframe
            Numerical lookup table.
                Must include following columns:
                    * Feature Names (e.g. age)
                Must include the following indexes:
                    * median
                    * median_dev
                    * 0.05
                    * 0.005
                    * 0.01
                    * 0.95
                    * 0.995
                    * 0.99

    Returns
    -------
    pandas.DataFrame
        Dataframe with numeric imputations based on the numeric lookup table
    """
    data = input_data.copy(deep=True)

    # check for and add missing columns from feature list
    for col in numeric_lookup_table.columns.difference(data.columns).tolist():
        data[col] = None

    for col in numeric_lookup_table.columns.tolist():
        data.loc[:, col] = outlier_detection_and_imputation(vect=data[col], imputation_series=numeric_lookup_table[col],
                                                            rounded_vars=["age", "cci", "nephrotoxic", "num_meds"])

    return data


def categorical_imputation_v2(data: pd.DataFrame, categorical_lookup_table: pd.DataFrame):
    """
    Transform categorical variables based on categorical lookup table.

    1. Create cat_lookup dictionary using categorical lookup table in the format of {Feature: {Category: Value}}
    2. Create misscat_lookup dictionary for categorical values that is not in the categorical lookup table in the format of {Feature: Mean(Value)}
    3. First use cat_lookup to transform if values does not exist labelled as NA, then use misscat_lookup to fill in NAs using mean value.

    Parameters
    ----------
    data : pd.DataFrame
        Generated dataframe.
    categorical_lookup_table : pd.DataFrame
        Categorical lookup table. Must contains the following columns:
            * Feature
            * Category
            * Value

    Returns
    -------
    df : pd.DataFrame
        Datframe with categorical columns data transformed based on categorical lookup table.

    """
    df = data.copy()
    # create lookup table for categorical variables
    cat_lookup = {}
    for i in categorical_lookup_table.itertuples():
        feature = i[1]
        category = i[2]
        value = i[3]
        cat_lookup.setdefault(feature, {})[category] = value

    # create lookup table for values not in categorical lookup table, using the mean to fill in those missing value
    misscat_lookup = {}
    for i in categorical_lookup_table.groupby(['Feature'])['Value'].mean().reset_index(name='mean_value').itertuples():
        misscat_lookup[i.Feature] = i.mean_value

    # transform values using lookup tables
    for i in df.columns:
        df[i] = df[i].astype(str)
        if i in cat_lookup.keys():
            df[i] = df[i].map(cat_lookup[i])

    df = df.fillna(misscat_lookup)

    return df


def assign_value_to_new_cpt(cpt_code: str,
                            CPTtree: dict,
                            final_class_cpt: dict,
                            missing_value: str = 'missing'):
    """
    Assign numerical value to input CPT.

    Called in cpt_imputation()
    1. If CPT is missing, impute with final_class_cpt["ALL"].
    2. If CPT does not exist in CPTtree, impute with final_class_cpt["ALL"] unless final_class_cpt["unassigned CPT's"] exists.
    3. If CPT exists in CPTtree, replace CPT value with corresponding numerical value in final_class_cpt.

    Parameters
    ----------
    cpt_code : str
        CPT code.
    CPTtree : dict
        CPT dictionary.
    final_class_cpt : dict
        CPT numerical dictionary.
    missing_value : str, optional
        Missing value fomat. The default is 'missing'.

    Returns
    -------
    float
        Numerical CPT value.

    """
    if cpt_code == missing_value:
        return final_class_cpt["ALL"]

    if cpt_code not in CPTtree:
        if "unassigned CPT's" in final_class_cpt:
            return final_class_cpt["unassigned CPT's"]

        else:
            return final_class_cpt["ALL"]

    ind = CPTtree[cpt_code]

    if 'CPT:' + cpt_code in final_class_cpt:
        return final_class_cpt['CPT:' + cpt_code]

    if 'Group:' + str(ind[0]) + ':' + str(ind[1]) in final_class_cpt:
        return final_class_cpt['Group:' + str(ind[0]) + ':' + str(ind[1])]

    if 'CCS:' + str(ind[0]) in final_class_cpt:
        return final_class_cpt['CCS:' + str(ind[0])]

    return final_class_cpt["ALL"]


def cpt_imputation(data_t: pd.DataFrame, cpt_lookup: pd.DataFrame, CPTtree: dict) -> pd.DataFrame:
    """
    Relates values of CPT features with their numeric representation.

    Function called in Preprocessing().
    1. Convert primary_proc to string.
    2. Apply assign_value_to_new_cpt function and generate numerical representation of each cpt.

    Parameters
    ----------
        data_t: pandas.DataFrame
            Dataframe containing categorical features to be converted
            Input dataframe must include the following columns:
                * primary_proc
        cpt_lookup: pandas.Dataframe
            Dataframe with numeric equivalents of CPT codes
            Input dataframe must include the following columns:
                * ALL
                * unassigned_CPT's
        CPTtree: dict
            Dictionary containing CPT codes and their conditional probabilites with the outcome
    Returns
    -------
    pandas.DataFrame
    """
    data = data_t.copy(deep=True)
    data["primary_proc"] = data["primary_proc"].astype(str)
    data["primary_proc"] = data["primary_proc"].apply(lambda x: assign_value_to_new_cpt(x, CPTtree, cpt_lookup))
    return data


def data_preprocessing_v2(generated_data_df: pd.DataFrame,
                          non_aki_numeric_lookup_table: pd.DataFrame,
                          aki_numeric_lookup_table: pd.DataFrame) -> dict:
    """
    Call numerical_imputation_v2 function to conduct numerical imputation of input dataframe based on numeircal lookup tables and return a dictionary of dataframes based of whether complication is AKI or not.

    Parameters
    ----------
    generated_data_df : pd.DataFrame
        Generated variables data.
    non_aki_numeric_lookup_table : pd.DataFrame
        Numeric lookup table for non aki complications.
    aki_numeric_lookup_table : pd.DataFrame
        Numeric lookup table for aki.

    Returns
    -------
    dict
        Dictionary contains AKI complication and nonAKI complication numerical imputed dataframe.

    """
    return {'nonAKIcomplications': numeric_imputation_v2(input_data=generated_data_df.copy(), numeric_lookup_table=non_aki_numeric_lookup_table),
            'AKIcomplications': numeric_imputation_v2(input_data=generated_data_df.copy(), numeric_lookup_table=aki_numeric_lookup_table)}


def data_transforming_v2(CPTtree: pd.DataFrame,
                         feature_list: pd.DataFrame,
                         preprocessed_data: dict,
                         lookup_tables: dict,
                         project_name: str = None,
                         out_dir: str = None,
                         batch_num: int = None,
                         return_dict: bool = False,
                         return_list: bool = False,
                         **logging_kwargs) -> dict:
    """
    Transform data dynamically.

    1. Perform numerical imputation
    2. Perform categorica and cpt imputation
    3. Optionally save output to file vs return dictionary of output

    Parameters
    ----------
    CPTtree : dict
        CPT master dictionary.
    feature_list : pd.DataFrame
        Feature list dataframe. Must contains:
            * feature_name
            * feature_type
    preprocessed_data : dict
        Dictionary containing two dataframes with the keys 'AKIcomplications' and 'nonAKIcomplications'
    lookup_tables : dict
        Dictionary with the following structure {'complication name': {'cpt': cpt_df: pd.DataFrame,
                                                                       'cat': cat_df: pd.DataFrame},
                                                 'complication name 2': {'cpt': cpt_df: pd.DataFrame,
                                                                         'cat': cat_df: pd.DataFrame}}
    project_name : str, optional
        Project name to be included in filename, by default None
    out_dir : str, optional
        Directory to save result, by default None
    batch_num: int, optional
        The batch number for the files, by default None
    return_dict : bool, optional
        [description], by default False

    Returns
    -------
    dict
        Dictionary with transformed data fro each complication. With the following structure {'complication name': pd.DataFrame(),
                                                                                              'complication name 2': pd.DataFrame()}
    """
    out: dict = {}

    for complication, lookup_dict in lookup_tables.items():

        out[complication] = cpt_imputation(categorical_imputation_v2(preprocessed_data['AKIcomplications' if 'AKI_overall' in complication.upper() else 'nonAKIcomplications'].copy(),
                                                                     lookup_dict['cat']),
                                           lookup_dict['cpt'], CPTtree.copy())

        if isinstance(out_dir, str) and isinstance(project_name, str):
            save_data(df=out[complication],
                      out_path=os.path.join(out_dir,
                                            f'{project_name}_{complication}_Transformed_data_chunk_{batch_num}.csv'),
                      **logging_kwargs)

    if return_dict:
        return out

    if return_list:
        return list(out.values())


def transform_data_v2(project_name: str,
                      reference_dir: str,
                      status_dir: str,
                      out_dir: str,
                      CPTtree: dict,
                      lookup_tables: dict,
                      complications: list,
                      all_generated_variables: pd.DataFrame,
                      feature_list: pd.DataFrame,
                      aki_numeric_lookup_table: pd.DataFrame,
                      non_aki_numeric_lookup_table: pd.DataFrame,
                      batch_num: str,
                      return_dict: bool = False,
                      **logging_kwargs):
    """
    Systematically generate transform data for each complication.

    1. Generate success file path and proceed if only success file path does not exist
    2. Load feature list
    3. If complication is 'AKI_Overall' apply data_preprocessing_v2 function to generate preprocessed_data.
    4. If complication is not 'AKI_Overall', preprocessed_data is {'nonAKIcomplications': pd.DataFrame()}
    5. Write success fiel path in out directory

    Parameters
    ----------
    lookup_table_dir : str
        Directory of lookup table.
    project_name : str
        Project name.
    reference_dir : str
        Reference lookup table directory.
    out_dir : str
        Directory to save final output files.
    lookup_basis : str
        lookup table specific name.
    complications : list
        List of complications.
    all_generated_variables : pd.DataFrame
        Dataframe consists of generated variables. OneFL should have 400 variables.
    feature_list : pd.DataFrame
        Feature list. Must contains the following columns:
            * feature_name
            * feature_type
    batch_num : str
        batch_id.
    return_dict : bool, optional
        Indicator to check if returned result should be in dictionary format. The default is False.

    Returns
    -------
    dict
        Dictionary with transformed data fro each complication. With the following structure {'complication name': pd.DataFrame(),
                                                                                              'complication name 2': pd.DataFrame()}

    """
    sucess_fp: str = os.path.join(status_dir,
                                  f'{project_name}_batch_num_variable_transformation_success')

    if os.path.exists(sucess_fp):
        logm(message=f'variable transformation complete {batch_num}',
             **logging_kwargs)
        return

    # load feature list
    feature_list.feature_name = feature_list.feature_name.astype(str).str.lower()

    if 'aki_overall' in [i.lower() for i in complications]:
        # transform data
        out: dict = data_transforming_v2(CPTtree=CPTtree,
                                         feature_list=feature_list,
                                         preprocessed_data=data_preprocessing_v2(generated_data_df=all_generated_variables.reset_index(drop=True),

                                                                                 non_aki_numeric_lookup_table=non_aki_numeric_lookup_table,
                                                                                 aki_numeric_lookup_table=aki_numeric_lookup_table),
                                         lookup_tables=lookup_tables,
                                         project_name=project_name,
                                         out_dir=out_dir,
                                         batch_num=batch_num,
                                         return_dict=return_dict,
                                         **logging_kwargs)

    else:
        # transform data
        out: dict = data_transforming_v2(CPTtree=CPTtree,
                                         feature_list=feature_list,
                                         preprocessed_data={'nonAKIcomplications': numeric_imputation_v2(input_data=all_generated_variables.reset_index(drop=True),
                                                                                                         numeric_lookup_table=non_aki_numeric_lookup_table)},
                                         lookup_tables=lookup_tables,
                                         project_name=project_name,
                                         out_dir=out_dir,
                                         batch_num=batch_num,
                                         return_dict=return_dict,
                                         **logging_kwargs)

    return out


def transform_data_for_batches(lookup_table_dir: str,
                               cohorts: list,
                               status_dir: str,
                               source_dir: str,
                               out_dir: str,
                               CPTtree: dict,
                               lookup_basis: str,
                               complications: list,
                               feature_list: pd.DataFrame,
                               serial: bool = False,
                               **logging_kwargs):
    """
    Generate Variables for folder or batch list.

    Parameters
    ----------
    pid : str
        DESCRIPTION.
    eid : str
        DESCRIPTION.
    dir_dict : dict
        DESCRIPTION.
    independent_sub_batches : bool
        DESCRIPTION.
    source_type : str
        DESCRIPTION.
    real_time : bool, optional
        DESCRIPTION. The default is False.
    sched_surg_start_col : str, optional
        DESCRIPTION. The default is 'sched_start_datetime'.
    hospital : str, optional
        DESCRIPTION. The default is 'both'.
    default_zip : int, optional
        DESCRIPTION. The default is 32610.
    batches : list, optional
        DESCRIPTION. The default is None.

    Returns
    -------
    None.

    """
    batches: list = get_batches_from_directory(directory=source_dir,
                                               batches=batches, file_name='^encounters_clean',
                                               independent_sub_batches=True)
    kwargs_list: list = []
    for batch in batches:
        for c in cohorts:
            success_path: str = os.path.join(status_dir, f'preop_transformed_data_{batch}_{c}_success')
            if not os.path.exists(success_path):
                kwargs_list.append({'non_aki_numeric_lookup_table': check_load_df(os.path.join(lookup_table_dir,
                                                                                               f"numeric_lookup_table_NonAKI_outcomes_from_{lookup_basis}.csv")),
                                    'aki_numeric_lookup_table': check_load_df(os.path.join(lookup_table_dir,
                                                                                           f"numeric_lookup_table_AKI_from_{lookup_basis}.csv")),
                                    'CPTtree': CPTtree,
                                    'lookup_tables': {x: {'cpt': check_load_df(os.path.join(lookup_table_dir, f"{lookup_basis}_CPT_lookup_table_{x}.p")),
                                                          'cat': check_load_df(os.path.join(lookup_table_dir, f"categorical_lookup_table_{x}_from_{lookup_basis}.csv"))} for x in complications},
                                    'display': logging_kwargs.get('display', False),
                                    'project_name': c,
                                    'status_dir': status_dir,
                                    'all_generated_variables': check_load_df(os.path.join(source_dir, f'all_generated_variables_{c}_chunk_{batch}.csv')),
                                    'out_dir': out_dir,
                                    'complications': complications,
                                    'return_dict': False,
                                    'feature_list': feature_list,
                                    'batch_num': batch,
                                    'log_name': f'IDEALIST_PREOP_VARIABLE_TRANSFORMATION_BATCH_{batch}'})

    run_function_in_parallel_v2(transform_data_v2,
                                kwargs_list=kwargs_list,
                                max_workers=min(20, os.cpu_count()),
                                update_interval=10,
                                disp_updates=True,
                                log_name='IDEALIST_PREOP_VARIABLE_TRANSFORMATION',
                                list_running_futures=True,
                                debug=serial)

    assert len(find_files(directory=out_dir, patterns=[r'[0-9_A-z-]+_Transformed_data_chunk_[0-9_A-z-]+\.csv'], regex=True)
               ) == len(batches) * 2, 'IDEALIST_PREOP_VARIABLE_TRANSFORMATION did not complete successfully'
