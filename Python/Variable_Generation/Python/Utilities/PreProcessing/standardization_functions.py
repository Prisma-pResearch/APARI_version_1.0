# -*- coding: utf-8 -*-
"""
Module to standardize input dataframes in a consistent and reproducible manner.

Created on Tue Dec 28 14:15:25 2021.

@author: ruppert20
"""
from typing import Union  #, List, Dict
import re
import os
import numpy as np
import pandas as pd
import random
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import LabelEncoder
from ..Logging.log_messages import log_print_email_message as logm
from joblib import dump, load
from scipy.stats import median_abs_deviation as mad
import copy
import json
from ..FileHandling.io import load_data, save_data, get_file_name_components, check_format_series, get_column_names, detect_file_names, check_load_df, get_batches_from_directory
from ..General.func_utils import get_func, convert_func_to_string  # , debug_inputs
from ..ResourceManagement.parallelization_helper import run_function_in_parallel_v2
from ..PreProcessing.aggregation_functions import _numeric_aggregators, _default_non_numeric_agg
# from .data_format_and_manipulation import sanatize_columns, notnull, remove_illegal_characters, deduplicate_and_join
from tqdm import tqdm
try:
    from pytorch_lightning import seed_everything
    pyt_seed: bool = True
except ImportError or ModuleNotFoundError:
    pyt_seed: bool = False
import datetime as dt
tqdm.pandas()
# from sklearn.experimental import enable_iterative_imputer
# from sklearn.impute import IterativeImputer


def process_df_v2(df: pd.DataFrame,
                  instruction_fp: str,
                  training_run: bool,
                  master_config_dict: dict = {},
                  encoder_dir: str = None,
                  train_ids: list = None,
                  id_index: str = None,
                  use_existing_instructions: bool = True,
                  index_cols: list = [],
                  time_index_col: str = None,
                  default_na_values: list = ['missing', 'unavailable', 'not available', 'unknown',
                                             'abnormal_value', 'no egfr', 'no reference creatinine'],
                  default_missing_value_numeric: str = 'xxxmedian_mad_normal_randomxxx',
                  default_missing_value_binary: str = 'xxxweighted_randomxxx',
                  default_other_value_binary: int = None,
                  default_missing_value_cat: str = 'unknown',
                  default_other_value_cat: str = 'other',
                  default_case_standardization: str = 'lower',
                  default_min_num_cat_levels: int = 5,
                  default_one_hot_embedding_threshold: int = 5,
                  default_missingness_threshold: float = 0.5,
                  default_lower_limit_percentile: float = 0.01,
                  default_scale_values: bool = True,
                  default_upper_limit_percentile: float = 0.99,
                  default_fill_lower_upper_bound_percentile: float = 0.15,
                  default_fill_upper_lower_bound_percentile: float = 0.85,
                  default_ensure_col: bool = False,
                  debug_column: str = None,
                  seperate_by_type: bool = False,
                  stacked_meas_name: str = 'measurement_name',
                  stacked_meas_value: str = 'measurement_value',
                  start: Union[pd.Series, pd.DataFrame, pd.Timestamp] = None,
                  end: Union[pd.Series, pd.DataFrame, pd.Timestamp] = None,
                  time_bin: str = '4H',
                  interpolation_method: str = 'linear',
                  interpolation_limit_direction: str = 'forward',
                  interpolation_limit: int = None,
                  interpolation_limit_area: str = None,
                  resample_origin: str = 'start',
                  resample_label: str = 'left',
                  resample_fillna_val: any = None,
                  resample_agg_func: Union[callable, str] = None,
                  default_dtype: str = None,
                  pre_resample_default_dtype: str = None,
                  random_seed: int = 42,
                  **logging_kwargs) -> Union[dict, tuple, pd.DataFrame]:
    """
    Clean/standardize/resample data.

    Training Run Actions
    -------
        1. Create specification template to execute procedures from default kwargs and master_config_dict.
        2. Determine source data type
        3. Perform case standardization and whitespace removal
        4. Apply standardization dictionary (if provided in master_config_dict)
        5. Infer column dtype if no output_dtype specification is provided in the master_config_dict
        6. Coerce data to to the specified or inferred data type
        7. Calculate Train and overall Missigness
        8. Compare train missingness to defined threshold
            if a time_index col is specified the missigness check is deffered until after resampling/aggregation,
                otherwise the missigness is compared to the allowed threshold "missingness_threshold" and if that value is exceeded the column is dropped
                and annoted as such in the instructions for future runs.
            **Note**: this can be overwritten via the ensure_col parameter in the master config dict
        9. Check if there are atleast two different values in the column and drop if there is only one.
            **Note**: this can be overwritten via the ensure_col parameter in the master config dict
        10. Calculate stats respective to each datatype:
            * 'cat_one_hot', 'cat_embedding', 'binary', and 'cat_str':
                calculate distinct levels as well as there value counts in the training set and overall
                calculate mode
            * 'int' and 'float':
                calculate:
                        1. Mean
                        2. Median
                        3. Mode
                        4. Standard Deviation
                        5. Median Absolute Deviation
                        6. Min
                        7. Max
                        8. lower limit percentile (see parameters for additional details)
                        9. Upper limit percentile (see parameters for additional details)
                        10. Upper Lower bound percentile (see parameters for additional details)
                        11. Lower Upper bound percentile(see parameters for additional details)
        11. Perform Type specific Checks:
            * int and float:
                1. remove values above upper limit or lower limit and replace with a random value between
                lower_limit and upper_lower_limit and lower_upper_limit and upper_limit respectively
            * 'cat_one_hot' and 'cat_embedding'
                1. remove values the occur less than the "default_min_num_cat_levels" theshold and replace with
                    the value specified by "default_other_value_cat"
                2. remove unobserved levels that are not in the training set and replace with "default_other_value_cat"
        12. Perform imputation based on specification (see parameter documenation for details)
            ****NOTE****: Imputation is deffered until after resampling/aggregation if a time_index column is provided
        13. Scale Numeric Values to mean zero unit variance using traning data as a basis to train a Standard Scaler.
        14. Aggregate statistics/finding from the processing phase into the processing template
        ####### Time Series only steps 15-19, for static variables skip to step 20  #######
        15. Add "start" and/or "end" timepoints with NULL values (if they are provided)
            **Note**: A common use to this would be to mark the admission/discharge of a hospital encounter/ICU stay or the start/stop of a surgery
        16. Resample the data to the specified freqency "time_bin" and resampling parameters below
        17. Aggregate the data in each time bin as specifed in the master_config_dict or default method based on the datatype (see parameter documentation for additional details)
        18. Interopate/fill forward based on the interpolation parameters below
        19. Repeat steps 1 through 14 using the resampled data
        ###### End Time Series Only ######
        20. Write the instruction specification to file along with the master_config_dict to a YAML file.
        21. Return output
            if time_index_col
                Tuple of three dataframes
                    out (final output), out_post_resample(output post resampling before final processing), out_pre_resample (standardized output prior to resampling)
            else:
                out (final output)
            **Note**: The final output can be split by type using the "seperate_by_type" parameter which will return a dictionary of dataframes with the data type as the key


    Parameters
    ----------
    df: pd.DataFrame, required*
        Pandas DataFrame to be processed.
    instruction_fp: str, required*
        File path to a .csv file with instructions on how to process the data.
        *NOTE* This file will be created if one does not exist.
    training_run: bool, required*
        Whether or not instructions should be created or should be used.
    master_config_dict: dict, optional
        Column level specification for any of the parameters for standardization or resampling. The default is {}.
        The template is as follows:
            {column_name/stacked_name_level: {
                                                'pre_resample_output_dtype': str (used to force a dtype prior to resampling if different then the final outputdtype)
                                                 ###########  START DTYPE OPTIONS:  ###########
                                                     * 'str': (passes through with case standardization, but no level calculations or value checks **Not recomended**)
                                                     * 'object': (see string)
                                                     * 'int': (forces coercsion to int and has full support for all functionality)
                                                     * 'float': (forces coercsion to float and has full support for all functionality)
                                                     * 'cat_one_hot': (treats as a category and will be converted to a series of one_hot encoded columns and has full support for all functionality)
                                                     * 'cat_embedding': (treats as a category that will be converted to a categorical embedding and has full support for all functionality)
                                                     * 'binary': (coerces to an int 0 or 1 and has full support for all funcationality)
                                                     * 'datetime': (forces coersion to datetime, but no level calculations or value checks **Not Recommended**)
                                                     * 'timestamp' (forces coersion to datetime, but no level calculations or value checks **Not Recommended**)
                                                     * 'cat_str': (treats as a categorical variable except observed value and min level checks are not performed.
                                                                   Usefull when a resampling function splits complex strings into seperate parts.)
                                                 ###########  END DTYPE OPTIONS:  ###########
                                                 'output_dtype': str (used to force a final output dtype, see above for options)
                                                 'drop_column': bool (used to manuall drop a column)
                                                 'ensure_col': bool (used to force the inclusion of a column in the final output)
                                                 'standardization_values': dict (dictionary with source levels as keys and desired replacements as keys.
                                                                                 A common use is to convert sex to binary e.g. {'male': 0, 'female': 1})
                                                 'na_values': (list of values to be considered null
                                                               ***NOTE***: this will override the default_na_values parameter)
                                                 'case_standardization': str (force custom case standardiation, see default_case_standardiation for options)
                                                 'missing_value': Union[float, int, str] (force custom default missing value, see default missing value parameters for options
                                                                                          **NOTE**: overrides whatever the default missing value is for the desired output_dtype.)
                                                 'other_value': Union[str, int] (force custom other value, see other value parameters for options
                                                                                 NOTE: overrides whatever the default other value is for the desired output_dtype)
                                                 'missingness_threshold': float [0, 1] (custom missingness threshold for this particular column)
                                                 'min_categorical_count': int (custom threshold for minimum level count, overrides default_min_num_cat_levels)
                                                 'lower_limit_percentile': float [0,1] (custom lower limit threshold, see parameter documentation for details)
                                                 'upper_limit_percentile': float [0,1] (custom upper limit threshold, see parameter documentation for details)
                                                 'fill_lower_upper_bound_percentile': float [0,1] (custom lower upper threshold, see parameter documentation for details)
                                                 'fill_upper_lower_bound_percentile': float [0,1] (custom upper lower threshold, see parameter documentation for details)
                                                 'scale_values': bool (force numeric values to be kept as source or scaled)
                                                 'one_hot_embedding_threshold': int (custom theshold for differentatng between one_hot encoding and categorical embedding)
                                                 ###########  Time Series Resampling OPTIONS:  ###########
                                                 'ds_dtype': str (custom resampling column dtype, NOTE: only used in resampling **NOT Recommended**)
                                                 'function': Union[str, callable] (custom function to be used for time bin aggregation)
                                                 'agg_names': List[str] (custom list of column names derived from the source column)
                                                 'interpolation_limit_direction': str (custom interpolation limit direction, see parameter documentaiton for details)
                                                 'interpolation_limit_area': str (custom interpolation limit area, see parameter documentaiton for details)
                                                 'interpolation_method': str (custom interpolation method, see parameter documentaiton for details)
                                                 'limit': str (custom interpolation limit, see parameter documentaiton for details)
                                                 'fillna_val': str (custom fill na value, see parameter documentaiton for details)

        Static Example:
            {'sex': {'standardization_values': {'male': 0, 'female': 1}, 'output_dtype': 'binary'},
            'ethnicity': {'standardization_values': {'non-hispanic': 0, 'hispanic': 1}, 'output_dtype': 'binary'},
            'language': {'standardization_values': {'english': 0, 'non-english': 1}, 'output_dtype': 'binary'},
            'sched_trauma_room': {'standardization_values': {'n': 0, 'y': 1}, 'output_dtype': 'binary'},
            'admission_source': {'standardization_values': {'non-transfer': 0, 'transfer': 1}, 'output_dtype': 'binary'},
            'emergent': {'standardization_values': {'non-emergency': 0, 'emergency': 1}, 'output_dtype': 'binary'},
            'postop_loc': {'standardization_values': {'non-icu': 0, 'icu': 1}, 'output_dtype': 'binary'},
            'anesthesia_type': {'standardization_values': {'general': 0, 'local/regional': 1}, 'output_dtype': 'binary'},
            'attend_doc': {'output_dtype': 'cat_embedding'},
            'sched_start_time': {'output_dtype': 'datetime'},
            'admit_year': {'output_dtype': 'cat_embedding'},
            'admit_hour': {'output_dtype': 'cat_embedding'},
            'zip': {'output_dtype': 'cat_embedding'},
            'bmi': {'output_dtype': 'float'},
            'primary_proc': {'output_dtype': 'cat_embedding'},
            'total': {'output_dtype': 'float'},
            'distance_from_shands': {'drop_column': True},
            'median_income': {'output_dtype': 'float'},
            'perc_below_poverty': {'output_dtype': 'float'},
            'prop_black': {'output_dtype': 'float'},
            'prop_hisp': {'output_dtype': 'float'},
            'distance_to_facility': {'output_dtype': 'float'}}
        Time Series Example:
            {
                "weight_kgs": {
                    "function": "Utils.aggregation_functions._numeric_aggregators"
                },
                "location": {
                    "function": "Utils.aggregation_functions._worst_station_agg",
                    "interpolation_method": "ffill"
                },
                "device_type": {
                    "pre_resample_output_dtype": "cat_str",
                    "function": "Utils.aggregation_functions._worst_resp_device_agg"
                },
                "CAM": {
                    "pre_resample_output_dtype": "cat_str",
                    "function": "Utils.aggregation_functions._worst_cam_agg",
                    "interpolation_method": "ffill",
                    "missing_value": "NEGATIVE"
                },
                "activity": {
                    "pre_resample_output_dtype": "str",
                    "function": "Utils.aggregation_functions._best_activity",
                    "interpolation_method": "ffill"
                },
                "creatinine": {
                    "pre_resample_output_dtype": "float"
                },
                "aki": {
                    "pre_resample_output_dtype": "cat_str",
                    "function": "Utils.aggregation_functions._worst_aki_binarization_agg",
                    "interpolation_method": "ffill"
                },
                "ckd": {
                    "pre_resample_output_dtype": "str",
                    "function": "Utils.aggregation_functions._worst_ckd",
                    "interpolation_method": "ffill"
                },
                "arterial_catheter": {
                    "function": "Utils.aggregation_functions._default_non_numeric_agg",
                    "fillna_val": 0,
                    "ensure_col": true
                }
            }
    encoder_dir: str, optional
        Folder used to store standard scalars and categorical encoders. The default is None.
        *NOTE*: This is required if encoders are being used.
    train_ids: list, optional
        List of ids to be considered part of the training dataset. The default is None.
        *NOTE* If no ids are provided and it is a training run, it is assumed the entire dataframe contains training data.
    id_index: str, optional
        Name of series (can be in the index or in the columns) that contains the id labels used to distinguish between train and other cohorts. The default is None.
    use_existing_instructions: bool, optional
        Whether to use existing instructions output by prior runs of this function using the specified instruction_fp. The default is True.
        **Note** When training this will automatically be set to false.
    index_cols: list, optional
        List of columns to be passed through the standardization module without modification. The default is [].
        **Note**: You do not need to list the ID or time columns here if you already provided them in the id_index or time_index_col parameters.
    time_index_col: str, optional
        Time index column for time series data. The default is None, which assumes there is not one.
    default_na_values: list, optional
        List of values which should be considered NULL. The default is ['missing', 'unavailable', 'not available', 'unknown',
                                                                        'abnormal_value', 'no egfr', 'no reference creatinine'].
    default_missing_value_numeric: str, optional
        How missing numeric values should be handled unless otherwise specified in the master config dict. The default is 'xxxmedian_mad_normal_randomxxx'.
        **Note**: The values ncecesarry to complete these imputaiton operations are computed variable wise and saved in the configuration dictionary.
        Available options:
            * 'xxxmean_std_normal_randomxxx':
                Fill missing values using values from a normal distribution matching the training data.

            * 'xxxmedian_mad_normal_randomxxx':
                Fill missing values using values from a normal distribution matching the training data using the Median in place of the mean
                    and the Median Absolute Deviation inplace of the standard deviation.

            * 'xxxuniform_randomxxx':
                Fill missing values using values from a uniform distribution from the min/max values of the training data

            * 'xxxmodexxx':
                Fill missing values using mode of the traning data.

            * 'xxxmeanxxx':
                Fill missing values using mean of the training data.

            * 'xxxmedianxxx':
                Fill missing Values with the median of the training data.
    default_missing_value_binary: str, optional
        How missing binary values will be imputed unless specified in the master config dict. The default is 'xxxweighted_randomxxx'.

        Available Options:
            * 'xxxmodexxx':
                Fill missing values using mode of the training data.
            * 'xxxotherxxx':
                Fill missing values using the value specified in the "other_value_binary" or "default_other_value_binary" parameters
                    from the master config dict or kwargs respectively.
                **Note**: if this value was not present in the training set, it will be replaced with a weighted random sample based on the training set.
                            You will see a warning about this in the Logs.

            * 'xxxuniform_randomxxx'
                Fill missing values using a uniform random sample from the distinct levels observed traning data.
            * 'xxxweighted_randomxxx':
                Fill missing values using a weighted random sample from the distinct levels observed traning data.
            * 0 (fill all other values with zero)
            * 1 (fill all other values with one)

    default_other_value_binary: int, optional
        Values not in the standardization dict for a binary column will be replaced with this value. The default is None which will treat the unoberserved value as missing.
        **Note**: if this value is provided with a standardization dict. Any Non Null value that is not in the standardization_dict will be filled with this value.

    default_missing_value_cat: str, optional
        Missing Categorical variables will be filled with this value. The default is 'unknown', which fill fill the missing rows with the string 'unknown'.

        The special missing value options for binary values ['xxxweighted_randomxxx', 'xxxuniform_randomxxx', 'xxxmodexxx', 'xxxotherxxx'] are also available here.

        **Note**: if there are no missing values in the training data, 'xxxweighted_randomxxx' will be used by default unless otherwise specified.
                Additionally, the program will not allow a value to be filled that is not in the traning data. This is expected behavior.
    default_other_value_cat: str, optional
        Fill levels with a count less than the specified threshold in the training set with this value as well as unobserved values outside the training set.
        The defaul is 'other'. Note: this will default to xxxweighted_randomxxx' if there is no other values in the tranining set.

    default_case_standardization: str, optional
        Convert all strings to the specified case. The default is 'lower'.
        The options are:
            * 'lower' (convert to all lowercase)
            * 'upper' (convert to all upppercase)
            * '' (leave existing capitalization)
            * 'capitalize' (convert to sentence case)

    default_min_num_cat_levels: int, optional
        The minumum number of observed levels in the training set for it to be kept. Levels with less than this number will be replaced with "default_other_value_cat".
        The default is 5.

    default_one_hot_embedding_threshold: int, optional
        The maximum number of levels a one_hot_encoded categorical variable is allowed to have before it is converted to a categorical embedding. The defaul is 5.

    default_missingness_threshold: float, optional
        The maxium amount of missigness allowed in the training set for a given variable. This is ignored in all other datasets. The default is 0.5.

    default_lower_limit_percentile: float, optional
        The lower cutoff for extreme values to be clipped and replaced by values between this threshold and the "default_fill_lower_upper_bound_percentile".
        The default is 0.01.

    default_scale_values: bool, optional
        Whether numeric values (int, float) should be scaled with mean zero, unit variance. The default is True.

    default_upper_limit_percentile: float, optional
        The upper cutoff for extreme values to be clipped and replaced by values between the "default_fill_upper_lower_bound_percentile" and this threshold.
        The default is  0.99.

    default_fill_lower_upper_bound_percentile: float, optional
        The Upper end of the lower fill range used to replace values more extreme than the percentil defined by "default_lower_limit_percentile"
        The default is 0.15.

    default_fill_upper_lower_bound_percentile: float, optional
        The lower end of the upper fill range used to replace values more extreme than the percentile defined by "default_upper_limit_percentile".
        The default is 0.85.

    default_ensure_col: bool, optional
        Force all columns to be kept by default (e.g. ignore the missigness threshold and atleast two different value thresholds).
        The default is False, which will subject all columns, unless otherwise specified in the master_config_dict to the aforementioned contstraints.

    debug_column: str, optional.
        A column or level in a larger dataframe to be ran on exclusively for the purpose of debugging.
        The default is None.
        **Note**: this should be considered a beta feature.

    seperate_by_type: bool, optional
        Whether or not to seperate output columns by their output_dtype. The defaul is False which will return them all as one wide dataframe.

    stacked_meas_name: str, optional
        Column name which contains multiple different types of observations. This is to be used with stacked data. The default is 'measurement_name'.
        **Note**: if this column is present, any data not in the measure name or measure value will be ignored.

    stacked_meas_value: str, optional
        Column name which contains multiple different types of data corresponding to the different observation type labels in "stacked_meas_name".
        This is to be used with stacked data. The default is 'measurement_value'.
        **Note**: if this column is present, any data not in the measure name or measure value will be ignored.

    start : Union[pd.Series, pd.DataFrame, pd.Timestamp, None]
        first timestamp to ensure is in the series.
        **NOTE**:
                * a Timesamp is only accepted if there is only one ID.

                * a pd.Series will need to have the id_col as the index.

                * a pd.DataFrame will need to have the following columns [id_col, 'start']
    end : Union[pd.Series, pd.DataFrame, pd.Timestamp, None]
        last timestamp to ensure is in the series.
        **NOTE**:
                * a Timesamp is only accepted if there is only one ID.

                * a pd.Series will need to have the id_col as the index.

                * a pd.DataFrame will need to have the following columns [id_col, 'end']
    time_bin : str, optional
        time window to bin data into. The default is '4H'.

    interpolation_method : str, optional
        Method used to interpolate the data. The default is 'linear'.

        *‘linear’: Ignore the index and treat the values as equally spaced. This is the only method supported on MultiIndexes.
        *‘time’: Works on daily and higher resolution data to interpolate given length of interval.
        *‘index’, ‘values’: use the actual numerical values of the index.
        *‘pad’: Fill in NaNs using existing values.
        *‘nearest’, ‘zero’, ‘slinear’, ‘quadratic’, ‘cubic’, ‘spline’, ‘barycentric’, ‘polynomial’: Passed to scipy.interpolate.interp1d.
            These methods use the numerical values of the index. Both ‘polynomial’ and ‘spline’ require that you also specify an order (int), e.g. df.interpolate(method='polynomial', order=5).
        *‘krogh’, ‘piecewise_polynomial’, ‘spline’, ‘pchip’, ‘akima’, ‘cubicspline’: Wrappers around the SciPy interpolation methods of similar names. See Notes.
        *‘from_derivatives’: Refers to scipy.interpolate.BPoly.from_derivatives which replaces ‘piecewise_polynomial’ interpolation method in scipy 0.18.

    interpolation_limit_direction : str, optional [‘forward’, ‘backward’, ‘both’, None]
        Consecutive NaNs will be filled in this direction. The default is 'forward'.

        If limit is specified:
            If ‘method’ is ‘pad’ or ‘ffill’, ‘limit_direction’ must be ‘forward’.
            If ‘method’ is ‘backfill’ or ‘bfill’, ‘limit_direction’ must be ‘backwards’.

        If ‘limit’ is not specified:
            If ‘method’ is ‘backfill’ or ‘bfill’, the default is ‘backward’
            else the default is ‘forward’

    interpolation_limit : int, optional
        Maximum number of consecutive NaNs to fill. Must be greater than 0 if specified. The default is None.

    interpolation_limit_area : str, optional [None, ‘inside’, ‘outside’], default is None.
        If limit is specified, consecutive NaNs will be filled with this restriction.
            *None: No fill restriction.
            *‘inside’: Only fill NaNs surrounded by valid values (interpolate).
            *‘outside’: Only fill NaNs outside valid values (extrapolate).

    resample_origin: str, optional [‘epoch’, ‘start’, ‘start_day’, ‘end’, ‘end_day’], default is 'start'
        *‘epoch’: origin is 1970-01-01
        *‘start’: origin is the first value of the timeseries
        *‘start_day’: origin is the first day at midnight of the timeseries
        *‘end’: origin is the last value of the timeseries
        *‘end_day’: origin is the ceiling midnight of the last day

    resample_label: str, optional ['left', 'right']
        Which bin edge label to label bucket with. The default is 'left'.

    id_col: str, optional
        Column or index name that distinguishes encounters/patients. The default is None.

    label_col: str, optional
        The column in the multi-index which may be used as a column label if so desired.
        This is used with long tables not with wide tables. The default is None.

    dt_index: str, optional
        The datetime column/index column to be used for resampling/interpolation. The default is None, which will infer the column automatically.

    resample_fillna_val: any, optional
        The value to fill any missing values after aggregation. The default is None, which will attempt to use interpolation.

    ds_dtype: str, optional
        Can be used to force a specific datatype for resampling. The default is None.
        The use case for this would generally be when only the count of observations is important as apposed to the actual numbers for a numeric variable.

    resample_agg_func: Union[callable, str], optional
        Function to be used to aggregate data within each time bin.
        This may either be a function itself or a string referncing a specific module e.g. "Utils.aggregation_functions._default_non_numeric_agg".
        The default is None, which will use a numeric aggregator on numeric vars (extracts min, max, median, mean, etc.) or non-numeric agg which extracts count.

    default_dtype : str, optional
        Default data type to coerce all variables into. The default is None.

    pre_resample_default_dtype : str, optional
        Deafult datatype to coerce all varialbes into prior to resampling. The default is None.
        **NOTE**: This is only applicable to time series variables.

    **logging_kwargs : TYPE
        kwargs to be passed to the log_print_email_message funciton in the Utils.log_messages module.

    Returns
    -------
    pd.DataFrame or tuple of DataFrames.

    """
    # debug_inputs(function=process_df_v2, dump_fp='proc_v2.p', kwargs=locals())
    assert isinstance(df, pd.DataFrame), f'df must be a pandas dataframe, but a {type(df)} was found!'
    assert isinstance(master_config_dict, dict) or (master_config_dict is None), 'master_config_dict must be of type dict. empty dictionaires are permitted'
    assert sum([seperate_by_type]) < 2, '''Both return_type_dict and seperate_by_type are specified; however this function only supports returning one at a time.
    Please revise your function call accordingly.
    ***Note*** this information can be derived from the transformation_instruction_fp.'''

    use_existing_instructions: bool = not training_run

    index_cols: list = index_cols or []
    master_config_dict: dict = master_config_dict or {}

    process_df_log_kwargs: dict = copy.deepcopy(logging_kwargs)
    process_df_log_kwargs['log_name'] = re.sub(r'^\.', '', process_df_log_kwargs.get('log_name', '') + '.process_df')

    file_type: str = get_file_name_components(instruction_fp).file_type

    # ensure a unique index
    if df.index.nunique() < df.index.shape[0]:
        df.reset_index(drop=True, inplace=True)

    training_df: pd.DataFrame = None
    if use_existing_instructions:
        if os.path.exists(instruction_fp):
            training_df: pd.DataFrame = load_data(instruction_fp, dtype=None)
            training_df = training_df.where(pd.notnull(training_df), None)
            time_index_col: str = training_df.query('output_dtype == "time_index"').column_name.iloc[0] if training_df.output_dtype.isin(['time_index']).any() else None
            index_cols: list = training_df.query('(output_dtype == "index_column") & ~column_name.str.contains("missing_ind")', engine='python').column_name.tolist()
            id_index: str = training_df.query('output_dtype == "id_index"').column_name.iloc[0] if training_df.output_dtype.isin(['id_index']).any() else None
            training_df: pd.DataFrame = training_df.set_index('column_name')
            seperate_by_type: bool = training_df.loc['process_df_parameters', 'seperate_by_type']
            stacked_meas_name: str = training_df.loc['process_df_parameters', 'stacked_meas_name']
            stacked_meas_value: str = training_df.loc['process_df_parameters', 'stacked_meas_value']
            random_seed: str = int(training_df.loc['process_df_parameters', 'random_seed'])
            training_df.reset_index(drop=False, inplace=True)

    # set random seed for psuedorandom generators
    if pyt_seed:
        seed_everything(random_seed, workers=True)
    else:
        random.seed(random_seed)
        np.random.seed(random_seed)

    # ensure index columns, time_index, id_col are all present
    required_cols: list = pd.Series([time_index_col, id_index] + df.columns.intersection(index_cols).tolist(),
                                    dtype=object).dropna().unique().tolist()
    if len(required_cols) > 0:
        dropable_cols: list = df.columns.intersection(pd.Series([time_index_col, id_index],
                                                                dtype=object).dropna().unique().tolist()).tolist()
        if len(dropable_cols) > 0:
            df.dropna(subset=dropable_cols, how='any', inplace=True)

    if (time_index_col is None) and (stacked_meas_name in df.columns):
        try:
            df = df.pivot(index=required_cols, columns=stacked_meas_name, values=stacked_meas_value).reset_index(drop=False)
        except ValueError as e:
            if 'duplicate' in str(e):
                logm(message='Duplicate indexes found in DataFrame. You might have a time_index_col that was not specified. Please either add a time_index_col or de-duplicate the index',
                     error=True, raise_exception=True)
                # TODO: Potential enhacement would be the ability to average simultaneous observations before pivot if resampling is not desired.

    out, amended_training_df = _run_process(training_df=training_df,
                                            training_run=training_run,
                                            df=df,
                                            stacked_meas_value=stacked_meas_value,
                                            stacked_meas_name=stacked_meas_name,
                                            skip_imputation=isinstance(time_index_col, str),  # wait to impute missing until after resampling
                                            skip_encoding_scaling=isinstance(time_index_col, str),  # wait to encode/scale until after resampling
                                            skip_clip=False,  # do not alter clipping behavior here
                                            required_cols=required_cols,
                                            default_ensure_col=default_ensure_col,
                                            debug_column=debug_column,
                                            id_index=id_index,
                                            default_na_values=default_na_values,
                                            default_missing_value_numeric=default_missing_value_numeric,
                                            default_missing_value_binary=default_missing_value_binary,
                                            default_other_value_binary=default_other_value_binary,
                                            default_missing_value_cat=default_missing_value_cat,
                                            default_other_value_cat=default_other_value_cat,
                                            default_case_standardization=default_case_standardization,
                                            default_min_num_cat_levels=default_min_num_cat_levels,
                                            default_one_hot_embedding_threshold=default_one_hot_embedding_threshold,
                                            default_missingness_threshold=default_missingness_threshold,
                                            default_lower_limit_percentile=default_lower_limit_percentile,
                                            default_scale_values=default_scale_values,
                                            default_upper_limit_percentile=default_upper_limit_percentile,
                                            default_fill_lower_upper_bound_percentile=default_fill_lower_upper_bound_percentile,
                                            default_fill_upper_lower_bound_percentile=default_fill_upper_lower_bound_percentile,
                                            encoder_dir=encoder_dir,
                                            master_config_dict=master_config_dict,
                                            default_dtype=pre_resample_default_dtype,
                                            train_ids=train_ids,
                                            time_index_col=time_index_col,
                                            pre_resample=True,
                                            generate_missing_indicators=not isinstance(time_index_col, str),
                                            file_type=file_type,
                                            **process_df_log_kwargs)

    amended_training_df['pre_resample'] = None

    amended_training_df = amended_training_df.set_index('column_name')

    # save paramters
    for k, v in {'seperate_by_type': seperate_by_type,
                 'stacked_meas_name': stacked_meas_name,
                 'stacked_meas_value': stacked_meas_value,
                 'random_seed': random_seed,
                 'output_dtype': 'parameter'}.items():
        amended_training_df.loc['process_df_parameters', k] = v

    amended_training_df.reset_index(drop=False, inplace=True)

    if training_run:
        save_data(df=amended_training_df, out_path=instruction_fp.replace(file_type, f'_pre_resampling{file_type}' if isinstance(time_index_col, str) else file_type))
    else:
        amended_training_df = training_df.copy()

    if isinstance(time_index_col, str):
        out_pre_resample: pd.DataFrame = out.copy(deep=True)
        amended_training_df_pre_resample: pd.DataFrame = amended_training_df.copy(deep=True)
        amended_training_df_pre_resample['pre_resample'] = '1'

        amended_training_df_pre_resample = amended_training_df_pre_resample.set_index('column_name')

        if training_run:
            # save paramters
            for k, v in {'time_bin': time_bin,
                         'resample_origin': resample_origin,
                         'resample_label': resample_label}.items():
                amended_training_df_pre_resample.loc['process_df_parameters', k] = v
        else:
            time_bin: str = amended_training_df_pre_resample.loc['process_df_parameters', 'time_bin']
            resample_origin: str = amended_training_df_pre_resample.loc['process_df_parameters', 'resample_origin']
            resample_label: str = amended_training_df_pre_resample.loc['process_df_parameters', 'resample_label']

        amended_training_df_pre_resample.reset_index(drop=False, inplace=True)

        out.loc[:, time_index_col] = check_format_series(out.loc[:, time_index_col], desired_type='datetime')

        if id_index == out.index.name:
            out.reset_index(inplace=True, drop=False)

        out.set_index(out.columns.intersection(list(set(required_cols + [stacked_meas_name]))).tolist(),
                      inplace=True)

        list_sep: str = 'XXXXSEPXXXX'

        # update master_config
        for idx, row in amended_training_df_pre_resample.copy(deep=True).iterrows():  # col in [x for x in out.columns if 'missing_ind' not in x]:
            if ('missing_ind' in row.column_name) or (row.output_dtype in ['time_index', 'id_index', 'index_column']):
                continue

            td: dict = master_config_dict.get(row.column_name, {})

            # set dtype
            td['ds_dtype'] = row.output_dtype

            # set resampling function
            td['function'] = row.resampling_func if not training_run\
                else (td.get('function') or resample_agg_func) or (_numeric_aggregators if td['ds_dtype'] in ['int', 'float', 'binary'] else _default_non_numeric_agg)
            if (not isinstance(td['function'], str)) and pd.notnull(td['function']):
                td['function'] = convert_func_to_string(td['function'])
            if training_run:
                amended_training_df_pre_resample.loc[idx, 'resampling_func'] = td['function']

            # set agg names
            if (training_run and ('agg_names' in td)) or pd.notnull(getattr(row, 'agg_names', None)):
                td['agg_names'] = row.agg_names if not training_run else td.get('agg_names')
                if training_run:
                    amended_training_df_pre_resample.loc[idx, 'agg_names'] = list_sep.join(td['agg_names']) if td['agg_names'] is not None else None
                else:
                    td['agg_names'] = td['agg_names'].split(list_sep) if td['agg_names'] is not None else None

            # set interpolation limits
            td['interpolation_limit_direction'] = row.interpolation_limit_direction if not training_run\
                else td.get('interpolation_limit_direction') or interpolation_limit_direction
            td['interpolation_limit_area'] = row.interpolation_limit_area if not training_run\
                else td.get('interpolation_limit_area') or interpolation_limit_area
            td['interpolation_method'] = row.interpolation_method if not training_run\
                else td.get('interpolation_method') or interpolation_method
            td['limit'] = row.interpolation_limit if not training_run\
                else td.get('limit') or interpolation_limit
            td['fillna_val'] = row.fillna_val if not training_run\
                else td.get('fillna_val') or resample_fillna_val

            if training_run:  # save configuration to instruction df
                for atr in ['interpolation_limit_direction', 'fillna_val', 'limit', 'interpolation_method', 'interpolation_limit_area']:
                    amended_training_df_pre_resample.loc[idx, re.sub(r'^limit$', 'interpolation_limit', atr)] = td[atr]

            # update master config
            master_config_dict[row.column_name] = {k: None if pd.isnull(v) else v for k, v in td.items()}

        out = resample_pandas(data_structure=out.drop(columns=[x for x in out.columns if 'missing_ind' in x], errors='ignore'),
                              start=start,
                              end=end,
                              time_bin=time_bin,
                              custom_agg_dict=master_config_dict,
                              interpolation_method=interpolation_method,
                              interpolation_limit_direction=interpolation_limit_direction,
                              interpolation_limit=interpolation_limit,
                              interpolation_limit_area=interpolation_limit_area,
                              resample_origin=resample_origin,
                              resample_label=resample_label,
                              id_col=id_index,
                              label_col=stacked_meas_name if str(stacked_meas_name) in out.index.names else None,
                              dt_index=time_index_col,
                              fillna_val=resample_fillna_val,
                              ds_dtype=None,
                              value_col=stacked_meas_value if str(stacked_meas_value) in out.index.names else None,
                              agg_func=resample_agg_func,
                              **logging_kwargs)
        out_post_resample: pd.DataFrame = out.copy(deep=True)

        # return out, input_kwargs, amended_training_df_pre_resample

        # if traning_run:
        #     for col in input_kwargs.colums.intersection
        #     amended_training_df_pre_resample['pre_resample'] = '1'

        # return out, input_kwargs

        # fill missing counts with zero and create missing_ind columns
        count_cols: list = [x for x in out.columns if bool(re.search(r'_count$', x))]
        missing_ind_cols: list = [re.sub(r'_count$', '_missing_ind', x) for x in count_cols]
        out.drop(columns=missing_ind_cols, errors='ignore', inplace=True)
        for c, m in zip(count_cols, missing_ind_cols):
            out.loc[:, c].fillna(0).astype(int)
            out[m] = (out[c] == 0).astype(int)

        out, amended_training_df = _run_process(training_df=training_df,
                                                training_run=training_run,
                                                df=out,
                                                stacked_meas_value=stacked_meas_value,
                                                stacked_meas_name=stacked_meas_name,
                                                skip_imputation=False,  # wait to impute missing until after resampling
                                                skip_encoding_scaling=False,  # wait to encode/scale until after resampling
                                                skip_clip=True,  # do not alter clipping behavior here
                                                required_cols=required_cols + missing_ind_cols,
                                                debug_column=debug_column,
                                                id_index=id_index,
                                                default_na_values=default_na_values,
                                                default_missing_value_numeric=default_missing_value_numeric,
                                                default_missing_value_binary=default_missing_value_binary,
                                                default_other_value_binary=default_other_value_binary,
                                                default_missing_value_cat=default_missing_value_cat,
                                                default_other_value_cat=default_other_value_cat,
                                                default_case_standardization=default_case_standardization,
                                                default_min_num_cat_levels=default_min_num_cat_levels,
                                                default_one_hot_embedding_threshold=default_one_hot_embedding_threshold,
                                                default_missingness_threshold=default_missingness_threshold,
                                                default_lower_limit_percentile=default_lower_limit_percentile,
                                                default_scale_values=default_scale_values,
                                                default_upper_limit_percentile=default_upper_limit_percentile,
                                                default_fill_lower_upper_bound_percentile=default_fill_lower_upper_bound_percentile,
                                                default_fill_upper_lower_bound_percentile=default_fill_upper_lower_bound_percentile,
                                                default_ensure_col=default_ensure_col,
                                                default_dtype=default_dtype,
                                                encoder_dir=encoder_dir,
                                                master_config_dict=master_config_dict,
                                                train_ids=train_ids,
                                                time_index_col=time_index_col,
                                                pre_resample=False,
                                                generate_missing_indicators=False,
                                                file_type=file_type,
                                                **process_df_log_kwargs)

        amended_training_df['pre_resample'] = '0'

        if training_run:
            amended_training_df.loc[((amended_training_df.output_dtype == 'index_column')
                                     & (amended_training_df.column_name.str.contains(r'missing_ind$', na=False, case=False))),
                                    'output_dtype'] = 'binary'
            # return amended_training_df, amended_training_df_pre_resample
            save_data(df=pd.concat([amended_training_df, amended_training_df_pre_resample], axis=0, ignore_index=False),
                      out_path=instruction_fp)
        else:
            amended_training_df = training_df.copy()

    if isinstance(id_index, str):
        if id_index in out.columns:
            out.set_index(id_index, inplace=True)

    if seperate_by_type:
        type_dict: dict = _seperate_by_output_dtype(df=out, transformation_df=amended_training_df)

        if isinstance(time_index_col, str):
            return type_dict, out_post_resample, out_pre_resample
        else:
            return type_dict

    if isinstance(time_index_col, str):
        return out, out_post_resample, out_pre_resample

    return out


def _process_column(series: pd.Series,
                    config_dict: dict,
                    train_idx: pd.Series,
                    training_run: bool,
                    skip_imputation: bool = False,
                    skip_encoding_scaling: bool = False,
                    skip_clip: bool = False,
                    ensure_series: bool = False,
                    **logging_kwargs) -> pd.Series:
    """
    Process column according to specification.

    Actions
    ------
    1. Verify input is correctly formed
    2. Configure logging
    3. Infer whether it is a trainig run or not based on wether the train_idx is the same shape as the input series
    4. Standardize case according to the key 'case_standardization'. *Note* stripping of whitespace characters is done for all case_standardization specifications
    5. Replace Na Values and convert to standards according to the key 'na_values' and 'standardization_values'.
        *Note* na_values and standardization_values for columns with case_standardization will automatiically be transformed to match the specified format.
    6. Convert to numeric, timestamp, or object based on key 'output_dtype'.
    7. Calculate missingness in the training portion of the dataset and drop if it exceeds that value and is part of a training run. Additionally record which rows had empty values.
    8. Perform type specific processing:
        Categorical Columns:
            1. Replace Values below threshold and unobserved values with the the other value using key 'other_value'.
            2. replace missing values with the missing_value using key 'missing_value'.
            3. convert to integers via one_hot (cat_one_hot output_dtype)or embedding (cat_embedding output_dtype)
                a. cat_embedding requires cat_encoder_fp
        Numeric Columns:
            1. Clip out of range values
            2. Fillna values using key 'missing_values'
            3. Standardize numerical values using standard scaler per key 'scale_values'.
                a. 'scaler_fp' is required

    Parameters
    ----------
    series : pd.Series
        DESCRIPTION.
    config_dict : dict
        DESCRIPTION.
    train_idx : pd.Series
        DESCRIPTION.
    **logging_kwargs : TYPE
        DESCRIPTION.

    Returns
    -------
    None.

    """
    # check input
    assert isinstance(
        series, pd.Series), f'The input vairable "series" must be a pandas Series, howerver a {type(series)} was found'

    out: pd.Series = series.copy(deep=True)

    # configure logging
    col_name: str = series.name
    # if col_name == 'sched_room':
    #     debug_inputs(function=_process_column, dump_fp='room.p', kwargs=locals())
    t_logging_kwargs: dict = copy.deepcopy(logging_kwargs)
    t_logging_kwargs['log_name'] = re.sub(r'^\.', '',
                                          t_logging_kwargs.pop('log_name', '') + f'.{col_name}')

    logm(message=f'Proccessing: {col_name}', **t_logging_kwargs)

    # standardize case for object columns if required
    case_standardization: str = config_dict.get('case_standardization', '').lower()
    assert case_standardization in ['', 'upper', 'lower', 'capitalize',
                                    'strip'], f"Unsupported case standardization specification: {case_standardization} provided. Currently only 'upper', 'lower', 'capitalize' are supported"
    replacement_values: dict = {x: None for x in config_dict.get('na_values', [])}
    standardization_values = config_dict.get('standardization_values', None)

    if isinstance(standardization_values, pd.Series):
        standardization_values = standardization_values.to_dict()

    if isinstance(standardization_values, dict):
        replacement_values.update(standardization_values)

    if case_standardization != '':
        logm(message=f'Standardizing Case to: {case_standardization}', **t_logging_kwargs)
        try:
            out = out.astype(object).str.strip()
        except AttributeError:  # allow for non=string values to pass through unchanged
            pass
        replacement_values: pd.Series = pd.Series(replacement_values)
        try:
            replacement_values.index = replacement_values.index.astype(str).str.strip()
        except AttributeError:  # allow for non=string values to pass through unchanged
            pass

        try:
            replacement_values = replacement_values.astype(str).str.strip()
        except AttributeError:  # allow for non=string values to pass through unchanged
            pass

        config_dict['missing_value'] = str(config_dict.get('cat_missing_value') or '').strip()

        config_dict['other_value'] = str(config_dict.get('other_value') or '').strip()

        if case_standardization == 'upper':
            try:
                out = out.str.upper()
            except AttributeError:  # allow for non=string values to pass through unchanged
                pass
            try:
                replacement_values.index = replacement_values.index.str.upper()
            except AttributeError:  # allow for non=string values to pass through unchanged
                pass
            try:
                replacement_values = replacement_values.str.upper()
            except AttributeError:  # allow for non=string values to pass through unchanged
                pass
            config_dict['missing_value'] = config_dict.get('missing_value', '').upper()
            config_dict['other_value'] = config_dict.get('other_value', '').upper()
        elif case_standardization == 'lower':
            try:
                out = out.str.lower()
            except AttributeError:  # allow for non=string values to pass through unchanged
                pass
            try:
                replacement_values.index = replacement_values.index.str.lower()
            except AttributeError:  # allow for non=string values to pass through unchanged
                pass
            try:
                replacement_values = replacement_values.str.lower()
            except AttributeError:  # allow for non=string values to pass through unchanged
                pass
            config_dict['missing_value'] = config_dict.get('missing_value', '').lower()
            config_dict['other_value'] = config_dict.get('other_value', '').lower()
        elif case_standardization == 'capitalize':
            try:
                out = out.str.capitalize()
            except AttributeError:  # allow for non=string values to pass through unchanged
                pass
            try:
                replacement_values.index = replacement_values.index.str.capitalize()
            except AttributeError:  # allow for non=string values to pass through unchanged
                pass
            try:
                replacement_values = replacement_values.str.capitalize()
            except AttributeError:  # allow for non=string values to pass through unchanged
                pass
            config_dict['missing_value'] = config_dict.get('missing_value', '').capitalize()
            config_dict['other_value'] = config_dict.get('other_value', '').capitalize()

        replacement_values = replacement_values.to_dict()

    # replace values with standards
    logm(message=f'Replacing replacement_values: {replacement_values}', **t_logging_kwargs)
    default_other: str = replacement_values.get('xxxotherxxx', replacement_values.get('xxxotherxxx'.upper(), replacement_values.get('xxxotherxxx'.capitalize(), config_dict.get('other_value'))))
    _replaced: bool = False
    if isinstance(standardization_values, dict):
        if len(standardization_values) > 0:
            if str(config_dict.get('output_dtype', '')).lower() == 'binary':
                out[out.notnull()] = out[out.notnull()].apply(lambda x: replacement_values.get(x, config_dict.get('binary_other_value')))
                _replaced: bool = True
            elif default_other is not None:
                out[out.notnull()] = out[out.notnull()].apply(lambda x: replacement_values.get(x, default_other))
                _replaced: bool = True
    if not _replaced:
        out.replace(replacement_values, inplace=True)
    del _replaced

    # convert to desired type
    output_dtype: str = str(config_dict.get('output_dtype',
                                            _get_column_type(series=out,
                                                             one_hot_threshold=config_dict.get('one_hot_embedding_threshold', 10)))).lower()
    assert output_dtype in ['str', 'object', 'int', 'float', 'cat_one_hot', 'cat_embedding', 'binary', 'datetime', 'timestamp', 'cat_str'], f"""Unregonzized output_dtype: {output_dtype}.
    Currently supported output_dtypes include ['str', 'object', 'int', 'float', 'cat_one_hot', 'cat_embedding', 'binary', 'datetime', 'timestamp', 'cat_str']"""

    logm(message=f'Converting to {output_dtype}', **t_logging_kwargs)
    config_dict['output_dtype'] = output_dtype
    if output_dtype in ['str', 'object', 'cat_one_hot', 'cat_embedding', 'cat_str']:
        # out = out.astype(object) if output_dtype == 'object' else out.fillna('XXXXXXNULLXXXXXXX').astype(str).replace({'XXXXXXNULLXXXXXXX': None})
        out = out.fillna('XXXXXXNULLXXXXXXX').astype(str).replace({'XXXXXXNULLXXXXXXX': None})
        config_dict['scaler_fp'] = None
        config_dict['scale_values'] = None
        config_dict['lower_limit_percentile'] = None
        config_dict['upper_limit_percentile'] = None
        config_dict['fill_lower_upper_bound_percentile'] = None
        config_dict['fill_upper_lower_bound_percentile'] = None
        config_dict['missing_value'] = config_dict.get('missing_value')
    elif output_dtype in ['int', 'float', 'binary']:
        # TODO: add check for two levels on converting to binary by creating standardization values
        if out.isin(['n', 'y', 'N', 'Y', 'YES', 'NO', 'yes', 'no']).any():
            raise Exception('Binary Values needs standardization dict. This may require updates to the standardization functions.')
        if (not training_run) and (output_dtype == 'binary'):
            config_dict['value_counts'].index = pd.to_numeric(config_dict.get('value_counts').index, errors='coerce')
        out = pd.to_numeric(out.where(pd.notnull(out), None), errors='coerce')
        config_dict['missing_value'] = config_dict.get('numeric_missing_value' if output_dtype in ['float', 'int'] else 'binary_missing_value')
        config_dict['missing_value'] = config_dict.get('numeric_missing_value' if output_dtype in ['float', 'int'] else 'binary_missing_value')
        if output_dtype == 'binary':
            config_dict['other_value'] = config_dict.get('binary_other_value')
        config_dict['min_categorical_count'] = None
        config_dict['cat_encoder_fp'] = None
        config_dict['one_hot_embedding_threshold'] = None
    elif output_dtype in ['datetime', 'timestamp']:
        out = pd.to_datetime(out.fillna(np.nan), errors='coerce')
        config_dict['scaler_fp'] = None
        config_dict['scale_values'] = None
        config_dict['lower_limit_percentile'] = None
        config_dict['upper_limit_percentile'] = None
        config_dict['fill_lower_upper_bound_percentile'] = None
        config_dict['fill_upper_lower_bound_percentile'] = None
        config_dict['case_standardization'] = None
        config_dict['min_categorical_count'] = None
        config_dict['cat_encoder_fp'] = None
        config_dict['one_hot_embedding_threshold'] = None

    # calculate missignness
    logm(message='Calculating missingness', **t_logging_kwargs)
    config_dict['missing_idx'] = out.isnull().astype(int).rename(f'{col_name}_missing_ind')

    train_missing_count = out[train_idx].isnull().sum()
    train_missing_percentage = train_missing_count / out[train_idx].shape[0]
    config_dict['train_missingness'] = f'{train_missing_count}/{out[train_idx].shape[0]} {train_missing_percentage:.2%}'
    config_dict['overall_missingness'] = f'{out.isnull().sum()}/{out.shape[0]} {out.isnull().sum() / out.shape[0]:.2%}'

    if training_run:
        if 'cat' in output_dtype:
            vc: pd.Series = out.value_counts(dropna=True)
        else:
            vc: pd.Series = pd.Series([1E6])

        if not ensure_series:
            # drop if training run and exceeds threshold
            if ((out.dropna().nunique() == 1)
                or (train_missing_percentage >= config_dict.get('missingness_threshold', 1))
                or (((vc < int(config_dict.get('min_categorical_count') or -999)).sum() == vc.shape[0])
                    and ('cat' in output_dtype))):

                if out.dropna().nunique() == 1:
                    logm(message=f"Dropping column: {col_name} all of the non-null values are the same",
                         **t_logging_kwargs)
                elif (((vc < int(config_dict.get('min_categorical_count') or -999)).sum() == vc.shape[0]) and ('cat' in output_dtype)):
                    logm(message=f"Dropping column: {col_name} because none of the categories exceed the minimum threshold of {config_dict.get('min_categorical_count')}",
                         **t_logging_kwargs)
                elif train_missing_percentage >= config_dict.get('missingness_threshold', 1):
                    logm(message=f"Dropping column: {col_name} because missingness of {train_missing_percentage} exceeds threshold of {config_dict.get('missingness_threshold')}",
                         **t_logging_kwargs)
                config_dict['drop_column'] = True
                config_dict['output_dtype'] = None

                return None, config_dict
        elif out.isnull().all():
            return None, config_dict

    if 'cat' in output_dtype:
        # replace values below min value threshold with key other_value
        rvalue = config_dict.get('other_value', None)
        if training_run:
            min_cat_count: int = int(config_dict.get('min_categorical_count') or -999)
            config_dict['raw_value_counts'] = out.value_counts(dropna=False)
            if min_cat_count != -999:
                col_vcounts = out[train_idx].value_counts(dropna=True)
                lower_than_theshold = col_vcounts[col_vcounts < min_cat_count]
                config_dict['lower_than_theshold_counts'] = lower_than_theshold
                if len(lower_than_theshold) > 0:
                    for v in lower_than_theshold.index:
                        logm(message=f'Replacing {v} with {rvalue}.', **t_logging_kwargs)
                        out.replace({v: rvalue}, inplace=True)
            unobserved_values: list = list(set(out.dropna().unique()) - set(out[train_idx].dropna().unique()))
            config_dict['value_counts'] = out[train_idx].value_counts(dropna=True)
            config_dict['levels'] = config_dict['value_counts'].index.tolist()

        else:
            unobserved_values: list = list(set(out.dropna().unique())
                                           .difference(config_dict.get('levels')))

        if len(unobserved_values) > 0:
            logm(message=f'Dropping {len(unobserved_values)} unobserverd values. Including: {unobserved_values}', **t_logging_kwargs)

            r_candididates = config_dict.get('value_counts')[config_dict.get('value_counts').index.dropna()]

            final_r_value = rvalue if rvalue in config_dict.get('levels') else np.random.choice(a=r_candididates.index.values,
                                                                                                replace=True,
                                                                                                size=out.isin(unobserved_values).sum(),
                                                                                                p=None if rvalue == 'xxxuniform_randomxxx' else (r_candididates.values
                                                                                                                                                 / r_candididates.sum()))
            logm(message=f'Filling unseen embedding levels {unobserved_values} with {"weighted random level from selection" if isinstance(final_r_value, np.ndarray) else ("other_value key: " + final_r_value)}',
                 **t_logging_kwargs)

            out.loc[out.isin(unobserved_values)] = final_r_value

        # fillna's with missing_value
        out, stat_dict = _calculate_stats_and_impute(series=out,
                                                     training_run=training_run,
                                                     train_idx=train_idx,
                                                     config_dict=config_dict,
                                                     skip_imputation=skip_imputation,
                                                     **t_logging_kwargs)

        config_dict.update(stat_dict)

        # ensure it is of type str
        out = out.astype(str)

        # convert to one_hot
        if output_dtype == 'cat_one_hot':
            logm(message='One Hot encoding', **t_logging_kwargs)
            one_hot_df = pd.get_dummies(out, dummy_na=False, prefix=col_name, prefix_sep='_')
            config_dict['cat_encoder_fp'] = None
            if not training_run:
                for col in pd.Index([f'{col_name}_{x}' for x in config_dict.get('levels')]).difference(one_hot_df.columns).tolist():
                    logm(message=f'Filling unseen one_hot level {col} with zero', **t_logging_kwargs)
                    one_hot_df[col] = 0

            return one_hot_df, config_dict

        elif ((output_dtype == 'cat_embedding') and (not skip_encoding_scaling)):
            logm(message='Creating Categorical Embedding', **t_logging_kwargs)
            enc_fp: str = config_dict.get('cat_encoder_fp', None)
            assert isinstance(enc_fp, str), 'cat_encoder_fp is required'
            if training_run:
                encoder = LabelEncoder()
                out[train_idx] = encoder.fit_transform(out[train_idx])
                if train_idx.sum() < out.shape[0]:
                    out[~train_idx] = encoder.transform(out[~train_idx])
                dump(encoder, enc_fp, compress=True)
            else:
                assert os.path.exists(enc_fp), f'The pre-trained categorical encoder: {enc_fp}, could not be found!'
                out[:] = load(enc_fp).transform(out.values)
                # out = pd.Series(load(enc_fp).transform(out.values), index=out.index, name=out.name)

    elif output_dtype in ['float', 'int']:
        if skip_clip:
            config_dict['raw_min_value'] = out[train_idx].min(skipna=True)
            config_dict['raw_max_value'] = out[train_idx].max(skipna=True)
        else:
            out, stat_dict = _clip_and_impute(series=out,
                                              training_run=training_run,
                                              train_idx=train_idx,
                                              lower_limit_percentile=config_dict.get('lower_limit_percentile'),
                                              upper_limit_percentile=config_dict.get('upper_limit_percentile'),
                                              fill_lower_upper_bound_percentile=config_dict.get('fill_lower_upper_bound_percentile'),
                                              fill_upper_lower_bound_percentile=config_dict.get('fill_upper_lower_bound_percentile'),
                                              config_dict=config_dict,
                                              **t_logging_kwargs)
            config_dict.update(stat_dict)

        out, stat_dict = _calculate_stats_and_impute(series=out,
                                                     training_run=training_run,
                                                     train_idx=train_idx,
                                                     config_dict=config_dict,
                                                     skip_imputation=skip_imputation,
                                                     **t_logging_kwargs)
        config_dict.update(stat_dict)

        if (config_dict.get('scale_values', False) and (not skip_encoding_scaling)):
            logm(message='Scaling Numeric Values using StandardScaler',
                 **t_logging_kwargs)
            scaler_fp: str = config_dict.get('scaler_fp', None)
            assert isinstance(scaler_fp, str), 'scaler_fp is required'
            out = out.astype(float)  # convert to float (int columns will throw an index error if replaced with float values)
            if training_run:
                scaler = StandardScaler()
                out[train_idx] = scaler.fit_transform(out[train_idx].values.reshape(-1, 1)).reshape(-1)

                if train_idx.sum() < out.shape[0]:
                    out[~train_idx] = scaler.transform(out[~train_idx].values.reshape(-1, 1)).reshape(-1)
                dump(scaler, scaler_fp, compress=True)
            else:
                assert os.path.exists(
                    scaler_fp), f'The pre-trained standardScaler: {scaler_fp}, could not be found!'
                out[:] = load(scaler_fp).transform(out.values.reshape(-1, 1)).reshape(-1)

    elif output_dtype == 'binary':
        out, stat_dict = _calculate_stats_and_impute(series=out,
                                                     training_run=training_run,
                                                     train_idx=train_idx,
                                                     config_dict=config_dict,
                                                     skip_imputation=skip_imputation,
                                                     **t_logging_kwargs)
        if stat_dict.pop('drop_col_empty', False):
            config_dict['drop_column'] = True
            config_dict['output_dtype'] = None

            return None, config_dict

        config_dict.update(stat_dict)
        if out.notnull().all():
            out = out.astype(float).astype(int)
        else:
            out = out.astype(float)

    return out, config_dict


def _get_column_type(series: pd.Series, one_hot_threshold: int, downcast_floats: bool = False, downcast_dates: bool = False):
    assert isinstance(series, pd.Series) or isinstance(series, pd.DataFrame), f'Input series must be a pandas series or dataframe; however it was found to be of type {type(series)}'

    if isinstance(series, pd.DataFrame):
        return series.apply(_get_column_type, one_hot_threshold=one_hot_threshold, downcast_floats=downcast_floats, downcast_dates=downcast_dates, axis=0)

    inferred_dtype: str = str(series.dropna().infer_objects().dtype)
    if inferred_dtype == 'object':
        try:
            inferred_dtype: str = str(pd.to_numeric(series.dropna(), errors='raise', downcast='integer').dtype)
        except (ValueError, TypeError):
            try:
                inferred_dtype: str = str(pd.to_datetime(series.dropna(), errors='raise').dtype)
            except (ValueError, TypeError):
                pass

    if bool(re.search('^float', inferred_dtype)) and downcast_floats:
        inferred_dtype: str = str(pd.to_numeric(series.dropna(), errors='raise', downcast='integer').dtype)

    try:
        base_type = re.search(r'int|float|object|datetime|category|bool',
                              inferred_dtype).group(0)
    except Exception as e:
        print(inferred_dtype)
        raise Exception(e)

    unique_values: int = pd.to_numeric(series.dropna()).nunique() if base_type in ['float', 'int'] else series.dropna().nunique()

    if (base_type == 'datetime') and downcast_dates:
        if (pd.to_datetime(series.dropna(), errors='raise').dt.time == dt.time(0, 0)).all():
            base_type: str = 'date'

    if ((base_type in ['float', 'int']) and (unique_values == 2)) or (base_type in ['bool']):
        return 'binary'
    elif base_type in ['object', 'category'] and unique_values > one_hot_threshold:
        return 'cat_embedding'
    elif base_type in ['object', 'category']:
        return 'cat_one_hot'
    else:
        return base_type


def _clip_and_impute(series: pd.Series,
                     training_run: bool,
                     train_idx: pd.Series,
                     lower_limit_percentile: float = None,
                     upper_limit_percentile: float = None,
                     fill_lower_upper_bound_percentile: float = None,
                     fill_upper_lower_bound_percentile: float = None,
                     config_dict: dict = None,
                     **logging_kwargs) -> tuple:
    """
    Clip upper/lower values and impute in specified range.

    Parameters
    ----------
    series : pd.Series
        Numeric inputseries to be processed.
    training_run : bool
        Whether to calcuate values corresponding to the quantiles or to use provided ones.
    train_idx: pd.Series
        index of traning values in series. If there is no such differentiation, pass a series of all True with the index set to the index of the series.
    lower_limit_percentile : float, optional, range (0,1)
        The quantile below which values will be clipped or replaced.
        The default is None, which will not do anything with low values.
    upper_limit_percentile : float, optional, range (0,1)
        The quantile above which values will be clipped or replaced.
        The default is None, which will not do anything with lhigh values.
    fill_lower_upper_bound_percentile : float, optional, range (0,1)
        The quantile which defines the upper bound of low values to sample from in order to replace the values lost to the clip.
        e.g. if fill_lower_upper_bound_percentile is set to 0.15 and lower_limit_percentile was set to 0.01,
            values will be drawn from the uniform distribution [0.01, 0.15) to replace values below 0.01.
        The default is None, which will result in the clipping of low values without replacement if lower_limit_percentil is provided.
    fill_upper_lower_bound_percentile : float, optional, range (0,1)
        The quantile which defines the lower bound of  high values to sample from in order to replace the values lost to the clip.
        e.g. if fill_upper_lower_bound_percentile is set to 0.85 and upper_limit_percentile was set to 0.90,
            values will be drawn from the uniform distribution [0.85, 0.90) to replace values above 0.90.
    config_dict : dict, optional
        Dictionary containing the following keys.
            *lower_limit_value
            *upper_limit_value
            *fill_lower_upper_bound_value
            *fill_upper_lower_bound_value
        *Note*: Required for non-training runs. The default is None.
    **logging_kwargs : TYPE
        keywords to pass the to the log_print_email_message function from the Utils.log_messages module.

    Returns
    -------
    tuple
        clipped input series (pd.Series), stat_dict (dict).

        stat_dict may contain the following keys:
            *lower_limit_value
            *upper_limit_value
            *fill_lower_upper_bound_value
            *fill_upper_lower_bound_value
            *raw_min_value
            *raw_max_value

    """
    assert isinstance(
        series, pd.Series), f'The input series must be a pandas series; however a {type(series)} was found.'
    assert _return_numeric(series), f'The input series must be a numeric series (int, float), but type {series.dtype} was found'

    stat_dict: dict = {}

    if training_run:
        # calculate all stats before modification
        if isinstance(lower_limit_percentile, float):
            assert (lower_limit_percentile > 0) and (lower_limit_percentile
                                                     < 1), f'lower_limit_percentile must be in the range (0,1), but found value {lower_limit_percentile}'
            lower: float = series[train_idx].dropna().quantile(
                q=lower_limit_percentile, interpolation='midpoint')
            stat_dict['lower_limit_value'] = lower
        else:
            lower: float = None

        if isinstance(upper_limit_percentile, float):
            assert (upper_limit_percentile > 0) and (upper_limit_percentile
                                                     < 1), f'upper_limit_percentile must be in the range (0,1), but found value {upper_limit_percentile}'
            upper: float = series[train_idx].dropna().quantile(
                q=upper_limit_percentile, interpolation='midpoint')
            stat_dict['upper_limit_value'] = upper
        else:
            upper: float = None

        if isinstance(fill_lower_upper_bound_percentile, float):
            assert (fill_lower_upper_bound_percentile > 0) and (fill_lower_upper_bound_percentile
                                                                < 1), f'fill_lower_upper_bound_percentile must be in the range (0,1), but found value {fill_lower_upper_bound_percentile}'
            lower_upper: float = series[train_idx].dropna().quantile(
                q=fill_lower_upper_bound_percentile, interpolation='midpoint')
            stat_dict['fill_lower_upper_bound_value'] = lower_upper
        else:
            lower_upper: float = None

        if isinstance(fill_upper_lower_bound_percentile, float):
            assert (fill_upper_lower_bound_percentile > 0) and (fill_upper_lower_bound_percentile
                                                                < 1), f'fill_upper_lower_bound_percentile must be in the range (0,1), but found value {fill_upper_lower_bound_percentile}'
            upper_lower: float = series[train_idx].dropna().quantile(
                q=fill_upper_lower_bound_percentile, interpolation='midpoint')
            stat_dict['fill_upper_lower_bound_value'] = upper_lower
        else:
            upper_lower: float = None

        stat_dict['raw_min_value'] = series[train_idx].min(skipna=True)
        stat_dict['raw_max_value'] = series[train_idx].max(skipna=True)
    else:
        assert isinstance(
            config_dict, dict), 'A config_dict is required for non-training runs'

        lower: float = config_dict.get('lower_limit_value')
        lower_upper: float = config_dict.get('fill_lower_upper_bound_value')
        upper: float = config_dict.get('upper_limit_value')
        upper_lower: float = config_dict.get('fill_upper_lower_bound_value')

    # handle lower outliers
    if isinstance(lower, float):
        if isinstance(lower_upper, float):
            replaceable_idx: pd.Series = series < lower

            if replaceable_idx.any():
                logm(f'Replacing values less than {lower_limit_percentile:.2%} percentile with values between {lower_limit_percentile:.2%} and {fill_lower_upper_bound_percentile:.2%} percentiles',
                     **logging_kwargs)

                series.loc[replaceable_idx] = np.repeat([lower], replaceable_idx.sum()) if lower == lower_upper\
                    else np.random.uniform(low=lower,
                                           high=lower_upper,
                                           size=replaceable_idx.sum()) if 'float' in str(series.dtype)\
                    else np.random.randint(low=lower,
                                               high=lower_upper,
                                               size=replaceable_idx.sum())
            else:
                logm(message=f'No values less than {lower_limit_percentile:.2%} percentile were found', **logging_kwargs)
        else:
            logm(message=f'Clipping values less than {lower_limit_percentile:.2%} percentile', **logging_kwargs)
            series.clip(lower=lower, upper=None, inplace=True)

    # handle upper outliers
    if isinstance(upper, float):
        if isinstance(upper_lower, float):
            replaceable_idx: pd.Series = series > upper
            if replaceable_idx.any():
                logm(
                    f'Replacing values greater than {upper_limit_percentile:.2%} percentile with values between {fill_upper_lower_bound_percentile:.2%} and {upper_limit_percentile:.2%} percentiles',
                    **logging_kwargs)

                if upper_lower == upper:
                    logm(message=f'Upper and upper lower values were the same. Imputing with {upper}', **logging_kwargs)
                    series.loc[replaceable_idx] = [upper] * replaceable_idx.sum()
                else:
                    series.loc[replaceable_idx] = np.random.uniform(low=upper_lower,
                                                                    high=upper,
                                                                    size=replaceable_idx.sum()) if 'float' in str(series.dtype) else np.random.randint(low=upper_lower,
                                                                                                                                                       high=upper,
                                                                                                                                                       size=replaceable_idx.sum())
            else:
                logm(message=f'No values greater than {upper_limit_percentile:.2%} percentile were found', **logging_kwargs)
        else:
            logm(
                message=f'Clipping values greater than the {upper_limit_percentile:.2%} percentile', **logging_kwargs)
            series.clip(upper=upper, lower=None, inplace=True)

    return series, stat_dict


def _calculate_stats_and_impute(series: pd.Series,
                                training_run: bool,
                                train_idx: pd.Series,
                                config_dict: dict,
                                default_one_hot_embedding_threshold: int = 5,
                                skip_imputation: bool = False,
                                **logging_kwargs) -> tuple:
    """
    Calculate sttats for a series and impute missing values per specification.

    Parameters
    ----------
    series: pd.Series
        series from which calculations will be made during training runs and missing values imputed for all runs.
    training_run: bool
        Whether to compute statistics and impute or to impute using pre-calculated values.
    train_idx: pd.Series
        index of values corresponding to training set values. If there is no differentiation. Pass a series with the same index as the input series with all values as true
    config_dict: dict
        Configuration dictionary with the following keys:
            *output_dtype
            *mode
            *mean
            *median
            *std
            *mad
            *min
            *max
            *missing_value
            *other_value
            *value_counts
            *levels
    default_one_hot_embedding_threshold : int, optional
        cutoff value whether a series should be inferred to be a cat_one_hot or cat_embedding type. The default is 5.
    **logging_kwargs : TYPE
        kwarrgs to pass to the log_print_email_message function from the Utils.logging_module.

    Returns
    -------
    tuple
        imputed input series (pd.Series), stat_dict (dict).

        stat_dict may contain the following values
        *value_counts
        *levels
        *mean
        *median
        *std
        *mad
        *max
        *min

    """
    assert isinstance(series, pd.Series), f'The input series must be a pandas seires; however a series of type {type(series)} was provided.'

    # use key output_dtype from config_dict if available, else use inferred_dtype
    dtype: str = config_dict.get('output_dtype', _get_column_type(series, one_hot_threshold=config_dict.get(
        'one_hot_embedding_threshold', default_one_hot_embedding_threshold)))

    assert dtype in ['binary', 'cat_one_hot', 'cat_embedding', 'float',
                     'int', 'cat_str'], f"Unsupported dtype: {dtype}, currently only ['binary', 'cat_one_hot', 'cat_embedding', 'float', 'int', 'cat_str'] are supported"

    stat_dict: dict = {}
    if training_run:
        logm(message='Calculating Stats', **logging_kwargs)
        # calculate mode for all types
        try:
            stat_dict['mode'] = series[train_idx].mode(dropna=True).iloc[0]
        except IndexError:
            stat_dict['mode'] = None

        if dtype in ['binary', 'cat_one_hot', 'cat_embedding', 'cat_str']:
            # calculate levels and value counts for categorical and one_hot columns
            stat_dict['value_counts'] = series[train_idx].value_counts(dropna=False)
            stat_dict['levels'] = stat_dict['value_counts'].index.tolist()

        elif dtype in ['float', 'int']:
            # calculate mean, median, std, mad (median absolute deviance), min, and max for numeric types
            stat_dict['mean'] = series[train_idx].mean(skipna=True)
            stat_dict['median'] = series[train_idx].median(skipna=True)
            stat_dict['std'] = series[train_idx].std(skipna=True)
            stat_dict['mad'] = mad(series[train_idx], nan_policy='omit', scale='normal')
            stat_dict['min'] = series[train_idx].min(skipna=True)
            stat_dict['max'] = series[train_idx].max(skipna=True)

    # return series if there are no null values
    if series.notnull().all() or skip_imputation:
        if skip_imputation:
            stat_dict['missing_value'] = None
        return series, stat_dict

    # extract the missing instruction from the config_dict
    default_missing = str(config_dict.get('missing_value', ''))

    # generate impute values if necessary
    if dtype in ['binary', 'cat_one_hot', 'cat_embedding']:
        if default_missing.lower() == 'xxxmodexxx':
            logm(message='Filling missing values using mode', **logging_kwargs)
            series.fillna(config_dict.get('mode', stat_dict.get('mode')), inplace=True)
            default_missing: str = ''
        elif default_missing.lower() == 'xxxotherxxx':
            ov: str = 'binary_other_value' if dtype == 'binary' else 'other_value'
            default_missing: str = config_dict.get(ov, stat_dict.get(ov))

        if ((default_missing.lower() in ['xxxuniform_randomxxx', 'xxxweighted_randomxxx']) or ((default_missing not in config_dict.get('levels', [])) and (not training_run))):

            if (dtype == 'binary') and default_missing in [0, 1, '0', '1']:
                pass
            else:
                # pull from random uniform distribution in the target space
                if default_missing.lower() in ['xxxuniform_randomxxx', 'xxxweighted_randomxxx']:
                    logm(message=f'Filling missing values using: {default_missing.lower()}', **logging_kwargs)
                else:
                    logm(message=f'The default missing value ({default_missing}) was not in the training data. A weighted random sample from the training data will be used instead',
                         **logging_kwargs, warning=True)
                vc: pd.Series = config_dict.get('value_counts', stat_dict.get('value_counts'))
                vc = vc[vc.index.dropna()]

                if len(vc) == 0:
                    if dtype == 'binary':
                        vc: pd.Series = pd.Series({0: 50, 1: 50})
                    else:
                        raise Exception(f"There are no values from the traning set to sample from in order to fill {series.name} with 'xxxuniform_randomxxx' or 'xxxweighted_randomxxx'")

                series[series.isnull()] = np.random.choice(a=vc.index,
                                                           size=series.isnull().sum(),
                                                           replace=True,
                                                           p=None if 'xxxuniform_randomxxx' else vc.values / vc.sum())  # if p is None it is a uniform random dist
                default_missing: str = ''

        if default_missing != '':
            update_vc: bool = False
            if dtype == 'binary':
                # verify other value is in range [0,1]
                try:
                    other_value: int = int(float(default_missing))
                    fillable: bool = other_value in [0, 1]
                except ValueError:
                    fillable: bool = False
            else:
                # verify the other value is either in the training levels or there are atleast one missing value in the training set during the training run
                if default_missing in config_dict.get('levels', stat_dict.get('levels')):
                    fillable: bool = True
                elif (training_run and series[train_idx].isnull().any()):
                    fillable: bool = True
                    update_vc: bool = True
                else:
                    fillable: bool = False

            if fillable:
                logm(message=f'Filling missing values using {default_missing}', **logging_kwargs)
                series.fillna(default_missing, inplace=True)
                stat_dict['levels'] = series.unique().tolist()
                if update_vc:
                    stat_dict['value_counts'] = series[train_idx].value_counts(dropna=False)
                    stat_dict['levels'] = stat_dict['value_counts'].index.tolist()
            else:
                logm(message=f'The default missing value ({default_missing}) was not in the training data. A weighted random sample from the training data will be used instead',
                     **logging_kwargs, warning=True)

                vc: pd.Series = config_dict.get('value_counts', stat_dict.get('value_counts'))
                vc = vc[vc.index.dropna()]

                if len(vc.index.values) > 0:

                    series[series.isnull()] = np.random.choice(a=vc.index.values,
                                                               size=series.isnull().sum(),
                                                               replace=True,
                                                               p=None if 'xxxuniform_randomxxx' else vc.values / vc.sum())  # if p is None it is a uniform random dist
                else:
                    logm(f'There were no samples to choose from: Leaving the column blank and dropping: {series.name}', warning=True)
                    stat_dict['drop_col_empty'] = True

    elif dtype in ['int', 'float']:
        if default_missing.lower() in ['xxxmean_std_normal_randomxxx']:
            logm(message='Filling missing values using values from a normal distribution matching the training data', **logging_kwargs)
            series[series.isnull()] = np.random.normal(loc=config_dict.get('mean', stat_dict.get('mean')),
                                                       scale=config_dict.get('std', stat_dict.get('std')),
                                                       size=series.isnull().sum())
            default_missing: str = ''
        if default_missing.lower() in ['xxxmedian_mad_normal_randomxxx']:
            logm(message='Filling missing values using values from a normal distribution matching the training data', **logging_kwargs)
            series[series.isnull()] = np.random.normal(loc=config_dict.get('median', stat_dict.get('median')),
                                                       scale=config_dict.get('mad', stat_dict.get('mad')),
                                                       size=series.isnull().sum())
            default_missing: str = ''
        elif default_missing.lower() == 'xxxuniform_randomxxx':
            logm(message='Filling missing values using values from a uniform distribution from the min/max values of the training data', **logging_kwargs)
            lv: float = config_dict.get('min', stat_dict.get('min'))
            # add small amount because high limit is exclusive
            hv: float = config_dict.get('max', stat_dict.get('max')) + (1 if dtype == 'int' else 0.001)
            sz: int = series.isnull().sum()
            if dtype == 'int':
                series[series.isnull()] = np.random.randint(low=lv,
                                                            high=hv,
                                                            size=sz, dtype=int)
            elif dtype == 'float':
                series[series.isnull()] = np.random.uniform(low=lv,
                                                            high=hv,
                                                            size=sz)
            default_missing: str = ''
        elif default_missing.lower() == 'xxxmodexxx':
            logm(message='Filling missing values using mode', **logging_kwargs)
            series.fillna(float(config_dict.get(
                'mode', stat_dict.get('mode'))), inplace=True)
            default_missing: str = ''
        elif default_missing.lower() == 'xxxmeanxxx':
            logm(message='Filling missing values using mean', **logging_kwargs)
            series.fillna(float(config_dict.get(
                'mean', stat_dict.get('mean'))), inplace=True)
            default_missing: str = ''
        elif default_missing.lower() == 'xxxmedianxxx':
            logm(message='Filling missing values using median', **logging_kwargs)
            series.fillna(float(config_dict.get(
                'median', stat_dict.get('median'))), inplace=True)
            default_missing: str = ''
        # elif default_missing.lower() == 'xxxotherxxx':
        #     default_missing: str = config_dict.get('other_value', stat_dict.get('other_value'))

        if default_missing != '':
            try:
                series.fillna(float(default_missing), inplace=True)
                logm(
                    message=f'Filling missing values using {default_missing}', **logging_kwargs)
            except ValueError:
                logm(
                    message=f'the value {default_missing} is not in the levels of the training data and consequently was not filled', **logging_kwargs)

    if dtype in ['int', 'binary'] and series.notnull().all():
        series = series.astype(float).astype(int)

    return series, stat_dict


def _seperate_by_output_dtype(df: pd.DataFrame, transformation_df: pd.DataFrame) -> pd.DataFrame:
    out: dict = {}

    required_cols: list = df.columns.intersection(transformation_df.loc[transformation_df.output_dtype.isin(['time_index', 'id_index', 'index_column']), 'column_name'].tolist()).tolist()

    for dtype in ['str', 'object', 'int', 'float', 'cat_one_hot', 'cat_embedding', 'binary', 'datetime', 'timestamp']:
        cols: list = transformation_df.loc[((transformation_df.drop_column.isnull()
                                             | transformation_df.drop_column.isin([False, "False", "false", 0, "0", "0.0"]))
                                            & (transformation_df.output_dtype == f"{dtype}")), 'column_name'].tolist()

        if (len(df.columns.intersection(cols)) == 0) and (len(cols) > 0):
            cols: list = df.columns[df.columns.astype(str).str.match(r'^' + r'_|^'.join(cols) + '_')].tolist()

        if (dtype == 'cat_one_hot') and (len(cols) > 0):
            cols = df.columns[df.columns.str.match(r'|'.join([r'^{}_'.format(x) for x in cols])) & (~df.columns.isin([f'{x}_missing_ind' for x in cols]))].tolist()

        out[dtype] = df[df.columns.intersection(required_cols).tolist() + cols].copy() if len(cols) > 0 else None

    return out


def _return_numeric(input_v):
    if isinstance(input_v, pd.Series):
        return bool(re.search(r'int|float|binary', _get_column_type(series=input_v, one_hot_threshold=10), re.IGNORECASE))
    elif isinstance(input_v, pd.DataFrame):
        col_dtypes: list = input_v.apply(_get_column_type, one_hot_threshold=10, axis=0)
        return col_dtypes[col_dtypes.str.contains(r'int|float', regex=True, case=False)].index
    raise NotImplementedError(
        f'_return numeric has not yet been implemented for type: {type(input_v)}, please provide a pandas series or dataframe or modify this function')


def resample_pandas(data_structure: Union[pd.Series, pd.DataFrame],
                    start: Union[pd.Series, pd.DataFrame, pd.Timestamp, None] = None,
                    end: Union[pd.Series, pd.DataFrame, pd.Timestamp, None] = None,
                    time_bin: str = '4H',
                    custom_agg_dict: dict = None,
                    interpolation_method: str = 'linear',
                    interpolation_limit_direction: str = 'forward',
                    interpolation_limit: int = None,
                    interpolation_limit_area: str = None,
                    resample_origin: str = 'start',
                    resample_label: str = 'left',
                    id_col: str = None,
                    label_col: str = None,
                    dt_index: str = None,
                    fillna_val: any = None,
                    ds_dtype: str = None,
                    agg_func: Union[callable, str] = None,
                    value_col: str = None,
                    **logging_kwargs) -> pd.DataFrame:
    """
    Resample Time Series and interpolate.

    Parameters
    ----------
    data : Union[pd.Series, pd.DataFrame]
        Pandas Series or DataFrame with a datetime column/index.
    start : Union[pd.Series, pd.DataFrame, pd.Timestamp, None]
        first timestamp to ensure is in the series.
        **NOTE**:
                * a Timesamp is only accepted if there is only one ID.

                * a pd.Series will need to have the id_col as the index.

                * a pd.DataFrame will need to have the following columns [id_col, 'start']
    end : Union[pd.Series, pd.DataFrame, pd.Timestamp, None]
        last timestamp to ensure is in the series.
        **NOTE**:
                * a Timesamp is only accepted if there is only one ID.

                * a pd.Series will need to have the id_col as the index.

                * a pd.DataFrame will need to have the following columns [id_col, 'end']
    time_bin : str, optional
        time window to bin data into. The default is '4H'.
    custom_agg_dict : dict, optional
        custom summarization statistics to summarize the data. The default is None.
        The following template can be used:
            {column_name: {
                            'ds_dtype': str (custom resampling column dtype, NOTE: only used in resampling **NOT Recommended**)
                            'function': Union[str, callable] (custom function to be used for time bin aggregation)
                            'agg_names': List[str] (custom list of column names derived from the source column)
                            'interpolation_limit_direction': str (custom interpolation limit direction, see parameter documentaiton for details)
                            'interpolation_limit_area': str (custom interpolation limit area, see parameter documentaiton for details)
                            'interpolation_method': str (custom interpolation method, see parameter documentaiton for details)
                            'limit': str (custom interpolation limit, see parameter documentaiton for details)
                            'fillna_val': str (custom fill na value, see parameter documentaiton for details)
                        }
            }
    interpolation_method : str, optional
        Method used to interpolate the data. The default is 'linear'.

        *‘linear’: Ignore the index and treat the values as equally spaced. This is the only method supported on MultiIndexes.
        *‘time’: Works on daily and higher resolution data to interpolate given length of interval.
        *‘index’, ‘values’: use the actual numerical values of the index.
        *‘pad’: Fill in NaNs using existing values.
        *‘nearest’, ‘zero’, ‘slinear’, ‘quadratic’, ‘cubic’, ‘spline’, ‘barycentric’, ‘polynomial’: Passed to scipy.interpolate.interp1d.
            These methods use the numerical values of the index. Both ‘polynomial’ and ‘spline’ require that you also specify an order (int), e.g. df.interpolate(method='polynomial', order=5).
        *‘krogh’, ‘piecewise_polynomial’, ‘spline’, ‘pchip’, ‘akima’, ‘cubicspline’: Wrappers around the SciPy interpolation methods of similar names. See Notes.
        *‘from_derivatives’: Refers to scipy.interpolate.BPoly.from_derivatives which replaces ‘piecewise_polynomial’ interpolation method in scipy 0.18.

    interpolation_limit_direction : str, optional [‘forward’, ‘backward’, ‘both’, None]
        Consecutive NaNs will be filled in this direction. The default is 'forward'.

        If limit is specified:
            If ‘method’ is ‘pad’ or ‘ffill’, ‘limit_direction’ must be ‘forward’.
            If ‘method’ is ‘backfill’ or ‘bfill’, ‘limit_direction’ must be ‘backwards’.

        If ‘limit’ is not specified:
            If ‘method’ is ‘backfill’ or ‘bfill’, the default is ‘backward’
            else the default is ‘forward’

    interpolation_limit : int, optional
        Maximum number of consecutive NaNs to fill. Must be greater than 0 if specified. The default is None.

    interpolation_limit_area : str, optional [None, ‘inside’, ‘outside’], default is None.
        If limit is specified, consecutive NaNs will be filled with this restriction.
            *None: No fill restriction.
            *‘inside’: Only fill NaNs surrounded by valid values (interpolate).
            *‘outside’: Only fill NaNs outside valid values (extrapolate).

    resample_origin: str, optional [‘epoch’, ‘start’, ‘start_day’, ‘end’, ‘end_day’], default is 'start'
        *‘epoch’: origin is 1970-01-01
        *‘start’: origin is the first value of the timeseries
        *‘start_day’: origin is the first day at midnight of the timeseries
        *‘end’: origin is the last value of the timeseries
        *‘end_day’: origin is the ceiling midnight of the last day

    resample_label: str, optional ['left', 'right']
        Which bin edge label to label bucket with. The default is 'left'.

    id_col: str, optional
        Column or index name that distinguishes encounters/patients. The default is None.
    label_col: str, optional
        The column in the multi-index which may be used as a column label if so desired.
        This is used with long tables not with wide tables. The default is None.
    dt_index: str, optional
        The datetime column/index column to be used for resampling/interpolation. The default is None, which will infer the column automatically.
    fillna_val: any, optional
        The value to fill any missing values after aggregation. The default is None, which will attempt to use interpolation.
    ds_dtype: str, optional
        Can be used to force a specific datatype for resampling. The default is None.
        The use case for this would generally be when only the count of observations is important as apposed to the actual numbers for a numeric variable.
    agg_func: Union[callable, str], optional
        Funciton to be used to aggregate data within each time bin.
        This may either be a function itself or a string referncing a specific module e.g. "Utils.aggregation_functions._default_non_numeric_agg".
        The default is None, which will use a numeric aggregator on numeric vars (extracts min, max, median, mean, etc.) or non-numeric agg which extracts count.
    value_col: str, optional
        The column which contains the actual values for a long table. This will be used to correctly name the resultant output in wide form, if so desired.
        The default is None.
    **logging_kwargs
        kwargs to be passed to the log_print_email_message funciton in the Utils.log_messages module.

    Raises
    ------
    NotImplementedError
        will return this error if a cstom_agg_dict is not provided or the data type is not numeric.

    Returns
    -------
    pd.DataFrame
        resampled data.

    """
    # sig, vard = inspect.signature(resample_pandas), locals()

    # extract dtypes for columns and indicies
    index_dtypes: pd.Series = data_structure.index.dtypes.astype(str) if isinstance(data_structure.index, pd.MultiIndex) else pd.Series({data_structure.index.name: str(data_structure.index.dtype)})
    index_dtypes = index_dtypes[index_dtypes.index.notnull()]
    column_dtypes: pd.Series = data_structure.dtypes.astype(str) if isinstance(data_structure, pd.DataFrame) else pd.Series({data_structure.name: str(data_structure.dtype)})

    # identify/validate dt_index
    if dt_index is None:
        logm(message='Inferring datetime columns/index', **logging_kwargs)
        dt_indexes = index_dtypes[index_dtypes.str.contains('datetime64[ns]', regex=False)].index.tolist() + column_dtypes[column_dtypes.str.contains('datetime64[ns]', regex=False)].index.tolist()
        assert len(dt_indexes) > 0, 'No datetime columns/indexes were found'
        assert len(dt_indexes) == 1, f'Multiple datetime columns/indexes were found: {dt_indexes}'
        dt_index: str = dt_indexes[0]
    else:
        assert len(index_dtypes.index.intersection([dt_index]).tolist() + column_dtypes.index.intersection([dt_index]).tolist()
                   ) == 1, f'Assert the index/column: {dt_index} was not found in the datastructure. Or was found more than once.'
        dt_col_type: str = (index_dtypes[index_dtypes.index.intersection([dt_index])].tolist() + column_dtypes[column_dtypes.index.intersection([dt_index])].tolist())[0]
        assert dt_col_type == 'datetime64[ns]', f'The column/index should be of type datetime64[ns], but was found to have type: {dt_col_type}'

    # convert to dataframe for consistancy
    if isinstance(data_structure, pd.Series):
        data_structure: pd.DataFrame = pd.DataFrame(data_structure)

    # check index cols are in the index
    for ic in [id_col, dt_index, label_col]:
        if isinstance(ic, str):
            if ic in column_dtypes:
                data_structure.set_index([ic], append=False if index_dtypes.shape[0] == 0 else True, inplace=True)
                index_dtypes[ic] = column_dtypes[ic]
                column_dtypes.drop(ic, inplace=True)

    # add start and stop sequences
    assert (start is None) or isinstance(start, pd.DataFrame) or isinstance(start, pd.Series) or isinstance(
        start, pd.Timestamp), f'Start must be either a dataframe, series, or timestamp; however, a {type(start)} was found.'
    assert (end is None) or isinstance(end, pd.DataFrame) or isinstance(end, pd.Series) or isinstance(
        end, pd.Timestamp), f'Start must be either a dataframe, series, or timestamp; however, a {type(end)} was found.'

    if (dt_index in index_dtypes) and (index_dtypes.shape[0] == 1):
        assert (start is None) or isinstance(start, pd.Timestamp), f'start must me a pd.Timestamp or None if a singluar datetimeindex is provided; however, a {type(start)} was provided'
        assert (end is None) or isinstance(end, pd.Timestamp), f'end must me a pd.Timestamp or None if a singluar datetimeindex is provided; however, a {type(end)} was provided'
        end_points: list = pd.Series([start, end]).dropna().tolist()
        if len(end_points) > 0:
            data_structure = data_structure.append(pd.DataFrame(index=end_points))
    elif isinstance(id_col, str):
        # create base
        # if len(final_index_cols) > 0 else pd.DataFrame(data_structure.index).drop_duplicates()
        base: pd.DataFrame = data_structure[[]].reset_index(drop=False).drop(columns=[dt_index], errors='ignore').drop_duplicates().copy()
        assert id_col in base.columns, f'The id_col: {id_col}, must be in the index columns'
        additions: list = []
        for i, e in enumerate([start, end]):
            if isinstance(e, pd.DataFrame):
                assert id_col in e.columns, f'id_col: {id_col} is missing from {"start" if i == 0 else "end"}'
                additions.append(base.merge(e.rename(columns={'start': dt_index, 'end': dt_index}),
                                            how='left',
                                            on=id_col))
            elif isinstance(e, pd.Series):
                assert e.index.name == id_col, f'index_name: {e.index.name} in {"start" if i == 0 else "end"} must match the id_col: {id_col}'
                additions.append(base.merge(e.rename(dt_index),
                                            how='left',
                                            left_on=id_col,
                                            right_index=True))

        if len(additions) > 0:
            data_structure = data_structure.append(pd.concat(additions, axis=0, ignore_index=True).set_index(data_structure.index.names), ignore_index=False)
        del base, additions
    else:
        pass

    # resample dataframe
    cols_to_agg: list = data_structure.columns.tolist()
    logm(f'resampling: {cols_to_agg}', **logging_kwargs)
    assert resample_origin in ['epoch', 'start', 'start_day', 'end', 'end_day'], f'Invalid resample_origin: {resample_origin} specified. Supported values include: [‘epoch’, ‘start’, ‘start_day’, ‘end’, ‘end_day’].'
    assert resample_label in ['left', 'right'], f'Invalid resample_label: {resample_label} specified. Supported Values include: [left and right].'

    # validate interpolation kwargs
    if isinstance(interpolation_limit, int):
        assert interpolation_limit > 0, 'Invalid limit value specified, limit must be greater than zero'
    else:
        assert interpolation_limit is None, f'Invalid limit type specified. it must be None or an int; howerver, a {type(interpolation_limit)} was found.'
    assert interpolation_limit_direction in ['forward', 'backward', 'both', None], f'Invalid limit direction: {interpolation_limit_direction} specified. Supported values include: [‘forward’, ‘backward’, ‘both’, None].'
    assert interpolation_limit_area in [None, 'inside', 'outside'], f'Invalid interpolation_limit_area: {interpolation_limit_area} specified. Supported values include: [None, ‘inside’, ‘outside’].'

    out = _auto_resampler(data_structure=data_structure,
                          cols_to_agg=cols_to_agg,
                          custom_agg_dict=custom_agg_dict,
                          ds_dtype=ds_dtype,
                          agg_func=agg_func,
                          interpolation_limit_direction=interpolation_limit_direction,
                          interpolation_limit_area=interpolation_limit_area,
                          interpolation_method=interpolation_method,
                          interpolation_limit=interpolation_limit,
                          fillna_val=fillna_val,
                          dt_index=dt_index,
                          time_bin=time_bin,
                          resample_origin=resample_origin,
                          resample_label=resample_label,
                          id_col=id_col)

    if isinstance(label_col, str) and isinstance(value_col, str):
        assert label_col in out.index.names, f'The label col: {label_col} was not found in the multi-index'
        out = out.reset_index(level=label_col).pivot(columns=label_col)
        out.columns = [x.replace(value_col, y) for x, y in out.columns]

    return out


def _auto_resampler(data_structure: pd.DataFrame,
                    cols_to_agg: list,
                    custom_agg_dict: dict,
                    ds_dtype: str,
                    agg_func: Union[str, callable],
                    interpolation_limit_direction: str,
                    interpolation_limit_area,
                    interpolation_method: Union[str, callable],
                    interpolation_limit: int,
                    fillna_val: any,
                    dt_index: str,
                    time_bin: str,
                    resample_origin: str,
                    resample_label: str,
                    id_col: str,
                    **logging_kwargs):
    if isinstance(id_col, str):
        levels: np.ndarray = data_structure[[]].reset_index(level=id_col)[id_col].unique()
        ulevels: int = len(levels)

        if ulevels <= 800:
            pass
        else:
            ds: pd.DataFrame = data_structure.reset_index(level=id_col)

            num_batches: int = int(ulevels / 500)

            logm(message=f'Splitting resampling job into {num_batches} to expedite resampling', display=True)

            auto_resample_kwargs: list = [{'data_structure': ds[ds[id_col].isin(g)].set_index([id_col], append=True),
                                           'cols_to_agg': cols_to_agg,
                                           'custom_agg_dict': custom_agg_dict,
                                           'ds_dtype': ds_dtype,
                                           'agg_func': agg_func,
                                           'interpolation_limit_direction': interpolation_limit_direction,
                                           'interpolation_limit_area': interpolation_limit_area,
                                           'interpolation_method': interpolation_method,
                                           'interpolation_limit': interpolation_limit,
                                           'fillna_val': fillna_val,
                                           'dt_index': dt_index,
                                           'time_bin': time_bin,
                                           'resample_origin': resample_origin,
                                           'resample_label': resample_label,
                                           'id_col': None,
                                           'log_name': logging_kwargs.get('log_name', '') + f'.auto_resampler.{i}',
                                           'display': logging_kwargs.get('display', False)} for i, g in enumerate(np.array_split(levels, num_batches))]

            return pd.concat([x['future_result'] for x in run_function_in_parallel_v2(function=_auto_resampler,
                                                                                      kwargs_list=auto_resample_kwargs,
                                                                                      max_workers=min(32, os.cpu_count()),
                                                                                      update_interval=10,
                                                                                      disp_updates=False,
                                                                                      list_running_futures=False,
                                                                                      return_results=True,
                                                                                      log_name='resampler',
                                                                                      executor_type='ProcessPool',
                                                                                      debug=False)], axis=0, sort=False, ignore_index=False)

    out: pd.DataFrame = None

    for col in cols_to_agg:
        config: dict = (custom_agg_dict or {}).get(col, {})
        # logm(config, **logging_kwargs)
        dtype = config.get('ds_dtype', ds_dtype) or _get_column_type(series=data_structure[col], one_hot_threshold=10)
        agg_f = (config.get('function') or agg_func) or (_numeric_aggregators if dtype in ['int', 'float', 'binary'] else _default_non_numeric_agg)
        if isinstance(agg_f, str):
            agg_f = get_func(agg_f)
        agg_names: list = config.get('agg_names')
        ld = config.get('interpolation_limit_direction') or interpolation_limit_direction
        la = config.get('interpolation_limit_area') or interpolation_limit_area
        m = config.get('interpolation_method') or interpolation_method
        lim = config.get('limit') or interpolation_limit
        fv: any = config.get('fillna_val') or fillna_val

        if (not bool(re.search(r'int|float', str(data_structure[col])))) and (agg_f.__name__ == '_numeric_aggregators'):
            data_structure.loc[:, col] = pd.to_numeric(data_structure.loc[:, col].fillna(np.nan), errors='coerce')

        if isinstance(data_structure.index, pd.MultiIndex):
            indexes: list = list(data_structure.index.names)
            indexes.remove(dt_index)

            base: pd.DataFrame = data_structure[col].reset_index(level=dt_index).groupby(level=indexes, group_keys=False)\
                .apply(lambda x: _resample_and_interpolate_group(df=x, dt_col=dt_index,
                                                                 ld=ld, la=la, m=m, lim=lim,
                                                                 freq=time_bin, origin=resample_origin, label=resample_label,
                                                                 fv=fv, agg_f=agg_f, name=col,
                                                                 agg_names=agg_names,
                                                                 display=logging_kwargs.get('display', False),
                                                                 log_name=logging_kwargs.get('log_name', '') + f'.column_{col}'))
        else:  # when the index is a single datetime index
            base: pd.DataFrame = _interpolate(ds=_apply_agg_f(ds=data_structure[col]
                                                              .resample(time_bin,
                                                                        origin=resample_origin,
                                                                        label=resample_label),
                                                              agg_f=agg_f, agg_names=agg_names, name=col),
                                              ld=ld, la=la, m=m, lim=lim, fv=fv, name=col, **logging_kwargs)

        if isinstance(out, pd.DataFrame):
            out = out.merge(base, right_index=True, left_index=True)
        else:
            out = base

    return out


def _apply_agg_f(ds: Union[pd.Series, pd.DataFrame], agg_f, agg_names: list, name: str) -> Union[pd.Series, pd.DataFrame]:

    try:
        out = agg_f(ds)  # For functions compatiable with seires groupby objects Much faster
    except AttributeError:
        out = ds.apply(agg_f)  # For functions not compatiable with seires groupby objects

    if isinstance(out, pd.Series):
        out: pd.DataFrame = pd.DataFrame(out)

    if agg_f.__name__ != '_numeric_aggregators':
        if isinstance(agg_names, list):
            out.columns = agg_names

    if agg_f.__name__ not in ['_numeric_aggregators', '_default_non_numeric_agg']:
        out = out.merge(_default_non_numeric_agg(ds), left_index=True, right_index=True)

    # fill count columns with 0
    out[f'{name}_count'].fillna(0, inplace=True)

    return out


def _interpolate(ds: Union[pd.Series, pd.DataFrame], ld: str, la: str, m: str, lim: int, fv: any, name: str, **logging_kwargs) -> Union[pd.Series, pd.DataFrame]:

    if fv is not None:
        return ds.fillna(fv)

    if isinstance(ds, pd.DataFrame):
        if not bool(re.search(r'int|float', ','.join(ds.dtypes.astype(str)[ds.columns.difference([f'{name}_count'])]))):
            ds['xxxdummycolxxx'] = 0

        # interpolate of fill other columns
        try:
            ds.loc[:, ds.columns.difference([f'{name}_count'])] = ds.loc[:, ds.columns.difference([f'{name}_count'])]\
                .interpolate(limit_direction=ld,
                             limit_area=la,
                             method=m,
                             limit=lim)
        except Exception as e:
            logm(ds.columns, **logging_kwargs)
            raise Exception(e)

    return ds.drop(columns=['xxxdummycolxxx'], errors='ignore')


def _resample_and_interpolate_group(df: pd.DataFrame, dt_col: str,
                                    ld: str, la: str, m: str, lim: int,
                                    freq: str, origin: str, label: str,
                                    fv: any, agg_f: callable, name: str,
                                    agg_names: list, **logging_kwargs):

    ds = df.set_index(dt_col)[name]\
        .resample(freq, origin=origin, label=label)

    return _interpolate(ds=_apply_agg_f(ds=ds, agg_f=agg_f, agg_names=agg_names, name=name),
                        ld=ld, la=la, m=m, lim=lim, fv=fv, name=name, **logging_kwargs)


def _run_process(training_df: pd.DataFrame,
                 training_run: bool,
                 df: pd.DataFrame,
                 stacked_meas_value: str,
                 stacked_meas_name: str,
                 skip_imputation: bool,
                 skip_encoding_scaling: bool,
                 skip_clip: bool,
                 required_cols: list,
                 debug_column: str,
                 id_index: str,
                 default_na_values: list,
                 default_missing_value_numeric: str,
                 default_missing_value_binary: str,
                 default_other_value_binary: str,
                 default_missing_value_cat: str,
                 default_other_value_cat: str,
                 default_case_standardization: str,
                 default_min_num_cat_levels: int,
                 default_one_hot_embedding_threshold: int,
                 default_missingness_threshold: float,
                 default_lower_limit_percentile: float,
                 default_scale_values: bool,
                 default_upper_limit_percentile: float,
                 default_fill_lower_upper_bound_percentile: float,
                 default_fill_upper_lower_bound_percentile: float,
                 default_ensure_col: bool,
                 default_dtype: str,
                 encoder_dir: str,
                 master_config_dict: dict,
                 train_ids: list,
                 time_index_col: str,
                 file_type: str,
                 pre_resample: bool = True,
                 generate_missing_indicators: bool = True,
                 **process_df_log_kwargs) -> tuple:
    # debug_inputs(function=_run_process, dump_fp='run_process.p', kwargs=locals())
    if not isinstance(training_df, pd.DataFrame):

        output_cols: list = ['column_name', 'source_dtype', 'output_dtype', 'train_missingness',
                             'overall_missingness', 'drop_column', 'levels', 'standardization_values', 'mean', 'median', 'std',
                             'mad', 'max', 'min', 'mode', 'raw_min_value', 'raw_max_value', 'value_counts', 'raw_value_counts', 'na_values',
                             'case_standardization', 'missing_value', 'other_value',
                             'missingness_threshold', 'min_categorical_count', 'lower_limit_value',
                             'upper_limit_value', 'fill_lower_upper_bound_value',
                             'fill_upper_lower_bound_value',
                             'lower_percentile', 'cat_encoder_fp',
                             'scale_values', 'scaler_fp', 'ensure_col',
                             'lower_than_theshold_counts', 'one_hot_embedding_threshold',
                             'lower_limit_percentile', 'upper_limit_percentile',
                             'fill_lower_upper_bound_percentile',
                             'fill_upper_lower_bound_percentile']

        c_list: list = []
        train_cols: list = ((required_cols + [debug_column]) if isinstance(debug_column, str) else
                            (required_cols + df[stacked_meas_name].dropna().unique().tolist()) if (stacked_meas_name in df.columns) else
                            df.columns.tolist())

        for col in train_cols:
            if col in required_cols:
                c_list.append(pd.DataFrame({'column_name': [col],
                                            'source_dtype': [str(df[col].dtype)],
                                            'output_dtype': ['time_index' if col == time_index_col else 'id_index' if col == id_index else 'index_column']}))
            else:
                spec_dict: dict = master_config_dict.get(col, {})
                c_list.append(pd.DataFrame({'column_name': [col],
                                            'source_dtype': [str(df[stacked_meas_name if ((stacked_meas_name in df.columns) and (col not in required_cols)) else col].dtype)],
                                            'output_dtype': [spec_dict.get('pre_resample_output_dtype', 'int' if bool(re.search(r'_count$', col, re.IGNORECASE)) else
                                                                           default_dtype) if (pre_resample and isinstance(time_index_col, str)) else
                                                             spec_dict.get('output_dtype', 'int' if bool(re.search(r'_count$', col, re.IGNORECASE)) else default_dtype)],
                                            'drop_column': [spec_dict.get('drop_column')],
                                            'ensure_col': [spec_dict.get('ensure_col', default_ensure_col)],
                                            'standardization_values': [spec_dict.get('standardization_values')],
                                            'na_values': [None if spec_dict.get('drop_column', False) else spec_dict.get('na_values', default_na_values)],
                                            'case_standardization': [spec_dict.get('case_standardization', default_case_standardization)],
                                            'numeric_missing_value': [spec_dict.get('missing_value', default_missing_value_numeric)],
                                            'binary_missing_value': [spec_dict.get('missing_value', default_missing_value_binary)],
                                            'cat_missing_value': [spec_dict.get('missing_value', default_missing_value_cat)],
                                            'other_value': [spec_dict.get('other_value', default_other_value_cat)],
                                            'binary_other_value': [spec_dict.get('other_value', default_other_value_binary)],
                                            'missingness_threshold': [spec_dict.get('missingness_threshold', default_missingness_threshold)],
                                            'min_categorical_count': [spec_dict.get('min_categorical_count', default_min_num_cat_levels)],
                                            'cat_encoder_fp': [None if skip_encoding_scaling else os.path.join(encoder_dir, f'{col}_cat_encoder.bin') if encoder_dir is not None else None],
                                            'lower_limit_percentile': [spec_dict.get('lower_limit_percentile', default_lower_limit_percentile)],
                                            'upper_limit_percentile': [spec_dict.get('upper_limit_percentile', default_upper_limit_percentile)],
                                            'fill_lower_upper_bound_percentile': [spec_dict.get('fill_lower_upper_bound_percentile', default_fill_lower_upper_bound_percentile)],
                                            'fill_upper_lower_bound_percentile': [spec_dict.get('fill_upper_lower_bound_percentile', default_fill_upper_lower_bound_percentile)],
                                            'scale_values': [False if skip_encoding_scaling else spec_dict.get('scale_values', default_scale_values)],
                                            'scaler_fp': [None if skip_encoding_scaling else os.path.join(encoder_dir, f'{col}_standard_scaler.bin') if encoder_dir is not None else None],
                                            'one_hot_embedding_threshold': [spec_dict.get('one_hot_embedding_threshold', default_one_hot_embedding_threshold)]}))
        training_df: pd.DataFrame = pd.concat(c_list, axis=0, sort=False, ignore_index=True)

        for c in [x for x in output_cols if x not in training_df.columns]:
            training_df[c] = None
    else:
        training_df = training_df.query('pre_resample.isnull() | pre_resample.isin(["1", "1.0", 1])' if pre_resample else 'pre_resample.isin(["0", "0.0", 0])',
                                        engine='python')

    if isinstance(train_ids, list) or isinstance(train_ids, pd.Series):
        if (id_index == str(df.index.name)):
            train_idx: pd.Series = pd.Series(df.index.isin(train_ids), index=df.index)
        elif (id_index in df.index.names):
            train_idx: pd.Series = pd.Series(df.index.get_level_values(df.index.names.index(id_index)).isin(train_ids), index=df.index)
        else:
            assert id_index in df.columns, 'id_index is required when train ids are passed'

            train_idx: pd.Series = df[id_index].isin(train_ids)
    else:
        train_idx: pd.Series = pd.Series([True] * df.shape[0], index=df.index)

    # if (not isinstance(time_index_col, str)) and isinstance(id_index, str) and (stacked_meas_name in df.columns) and (stacked_meas_value in df.columns):
    #     df = df.pivot(index=required_cols, columns=stacked_meas_name)[stacked_meas_value].reset_index(drop=False)

    index_req_cols: list = pd.Index(required_cols).intersection(df.index.names if isinstance(df.index, pd.MultiIndex) else [df.index.name]).tolist()
    col_req_cols: list = pd.Index(required_cols).intersection(df.columns).tolist()
    req_dif: list = pd.Index(required_cols).difference(index_req_cols + col_req_cols)
    assert len(req_dif) == 0, f'There following required columns were not found: {req_dif}'

    out: pd.DataFrame = df[col_req_cols] if len(col_req_cols) > 0 else pd.DataFrame(index=df.index)

    amended_training_df = training_df.copy()

    for idx, row in training_df.iterrows():

        if row.drop_column in [True, "True", "true", '1', '1.0', 1]:
            if training_run:
                amended_training_df.loc[idx, 'drop_column'] = True
        elif (row.output_dtype in ['time_index', 'id_index', 'index_column', 'parameter']) or ('missing_ind' in row.column_name):
            pass
        else:

            if stacked_meas_name in df.columns:
                union_idx: pd.Series = (df[stacked_meas_name] == row.column_name)
                t_idx: pd.Series = train_idx[union_idx]
                s: pd.Series = df.loc[union_idx, stacked_meas_value].copy().rename(row.column_name)
            else:
                t_idx: pd.Series = train_idx
                s: pd.Series = df[row.column_name].copy() if row.column_name in df.columns else pd.Series(index=df.index,
                                                                                                          name=row.column_name)

            cdict: dict = row.dropna().to_dict()

            if not training_run:
                for k in set(['levels', 'na_values']).intersection(cdict.keys()):
                    if isinstance(cdict[k], str):
                        cdict[k] = cdict[k].split('XXXXSEPXXXX')

                for k in set(['standardization_values', 'lower_than_theshold_counts', 'value_counts']).intersection(cdict.keys()):
                    if isinstance(cdict[k], str):
                        cdict[k] = pd.Series(json.loads(cdict[k]))

                if 'missing_value' in cdict:
                    cdict['cat_missing_value' if 'cat' in cdict.get('output_dtype')
                          else 'binary_missing_value' if 'binary' == cdict.get('output_dtype')
                          else 'numeric_missing_value'] = cdict.pop('missing_value')

            result, meta = _process_column(series=s,
                                           config_dict=cdict,
                                           train_idx=t_idx,
                                           training_run=training_run,
                                           skip_imputation=skip_imputation,
                                           skip_encoding_scaling=skip_encoding_scaling,
                                           skip_clip=skip_clip,
                                           ensure_series=row.dropna().to_dict().get('ensure_col', False),
                                           **process_df_log_kwargs)

            if result is not None:
                # concatenate previous output, result, and missingness indicator (if there are atleast some missing values in the training set)
                if (generate_missing_indicators
                        and ((meta['missing_idx'][train_idx].nunique() == 2) or (meta['missing_idx'].name in training_df.column_name.tolist()))):
                    out = pd.concat([out, result, meta['missing_idx']],
                                    axis=1, ignore_index=False)
                    amended_training_df = amended_training_df.append(pd.DataFrame({'column_name': [meta['missing_idx'].name],
                                                                                   'output_dtype': ['binary']}))
                else:
                    out = pd.concat([out, result],
                                    axis=1, ignore_index=False)

            for k, v in meta.items():
                if k == 'missing_idx':
                    continue

                if training_run:
                    amended_training_df.loc[idx, k] = 'XXXXSEPXXXX'.join([str(x) for x in v]) if isinstance(v, list) else json.dumps(
                        v) if isinstance(v, dict) else json.dumps(v.to_dict()) if isinstance(v, pd.Series) else v

    if training_run:
        if (id_index is not None):
            amended_training_df.loc[-1, ['column_name', 'output_dtype']] = [id_index, 'id_index']

        # cleanup columns
        amended_training_df: pd.DataFrame = amended_training_df[output_cols]

    return out, amended_training_df


def nih_race_ethncity(df: pd.DataFrame, ethnicity_col: str = 'ethnicity', race_col: str = 'race', sex_col: str = 'sex') -> pd.DataFrame:
    """
    Format Dataframe values into NIH standard.

    Parameters
    ----------
    df : pd.DataFrame
        DESCRIPTION.
    race_col : str, optional
        DESCRIPTION. The default is 'race'.
    ethnicity_col : str, optional
        DESCRIPTION. The default is 'ethnicity'.
    sex_col : str, optional
        DESCRIPTION. The default is 'sex'.

    Returns
    -------
    df : TYPE
        DESCRIPTION.

    """
    if ((ethnicity_col in df.columns) and (race_col in df.columns)):
        df.loc[((df[ethnicity_col].astype(str).str.contains('HISPANIC', flags=re.IGNORECASE, na=False)
                 & ~df[ethnicity_col].astype(str).str.contains('NOT', flags=re.IGNORECASE, na=False))
                | df[race_col].astype(str).str.contains('HISPANIC', flags=re.IGNORECASE, na=False)), ethnicity_col] = 'Hispanic or Latino'

    if race_col in df.columns:
        df.loc[df[race_col].isin(['AMERICAN INDIAN']), race_col] = 'American Indian/Alaska Native'

        df.loc[df[race_col].isin(['ASIAN']), race_col] = 'Asian'

        df.loc[df[race_col].isin(['PACIFIC ISLANDER']), race_col] = 'Native Hawaiian or Other Pacific Islander'

        df.loc[df[race_col].isin(['BLACK', 'BLACK HISPANIC']), race_col] = 'Black or African American'

        df.loc[(df[race_col].isin(['WHITE HISPANIC', 'WHITE', 'HISPANIC'])), race_col] = 'White'

        df.loc[df[race_col].isin(['MULTIRACIAL']), race_col] = 'More Than One Race'

        df.loc[df[race_col].isin(['OTHER', 'PATIENT REFUSED', 'UNKNOWN', '?', '??', None, '', np.nan]) | df[race_col].isnull(), race_col] = 'Unknnown or Not Reported'

        df.loc[df[ethnicity_col].isin(['NOT HISPANIC']), ethnicity_col] = 'Not Hispanic or Latino'

        df.loc[df[ethnicity_col].isin(['PATIENT REFUSED', 'UNKNOWN', '?', '??', None, '', np.nan]) | df[ethnicity_col].isnull(), ethnicity_col] = 'Unknown/Not Reported Ethnicity'

    if sex_col in df.columns:
        df.loc[df[sex_col].isin(['FEMALE']), sex_col] = 'Female'

        df.loc[df[sex_col].isin(['MALE']), sex_col] = 'Male'

        df.loc[~df[sex_col].isin(['Male', 'Female']), sex_col] = 'Unknown/Not Reported'

    return df


def backward_roll(df: pd.DataFrame, group_cols: Union[list, str], window: Union[int, str], on: str,
                  agg_func: callable, shift: int, last_value: any = None, value_cols: list = None):
    """
    Rolldataframe bawords e.g. Look forward instead of backward.

    Parameters
    ----------
    df : pd.DataFrame
        DESCRIPTION.
    group_cols : Union[list, str]
        DESCRIPTION.
    window : Union[int, str]
        DESCRIPTION.
    on : str
        DESCRIPTION.
    agg_func : callable
        DESCRIPTION.
    shift : int
        DESCRIPTION.
    last_value : any, optional
        DESCRIPTION. The default is None.
    value_cols : list, optional
        DESCRIPTION. The default is None.

    Returns
    -------
    pd.DataFrame
        DESCRIPTION.

    """
    t = df.groupby(group_cols, group_keys=False).rolling(on=on, closed='right', window=window, min_periods=0).apply(agg_func)

    if value_cols is None:
        value_cols: list = [x for x in t.columns.tolist() if x != on]

    lv_bf_shift: pd.DataFrame = t.groupby(group_cols, group_keys=False)[value_cols].tail(1)
    lv_bf_shift.index = t.groupby(group_cols, group_keys=False)[value_cols].head(1).index

    t.loc[:, value_cols] = t.groupby(group_cols, group_keys=False)[value_cols].shift(shift)

    t.loc[t.groupby(group_cols, group_keys=False)[value_cols].tail(1).index, value_cols] = last_value

    fv_af_shift = t.groupby(group_cols, group_keys=False)[value_cols].head(1)

    fv_af_shift = fv_af_shift[fv_af_shift.isnull().all(axis=1)]

    lv_bf_shift = lv_bf_shift.loc[fv_af_shift.index, :]

    t.loc[lv_bf_shift.index, value_cols] = lv_bf_shift[value_cols]

    t.loc[:, value_cols] = t.groupby(group_cols, group_keys=False)[value_cols].ffill()

    return t.reset_index(level=group_cols)


