"""
Module containing common dataformat manipuations.

Author: Ruppert20
"""
import numpy as np
import pandas as pd
try:
    import cudf
    import dask_cudf
    import cupy as cp
except ImportError or ModuleNotFoundError:
    pass
import dask.dataframe as dd
import logging
import re
from collections import namedtuple
from collections.abc import Iterable
import os
from sqlalchemy.engine.base import Engine
from unidecode import unidecode
from sqlalchemy.types import Integer, Date, DateTime, Float, CHAR, VARCHAR, NCHAR, NVARCHAR, Time,\
    SMALLINT, BIGINT, TEXT, DECIMAL, Enum, JSON, TIMESTAMP, BINARY, VARBINARY, BLOB, NUMERIC
from sqlalchemy.dialects.mssql import BIT
from sqlalchemy.dialects.mysql import LONGTEXT, DOUBLE, MEDIUMTEXT, SET
from typing import List, Union
from .aggregation_functions import nan_tolerant_min


def force_datetime(ds, date_cols: list = None, **kwargs):
    """Coerce values to datetimes using to_datetime function."""
    typedict = getDataStructureLib(ds)
    kwargs['errors'] = kwargs.pop('errors', 'coerce')
    kwargs['fillnaVal'] = kwargs.pop('fillnaVal', pd.NaT)

    if isinstance(date_cols, list) and typedict['type'] == 'dataframe':
        ds[date_cols] = force_datetime(ds[date_cols], date_cols=None, **kwargs, axis=0)
        return ds

    return apply_func(ds, func=pd.to_datetime if typedict['cpul'] else cudf.to_datetime, coerce_to_Series=False, **kwargs)


def check_format_series(ds: pd.Series, desired_type: any = None, conversion_func: callable = None, **kwargs):
    """
    Check the format of a pandas series and convert it to the desired type (if necessary).

    Parameters
    ----------
    ds : Union[pd.Series, cudf.Series]
        Input DataSeries.
    desired_type : any, optional
        desired type. The default is None.
    conversion_func : callable, optional
        custom function to convert the series to the desired type. The default is None.
    **kwargs : TYPE
        kwargs for the conversion function.


    Returns
    -------
    Input series in the desired format.

    """
    if desired_type == 'extract_num':
        ds = ds.apply(extract_num)
        desired_type = 'float'
    elif desired_type is None:
        desired_type = get_column_type(series=ds, one_hot_threshold=kwargs.get('one_hot_threshold', 5), downcast_floats=kwargs.get('downcast_floats', False))
        if ('desired_type' == 'int') and ds.isnull().any():
            desired_type: str = 'sparse_int'

    if (str(ds.dtype) == desired_type) or (ds.dtype == desired_type):
        return ds
    elif pd.notnull(conversion_func):
        return conversion_func(ds, **kwargs)
    elif desired_type in ['binary_indicator']:
        return ds.notnull()
    elif desired_type in ['datetime64[ns]', 'date', 'datetime', 'timestamp']:
        return force_datetime(ds, **kwargs).dt.date if desired_type == 'date' else force_datetime(ds, **kwargs)
    elif desired_type in ['float', 'float64', 'float32', 'float16', 'float8']:
        return force_numeric(ds, **kwargs)
    elif desired_type in ['int', 'int64', 'int32', 'int16', 'int8']:
        return force_numeric(ds, downcast=kwargs.pop('downcast', 'integer'), **kwargs)
    elif desired_type in ['str', 'cat_one_hot', 'cat_embedding', 'binary', 'sparse_int', 'cat_str', 'cat_top_n']:
        if str(ds.dtype) == 'category':
            ds = ds.cat.add_categories(['-99999', '-99999XXXXXX999999'])
        try:
            if kwargs.pop('format_sparse_int', False) or (desired_type in ['binary', 'sparse_int', 'cat_one_hot', 'cat_embedding', 'cat_top_n']):
                tp: pd.Series = ds.fillna('-99999').astype(float).astype(int).astype(str).replace({'-99999': None})
                if desired_type == 'cat_top_n':
                    return keep_top_n(series=tp, **kwargs)
                else:
                    return tp
        except ValueError as ve:
            if kwargs.pop('format_sparse_int', False) or (desired_type in ['binary', 'sparse_int']):
                logging.warning(ve)
                logging.warning('Falling Back to str formating')
        tp: pd.Series = ds.fillna('-99999XXXXXX999999').astype(str).replace({'-99999XXXXXX999999': None})
        if desired_type == 'cat_top_n':
            return keep_top_n(series=tp, **kwargs)
        else:
            return tp
    elif desired_type in ['object']:
        return ds.fillna('-99999XXXXXX999999').astype(object).replace({'-99999XXXXXX999999': None})
    else:
        logging.error(f'Implicit Conversion to {desired_type}, is currently not supported, please provide a conversion function')
        raise Exception(f'Implicit Conversion to {desired_type}, is currently not supported, please provide a conversion function')


supported_dtypes: List[str] = ['str', 'object', 'int', 'float', 'cat_one_hot', 'cat_embedding', 'binary', 'datetime', 'timestamp', 'cat_str', 'sparse_int', 'cat_top_n']


def keep_top_n(series: pd.Series, top_n: int, top_n_values: List[any] = None, other_unknown_cat: any = 'OTHER/UNKNOWN', fillnans: bool = True, **kwargs) -> pd.Series:
    """
    Keep only top n levels and convert remaining levels to other_unknown_cat.

    Parameters
    ----------
    series : pd.Series
        input series.
    top_n : int
        Number of levels to keep (not including other_unknown_cat catch all other level).
    top_n_values : List[any], optional
        predefined list of values. The default is None.
    other_unknown_cat : any, optional
        The value to be used to fill values not in the top n. The default is 'OTHER/UNKNOWN'.
    fillnans : bool, optional
        Whether Null values should be filled with the other_unknown_cat or left NULL. The default is True, which will fill the Nulls.

    Returns
    -------
    pd.Series
        Processed Pandas series containing only the top_n categories with others converted to other_unknown_cat.

    """
    if isinstance(top_n_values, list):
        pass
    else:
        vc: pd.Series = series.value_counts()

        # remove unknowns/others
        un_ot_index: pd.Series = vc.index.str.contains(r'other|unknown', case=False)
        if un_ot_index.any():
            un_ot_values: list = vc[un_ot_index].index.tolist()
            vc = vc[~un_ot_index]
            series.replace({x: other_unknown_cat for x in un_ot_values}, inplace=True)

        if len(vc) <= top_n:
            if fillnans:
                return series.fillna(other_unknown_cat)
            else:
                return series

        top_n_values: list = vc.index[0:top_n].tolist()

    ot_index: pd.Series = ~series.isin(top_n_values)
    if not fillnans:
        ot_index: pd.Series = ot_index & series.notnull()
    if ot_index.any():
        series[ot_index] = other_unknown_cat
    return series


def _format_time(input_s: str) -> str:

    if pd.notnull(input_s):

        try:
            t = str(input_s).strip().replace(':', '').zfill(4)
            return t[:2] + ':' + t[2:]
        except:
            pass
    return '00:00'


def apply_date_censor(censor_date: str, df: pd.DataFrame, df_time_index: Union[List[str], str]) -> pd.DataFrame:
    """
    Filter datetime column by keeping all observtions up until the cesnor date.

    Parameters
    ----------
    censor_date : str
        Maximum date to retain.
    df : pd.DataFrame
        Pandas Dataframe with atleast the following columns.
            *df_time_index (datetime)
    df_time_index : Union[List[str], str]
        The column in the df to filter by. It my also be a list, where the values are used in order of preference of the first not null value from the list in each row.

    Returns
    -------
    pd.DataFrame
        Dataframe filtered by time.

    """
    if isinstance(df_time_index, list):
        return df[(df[df_time_index].apply(lambda row: coalesce(*pd.to_datetime(row[df_time_index], errors='coerce')).date(), axis=1) <= pd.to_datetime(censor_date).date())]
    return df[(df[df_time_index].dt.date <= pd.to_datetime(censor_date).date())]


file_components = namedtuple('file_components', 'directory file_name batch_numbers file_type optimized bs_sep')


def get_file_name_components(file_path: str) -> tuple:
    """Extract directory, file_name, batch_numbers, and file type from a file_path."""
    if bool(re.search(r'\\|/', file_path)):
        directory: str = os.path.dirname(file_path)

        file_path: str = os.path.basename(file_path)
    else:
        directory = None

    if '.' in file_path:
        file_type: str = file_path[file_path.rfind('.'):]
        file_path: str = file_path.replace(file_type, '')
    else:
        file_type = None

    optimized: bool = bool(re.search(r'_optimized_ids$', file_path, re.IGNORECASE))
    if optimized:
        file_path: str = re.sub(r'_optimized_ids$', '', file_path, re.IGNORECASE)

    if bool(re.search(r'_[0-9]+_[0-9]+$', file_path)):
        file_name: str = re.sub(r'_[0-9]+_[0-9]+$', '', file_path)

        batch_nums: list = re.search(r'_[0-9]+_[0-9]+$', file_path).group(0)[1:].split('_')
    elif bool(re.search(r'_[0-9]+_[0-9]+_chunk_[0-9]+$', file_path)):
        file_name: str = re.sub(r'_[0-9]+_[0-9]+_chunk_[0-9]+$', '', file_path)

        batch_nums: list = re.search(r'_[0-9]+_[0-9]+_chunk_[0-9]+$', file_path).group(0)[1:].split('_')
    elif bool(re.search(r'_[0-9]+_chunk_[0-9]+$', file_path)):
        file_name: str = re.sub(r'_[0-9]+_chunk_[0-9]+$', '', file_path)

        batch_nums: list = re.search(r'_[0-9]+_chunk_[0-9]+$', file_path).group(0)[1:].split('_')
    elif bool(re.search(r'_[0-9]+$', file_path)):
        file_name: str = re.sub(r'_[0-9]+$', '', file_path)

        batch_nums: list = [re.search(r'_[0-9]+$', file_path).group(0)[1:]]
    elif bool(re.search(r'_[0-9]+_[a-z]+_stage$|_[0-9]+_[0-9]+_[a-z]+_stage$', file_path)):

        temp_batches = re.search(r'_[0-9]+_[a-z]+_stage$|_[0-9]+_[0-9]+_[a-z]+_stage$', file_path).group()
        file_name = file_path.replace(temp_batches, '')

        batch_nums: list = re.findall(r'[0-9]+', temp_batches)
    else:
        file_name = file_path
        batch_nums: list = []

    # format to integer array
    if 'chunk' in batch_nums:
        bs_sep: str = '_chunk_'
    elif 'subset' in batch_nums:
        bs_sep: str = '_subset_'
    else:
        bs_sep: str = '_'

    batch_nums = [int(x) for x in batch_nums if (('chunk' not in x) and ('subset' not in x))]

    return file_components(directory, file_name, batch_nums, file_type, optimized, bs_sep)


def tokenize_id(input_str: str, token_index: int = None, delimeter: str = '_', ignore_errors: bool = False) -> int:
    """
    Split identifier using a predefined delimeter.

    Parameters
    ----------
    input_str : str
        DESCRIPTION.
    token_index : int, optional
        Whether to return a list of tokens or a specific token based on position in the resultant list. The default is None.
    delimeter : str, optional
        character(s) used to split the string. The default is '_'.

    Raises
    ------
    Exception
        DESCRIPTION.

    Returns
    -------
    int
        DESCRIPTION.

    """
    if pd.isnull(input_str) or input_str in ['nan', 'None']:
        return None
    tokens = str(input_str).split(delimeter)

    if len(tokens) == 1:
        if isfloat(input_str):
            return str(int(float(input_str)))
        else:
            return input_str

    try:
        return str(int(float(tokens[token_index])))
    except:
        if ignore_errors:
            return input_str

        raise Exception(f'Invalid ID string: {input_str}')


def isfloat(value):
    """Check if value is a float."""
    if pd.isnull(value):
        return False
    try:
        float(value)
        return True
    except ValueError:
        return False


def remove_illegal_characters(string: str, preserve_case: bool, preserve_decimals: bool):
    """Strip illegal characters from column names."""
    temp = str(string).replace('?', '').replace('-', '_')\
        .replace('\\', '_').replace('(', '')\
        .replace(')', '').replace('/', '_')\
        .replace(' ', '_').replace(':', '_')\
        .replace("'", '').replace('__', '_')\
        .replace('+', '_and_')\
        .replace('*', 'asterisk')\
        .replace('1st', 'first')\
        .replace('_datatime', '_datetime')

    if not preserve_decimals:
        temp = temp.replace('.', '')

    if preserve_case:
        return temp

    return camel_to_snake_case(temp).lower()  # \
    # .replace('c_p_t_', 'cpt_')\
    # .replace('_p_o_a', '_poa')\
    # .replace('_m_i', '_mi')\
    # .replace('_c_o_p_d', '_copd')\
    # .replace('_c_h_f', '_chf')\
    # .replace('_h_i_v', '_hiv')\
    # .replace('_e_d_', '_ed_')\
    # .replace('s_s_d_i', 'ssdi')\
    # .replace('s_d_d_i_', 'ssdi_')\
    # .replace('i_c_d', 'icd')\
    # .replace('p_o_a', 'poa')\
    # .replace('_o_r_', '_or_')\
    # .replace('_c_h_a_r', '_char')\
    # .replace('n_or_a_', 'nora_')\
    # .replace('_y_n', '_yn')\
    # .replace('g_i_', 'gi_')\
    # .replace('e_d_', 'ed_')


def sanatize_columns(df: Union[pd.DataFrame, list, pd.Series], preserve_case: bool, preserve_decimals: bool) -> pd.DataFrame:
    """Ensure column names are formatted correctly."""
    if isinstance(df, pd.DataFrame):
        for col in list(df.columns):
    
            if col == 'anti-embolism intervention':
                if 'anti_embolism_intervention' in df.columns:
                    df.drop(columns=['anti_embolism_intervention'], inplace=True)
    
                df.rename(columns={'anti-embolism intervention': 'anti_embolism_intervention'}, inplace=True)
            elif col == 'anti_embolism_intervention.1':
                df.drop(columns=['anti_embolism_intervention.1'], inplace=True)

        df.columns = pd.Series(list(df.columns))\
            .apply(remove_illegal_characters,
                   preserve_case=preserve_case,
                   preserve_decimals=preserve_decimals)
        return df
    elif isinstance(df, (list, pd.Series)):
        return (pd.Series(df) if isinstance(df, list) else df)\
            .apply(remove_illegal_characters,
                   preserve_case=preserve_case,
                   preserve_decimals=preserve_decimals)
    else:
        raise NotImplementedError(f'Column Sanitation is not currently supported for inputs of type {type(df)}')


def camel_to_snake_case(input_str: str):
    """Convert Camel Case to Snake Case."""
    result: str = ''
    for i, l in enumerate(input_str):
        if l.isupper():
            try:
                if input_str[i - 1].islower() or input_str[i + 1].islower():
                    result += '_'
            except IndexError:
                pass
        result += l.lower()
    return result.lstrip('_').replace('__', '_')


def prepare_table_for_upload(df: pd.DataFrame,
                             engine: Engine,
                             table: str,
                             schema: str,
                             truncate_to_fit: bool = False,
                             force_type_conformance: bool = True,
                             get_dtypes_only: bool = False,
                             dtypes: dict = None,
                             skip_character_check: bool = False,
                             upload_floats_as_float: bool = True,
                             upload_ints_as_float: bool = False,
                             allow_silent_truncation: bool = False,
                             trim_cols_to_match_table: bool = False) -> tuple:
    """
    Format table for upload to SQL Server Table.

    Parameters
    ----------
    df : pd.DataFrame
        DESCRIPTION.
    engine : Engine
        DESCRIPTION.
    table : str
        DESCRIPTION.
    schema : str
        DESCRIPTION.
    truncate_to_fit : bool, optional
        DESCRIPTION. The default is False.
    force_type_conformance : bool, optional
        DESCRIPTION. The default is True.
    error_dir : str, optional
        DESCRIPTION. The default is None.

    Raises
    ------
    Exception
        DESCRIPTION.

    Returns
    -------
    TYPE
        DESCRIPTION.
    TYPE
        DESCRIPTION.

    """
    if isinstance(dtypes, dict):
        table_info = pd.Series(dtypes).reset_index().rename(columns={'index': 'COLUMN_NAME', 0: 'DATA_TYPE'})

        character_len_specification_mask = (table_info.DATA_TYPE.str.contains(r'\([0-9]+\)', regex=True, na=False)
                                            & table_info.DATA_TYPE.str.contains(r'char', case=False))

        table_info['IS_NULLABLE'] = 'YES'

        table_info['CHARACTER_MAXIMUM_LENGTH'] = None

        if any(character_len_specification_mask):
            table_info.loc[character_len_specification_mask,
                           ['DATA_TYPE', 'CHARACTER_MAXIMUM_LENGTH']] = table_info.loc[character_len_specification_mask,
                                                                                       'DATA_TYPE'].apply(lambda x: pd.Series({'DATA_TYPE': re.sub(r'\([0-9]+\)', '', x),
                                                                                                                               'CHARACTER_MAXIMUM_LENGTH': int(re.search(r'[0-9]+', x).group(0))}))
        table_info.DATA_TYPE = table_info.DATA_TYPE.str.lower()

    elif engine.name == 'mssql':
        if '.' in schema:
            database: str = schema.split('.')[0] + '.'
            schema: str = schema.split('.')[1]
        else:
            database: str = ''
        table_info = pd.read_sql(f'''SELECT
                                        inf.COLUMN_NAME,
                                        inf.DATA_TYPE,
                                        inf.IS_NULLABLE,
                                        COALESCE(inf.CHARACTER_MAXIMUM_LENGTH, DATETIME_PRECISION) [CHARACTER_MAXIMUM_LENGTH]
                                    FROM
                                        {database}sys.tables
                                        INNER JOIN {database}sys.columns ON tables.object_id = columns.object_id
                                        INNER JOIN {database}sys.types ON types.user_type_id = columns.user_type_id
                                        INNER JOIN {database}sys.schemas ON schemas.schema_id = tables.schema_id
                                        INNER JOIN {database}INFORMATION_SCHEMA.COLUMNS inf ON (inf.TABLE_SCHEMA=schemas.name
                                                                                                AND inf.TABLE_NAME=tables.name
                                                                                                AND inf.COLUMN_NAME=columns.name)
                                    WHERE
                                       inf.table_name = '{table}'
                                       AND
                                       inf.TABLE_SCHEMA = '{schema}'
                                       AND
                                       columns.is_identity <> 1;''', engine)
    elif engine.name == 'mysql':
        table_info = pd.read_sql(f'''SELECT
                                         COLUMN_NAME,
                                        DATA_TYPE,
                                        IS_NULLABLE,
                                        COALESCE(CHARACTER_MAXIMUM_LENGTH, DATETIME_PRECISION) as CHARACTER_MAXIMUM_LENGTH
                                    FROM
                                        INFORMATION_SCHEMA.COLUMNS
                                    WHERE
                                        TABLE_NAME = '{table}'
                                        AND
                                        TABLE_SCHEMA = '{schema}'
                                        AND
                                        EXTRA NOT LIKE '%%auto_increment%%';''', engine)
        upload_ints_as_float: bool = True
    elif engine.name == 'postgresql':
        table_info = pd.read_sql(f'''SELECT
                                         column_name as "COLUMN_NAME",
                                         CASE
                                             WHEN data_type = 'character varying' THEN 'varchar'
                                             WHEN data_type = 'time without time zone' THEN 'time'
                                             ELSE data_type END as "DATA_TYPE",
                                         is_nullable as "IS_NULLABLE",
                                         coalesce(character_maximum_length, datetime_precision) as "CHARACTER_MAXIMUM_LENGTH"
                                     from
                                         information_schema.columns
                                     where
                                         table_name = '{table}'
                                         and
                                         table_schema = '{schema}'
                                         and
                                         is_identity <> 'YES';''', engine)
    else:
        raise Exception(f'Unrecognized database type {engine.name}')

    if table_info.shape[0] > 0:

        # make a dep copy
        dtypes = table_info.copy()

        dtypes.DATA_TYPE.replace({'bigint': BIGINT,
                                  'nchar': NCHAR,
                                  'nvarchar': NVARCHAR,
                                  'datetime2': DateTime,
                                  'datetime2(0)': DateTime,
                                  'datetime': DateTime,
                                  'timestamp without time zone': DateTime,
                                  'date': Date,
                                  'float': Float,
                                  'time': Time,
                                  'tinyint': SMALLINT,
                                  'timestamp': TIMESTAMP,
                                  'int': Integer,
                                  'integer': Integer,
                                  'varchar': VARCHAR,
                                  'char': CHAR,
                                  'bit': BIT,
                                  'double': DOUBLE,
                                  'text': TEXT,
                                  'decimal': DECIMAL,
                                  'numeric': NUMERIC,
                                  'long_text': LONGTEXT,
                                  'enum': Enum,
                                  'mediumtext': MEDIUMTEXT,
                                  'json': JSON,
                                  'set': SET,
                                  'binary': BINARY,
                                  'varbinary': VARBINARY,
                                  'blob': BLOB},
                                 inplace=True)

        dtypes.loc[:, 'DATA_TYPE'] = dtypes.loc[:, ['DATA_TYPE', 'CHARACTER_MAXIMUM_LENGTH']]\
            .apply(lambda row: row.DATA_TYPE(row.CHARACTER_MAXIMUM_LENGTH) if (pd.notnull(row.CHARACTER_MAXIMUM_LENGTH) and (row.DATA_TYPE not in [Date, Time])) else row.DATA_TYPE(), axis=1)

        dtypes: dict = dtypes\
            .set_index('COLUMN_NAME').DATA_TYPE.to_dict()
    else:
        dtypes: dict = None

    if get_dtypes_only:
        return dtypes

    problem_df = pd.DataFrame(columns=['column', 'expected', 'observed'])

    if table_info.shape[0] == 0:
        return df.where(pd.notnull(df), None), dtypes, problem_df

    if trim_cols_to_match_table:
        df = df[df.columns.intersection(table_info.COLUMN_NAME)].copy()

    for _, row in table_info.iterrows():

        if row.COLUMN_NAME not in df.columns:
            df[row.COLUMN_NAME] = None
        else:

            if str(df[row.COLUMN_NAME].dtype) == 'object':
                df.loc[:,
                       row.COLUMN_NAME] = df.loc[:,
                                                 row.COLUMN_NAME].fillna('missingxxx').astype(str)\
                    .replace({'nan': None, '30000000|': None, 'missingxxx': None})\
                    .str.replace(r'^30000000\|', '', regex=True).values

            logging.info(f'Checking column: {row.COLUMN_NAME} format')

            if row.DATA_TYPE in ['date', 'datetime', 'time', 'timestamp', 'datetime2', 'timestamp without time zone']:
                # format datetime
                df.loc[:, row.COLUMN_NAME] = force_datetime(df.loc[:, row.COLUMN_NAME]).values

                if row.DATA_TYPE in ['datetime', 'timestamp', 'datetime2']:
                    df.loc[:, row.COLUMN_NAME] = df.loc[:, row.COLUMN_NAME].dt.strftime("%Y-%m-%d %H:%M:%S").values
                elif row.DATA_TYPE in ['time']:
                    df.loc[:, row.COLUMN_NAME] = df.loc[:, row.COLUMN_NAME].dt.strftime("%H:%M:%S").values
                elif row.DATA_TYPE in ['date']:
                    df.loc[:, row.COLUMN_NAME] = df.loc[:, row.COLUMN_NAME].dt.strftime("%Y-%m-%d").values

            elif row.DATA_TYPE in ['bigint', 'int', 'tinyint', 'float', 'double', 'decimal', 'integer', 'numeric']:
                # numeric conversion
                df.loc[:, row.COLUMN_NAME] = pd.to_numeric(df.loc[:, row.COLUMN_NAME].copy(),
                                                           errors='coerce' if force_type_conformance else 'ignore')

                # int conversion to string to preserve int format
                if row.DATA_TYPE in ['bigint', 'int', 'tinyint', 'integer']:

                    if upload_ints_as_float:
                        df.loc[:, row.COLUMN_NAME] = pd.to_numeric(df.loc[:, row.COLUMN_NAME], errors='coerce').values
                        try:
                            df.loc[:, row.COLUMN_NAME] = df.loc[:, row.COLUMN_NAME].astype(int).values
                        except:
                            pass
                    else:
                        df.loc[:, row.COLUMN_NAME] = df.loc[:, row.COLUMN_NAME]\
                            .apply(lambda x: str(int(float(x))) if isfloat(x) else None).values

                if row.DATA_TYPE in ['float', 'double', 'decimal']:
                    if upload_floats_as_float:
                        pass
                    else:
                        df.loc[:, row.COLUMN_NAME] = df.loc[:, row.COLUMN_NAME].apply(lambda x: f'{x:0.3f}' if pd.notnull(x) else None).values

            elif row.DATA_TYPE in ['nchar', 'nvarchar', 'varchar', 'char']:
                if str(df[row.COLUMN_NAME].dtype) == 'category':
                    df.loc[:, row.COLUMN_NAME] = df.loc[:, row.COLUMN_NAME].cat.add_categories(['missingxxx'])
                df.loc[:, row.COLUMN_NAME] = df.loc[:, row.COLUMN_NAME].fillna('missingxxx').astype(str).str.strip().replace({'nan': None, 'missingxxx': None}).values

                if skip_character_check:
                    pass
                else:

                    if row.DATA_TYPE in ['varchar', 'char']:

                        if force_type_conformance:
                            df.loc[:, row.COLUMN_NAME] = df.loc[:, row.COLUMN_NAME]\
                                .fillna('missingxxx').astype(str).apply(unidecode, errors='ignore').replace({'nan': None, 'missingxxx': None}).values

                        elif not all(df[row.COLUMN_NAME].astype(str).apply(_is_ascii)):
                            problem_df = pd.concat([problem_df,
                                                    pd.DataFrame({'column': [row.COLUMN_NAME],
                                                                  'expected': ['ascii'],
                                                                  'observed': ['utf-8']})], axis=0)

                        # if max length greater than 0
                        if row.CHARACTER_MAXIMUM_LENGTH > 0:

                            if all(df[row.COLUMN_NAME].isnull()):
                                max_l: int = 0
                            else:
                                char_lens: pd.Series = df[row.COLUMN_NAME].str.len()
                                max_l: int = char_lens.max()

                            if max_l > row.CHARACTER_MAXIMUM_LENGTH:

                                if (truncate_to_fit and ((row.CHARACTER_MAXIMUM_LENGTH > 3) or allow_silent_truncation)):
                                    logging.info(f'truncating: {row.COLUMN_NAME}')
                                    too_long_mask = (char_lens > row.CHARACTER_MAXIMUM_LENGTH)

                                    if allow_silent_truncation and (row.CHARACTER_MAXIMUM_LENGTH <= 3):
                                        df.loc[too_long_mask, row.COLUMN_NAME] = df.loc[too_long_mask,
                                                                                        row.COLUMN_NAME].str[:int(row.CHARACTER_MAXIMUM_LENGTH)].values
                                    else:
                                        df.loc[too_long_mask, row.COLUMN_NAME] = (df.loc[too_long_mask,
                                                                                         row.COLUMN_NAME].str[:int(row.CHARACTER_MAXIMUM_LENGTH - 3)] + '...').values
                                else:
                                    problem_df = pd.concat([problem_df,
                                                            pd.DataFrame({'column': [row.COLUMN_NAME],
                                                                          'expected': [row.CHARACTER_MAXIMUM_LENGTH],
                                                                          'observed': [max_l]})], axis=0)
            elif row.DATA_TYPE in ['bit']:
                df.loc[:, row.COLUMN_NAME] = df.loc[:, row.COLUMN_NAME]\
                    .apply(lambda x: True if x in [1, '1', '1.0', 'Y',
                                                   'y', 'YES', 'yes', 'Yes'] else False if x in [0, '0', '0.0', 'N',
                                                                                                 'n', 'NO', 'no', 'No'] else None).values
            elif row.DATA_TYPE in ['text']:
                pass
            else:
                raise Exception(f'Unknown Data Format {row.DATA_TYPE}')

        if row.IS_NULLABLE == 'NO':
            if row.DATA_TYPE in ['bigint', 'int', 'float', 'integer']:
                getattr(df, row.COLUMN_NAME).fillna(-999, inplace=True)
            elif row.DATA_TYPE in ['tinyint']:
                getattr(df, row.COLUMN_NAME).fillna(255, inplace=True)
            elif row.DATA_TYPE in ['bit']:
                getattr(df, row.COLUMN_NAME).fillna(False, inplace=True)
            elif row.DATA_TYPE in ['datetime', 'timestamp', 'datetime2']:
                getattr(df, row.COLUMN_NAME).fillna('1970-01-01 00:00:00', inplace=True)
            elif row.DATA_TYPE in ['date']:
                getattr(df, row.COLUMN_NAME).fillna('1970-01-01', inplace=True)
            else:
                getattr(df, row.COLUMN_NAME).fillna('-999', inplace=True)

    return df.where(pd.notnull(df), None), dtypes, problem_df


def _is_ascii(s):
    return all(ord(c) < 128 for c in s)


def force_numeric(ds, **kwargs):
    """Coerce values to numeric using to_numeric function."""
    typedict = getDataStructureLib(ds)
    kwargs['errors'] = kwargs.pop('errors', 'coerce')
    kwargs['fillnaVal'] = kwargs.pop('fillnaVal', pd.NaT)

    num_cols = kwargs.pop('num_cols', None)

    if isinstance(num_cols, list) and typedict['type'] == 'dataframe':
        ds[num_cols] = force_numeric(ds[num_cols], date_cols=None, **kwargs, axis=0)
        return ds

    return apply_func(ds, func=pd.to_numeric if typedict['cpul'] else cudf.to_numeric, coerce_to_Series=False, **kwargs)


def convert_list_to_string(input_list: list, encapsulate_values: bool = False, coercion_func: callable = None):
    """
    Convert a list to a coma seperated string with optional quote encapsulation.

    Parameters
    ----------
    input_list : list
        list of values to be converted.
    encapsulate_values : bool, optional
        whether or not to encapsulate each value in quotations. The default is False.

    Returns
    -------
    TYPE
        DESCRIPTION.

    """
    temp = pd.Series(input_list).drop_duplicates().dropna()

    if isinstance(coercion_func, callable):
        temp = coercion_func(temp)

    return ','.join([f"'{x}'" if encapsulate_values else str(x) for x in temp.tolist()])


def convert_to_comma_seperated_integer_list(input_value: Union[str, int, pd.Series, list]) -> str:
    """
    Convert a single value, list, or pandas Series into a coma seperated string of integers.

    Parameters
    ----------
    input_value : str, int, list, pd.Series
        DESCRIPTION.

    Returns
    -------
    str
        comma seperated list of integers.

    """
    try:
        if isinstance(input_value, str):
            if ',' in input_value:
                id_list = input_value
            else:
                input_value = [str(int(float(input_value)))]
        elif isinstance(input_value, int):
            id_list = [str(input_value)]
        elif isinstance(input_value, list):
            id_list = ','.join(pd.Series(input_value).dropna().astype(float).astype(int).astype(str))
        elif isinstance(input_value, pd.Series):
            id_list = ','.join(input_value.dropna().astype(float).astype(int).astype(str))
        else:
            raise Exception(f'Invalid input format: {input_value}')

        return id_list
    except Exception as e:
        raise Exception(f'Invalid input, {e}, {input_value}')


def move_cols_to_front_back_sort(df: pd.DataFrame, to_front: list = None, to_back: list = None, sort_middle: bool = False) -> pd.DataFrame:
    """
    Organize dataframe columns by placing certain columns at the start, others at the end, and optionally sorting the middle columns alphabetically.

    Parameters
    ----------
    df : pd.DataFrame
        DESCRIPTION.
    to_front : list, optional
        DESCRIPTION. The default is None.
    to_back : list, optional
        DESCRIPTION. The default is None.
    sort_middle : bool, optional
        DESCRIPTION. The default is False.

    Returns
    -------
    TYPE
        DESCRIPTION.

    """
    temp_cols: list = df.columns.tolist()

    if isinstance(to_front, list) and isinstance(to_back, list):
        temp: list = list(set(temp_cols) - set(to_front) - set(to_back))
        temp_cols = to_front + (sorted(temp) if sort_middle else temp) + to_back
    elif isinstance(to_front, list):
        temp: list = list(set(temp_cols) - set(to_front))
        temp_cols = to_front + (sorted(temp) if sort_middle else temp)
    elif isinstance(to_back, list):
        temp: list = list(set(temp_cols) - set(to_back))
        temp_cols = (sorted(temp) if sort_middle else temp) + to_back
    elif sort_middle:
        temp_cols: list = sorted(temp_cols)

    return df.copy()[temp_cols]


def getIndexes(dfObj: pd.DataFrame, value, return_dict: bool = False):
    """Get index positions of value in dataframe i.e. dfObj."""
    if return_dict:
        listOfPos: dict = {}
    else:
        listOfPos = list()
    # Get bool dataframe with True at positions where the given value exists
    result = dfObj.isin([value])
    # Get list of columns that contains the value
    seriesObj = result.any()
    columnNames = list(seriesObj[seriesObj == True].index)
    # Iterate over list of columns and fetch the rows indexes where value exists
    for col in columnNames:
        rows = list(result[col][result[col] == True].index)
        for row in rows:
            if return_dict:
                listOfPos[col] = row
            else:
                listOfPos.append((row, col))
    # Return a list of tuples indicating the positions of value in the dataframe
    return listOfPos


def chunk_list(l: list, n: int) -> list:
    """Split list into segments each containing n elements."""
    for i in range(0, len(l), n):
        yield l[i: i + n]


def create_dict(col_action_dict: dict, merge_encounters_flag: bool = False, merge_or_case_encounters_flag: bool = False,
                start_col: str = None, end_col: str = None) -> dict:

    d: dict = {}
    for key in col_action_dict:
        if key != 'grouping':
            d.update({x: key for x in col_action_dict[key]})

    for col in col_action_dict['grouping']:
        try:
            del d[col]
        except KeyError:
            pass

    if merge_encounters_flag:
        if 'isdead_y_n' in d:
            d['isdead_y_n'] = 'max'

        if 'death_indicator' in d:
            d['death_indicator'] = 'max'

        if 'death_date' in d:
            d['death_date'] = nan_tolerant_min

        if 'index_encounter_y_n' in d:
            d['index_encounter_y_n'] = 'max'

        d['admit'] = nan_tolerant_min
        d['discharge'] = 'max'

    elif merge_or_case_encounters_flag:
        if 'ed_dischg_datetime_all' in d:
            d['ed_dischg_datetime_all'] = 'unique'

    else:
        if isinstance(start_col, str):
            d[start_col] = nan_tolerant_min

        if isinstance(end_col, str):
            d[end_col] = 'max'

    return d


def take_highest_priority_group(input_series: pd.Series, priority_dict: dict, enforce_exaustivity: bool = True) -> str:
    """
    Take the highest priority element from an input series.

    Parameters
    ----------
    input_series : pd.Series
        DESCRIPTION.
    priority_dict : dict
        Dictionary or element priority where the key is an exaustive list of elements in the series and the value is the priority,
        with the highest priority being the lowest number.

    Returns
    -------
    highest priority value.

    """
    assert isinstance(input_series, pd.Series), f'The input to take_highest_priority_group must be a pandas series, but a {type(input_series)} was found'
    if enforce_exaustivity:
        integrity_check: set = set(input_series.dropna().tolist()) - set(priority_dict)
        assert len(integrity_check) == 0, f'There were {len(integrity_check)} values that were not in the priority dict. This included: {integrity_check}'

    return input_series[input_series.apply(lambda x: priority_dict.get(x, 2000)).idxmin()]


def list_intersection(lst1: list, lst2: list) -> list:
    """Return intersection of two lists."""
    lst3: list = [value for value in lst1 if value in lst2]
    return lst3


def _delimeter_agg(s: pd.Series, delimeter: str = '|') -> str:
    return delimeter.join(s.astype(str))


def stack_df(source_df: pd.DataFrame, index_cols: list, value_col: str, label_col: str) -> pd.DataFrame:
    """
    Stack a dataframe that has a shared index. Essentially reverse a melt.

    Parameters
    ----------
    source_df : pd.DataFrame
        DataFrame to be stacked.
    index_cols : list
        list of index columns.
    value_col : str
        value column.
    label_col : str
        label column.

    Returns
    -------
    pd.DataFrame
        pandas dataframe that has been stacked.

    """
    t = source_df.groupby(index_cols + [label_col], group_keys=False).agg({value_col: _delimeter_agg})[value_col].unstack()

    for col in t.columns:
        mask = t[col].str.contains('|', regex=False, na=False)

        if mask.any():
            t2 = t[mask]
            t = t[~mask]

            t3 = t2.merge(pd.DataFrame(t2[col].str.split('|').tolist(), index=t2.index).stack()
                          .droplevel(level=len(index_cols), axis=0).rename(f'new_{col}'), left_index=True, right_index=True)

            t3.loc[:, col] = t3.loc[:, f'new_{col}'].values

            t = pd.concat([t, t3.drop(columns=[f'new_{col}'])], axis=0, sort=False)
            del t2, t3
    return t.reset_index(drop=False)


def deduplicate_and_join(input_s: pd.Series,
                         sep: str = '|',
                         leading: str = '',
                         trailing: str = '',
                         sort: bool = False,
                         cast_1_before_sort: type = None,
                         cast_2_before_sort: type = None,
                         de_duplicate: bool = True) -> str:
    """
    De-duplicate list/array/series and concatenate with seperator.

    Parameters
    ----------
    input_s : pd.Series
        DESCRIPTION.
    sep : str, optional
        DESCRIPTION. The default is '|'.

    Returns
    -------
    str
        DESCRIPTION.

    """
    if isinstance(input_s, list) or isinstance(input_s, np.ndarray):
        input_s = pd.Series(input_s)
    try:
        if isinstance(input_s, cp.ndarray):
            input_s = cudf.Series(input_s)
    except NameError:
        pass

    typedict = getDataStructureLib(input_s)

    if typedict['type'] == 'series':
        if str(input_s.dtype) != 'str':
            input_s = input_s.dropna().astype(str)
        uv = input_s.str.strip().replace({'': None}).dropna().astype(str)
        if de_duplicate:
            uv = uv.unique()

        if isinstance(cast_1_before_sort, type):
            uv = uv.astype(cast_1_before_sort)
        if isinstance(cast_2_before_sort, type):
            uv = uv.astype(cast_2_before_sort)
        if sort:
            uv = np.sort(uv)

        if isinstance(cast_1_before_sort, type) or isinstance(cast_2_before_sort, type):
            uv = uv.astype(str)

        if uv.shape[0] == 0:
            return None
        return leading + sep.join(uv) + trailing
    elif pd.isnull(input_s):
        return None
    elif isinstance(input_s, str):
        return input_s.strip() if input_s.strip() != '' else None
    else:
        return leading + input_s + trailing


def update_cols(df: pd.DataFrame, mask: pd.Series, cols: dict) -> pd.DataFrame:
    for col, valueset in cols.items():
        if mask.any():
            df.loc[mask, col] = valueset['value'] if isinstance(valueset, dict) else valueset
        elif col not in df.columns:
            df.loc[mask, col] = valueset['default'] if isinstance(valueset, dict) else None
    return df


def get_object_name(xx):
    return [objname for objname, oid in globals().items()
            if id(oid) == id(xx)][0]


def _get_df_library(ds):
    """Get package underlying a dataframe."""
    try:
        return cudf if isinstance(ds, cudf.DataFrame) else dask_cudf if isinstance(ds, dask_cudf.DataFrame) else pd if isinstance(ds, pd.DataFrame) else dd if isinstance(ds, dd.DataFrame) else None
    except NameError:
        return pd if isinstance(ds, pd.DataFrame) else dd if isinstance(ds, dd.DataFrame) else None


def _get_series_library(ds):
    """Get package underlying a dataframe."""
    try:
        return pd if isinstance(ds, pd.Series) else cudf if isinstance(ds, cudf.Series) else None
    except NameError:
        return pd if isinstance(ds, pd.DataFrame) else None


def _get_array_libarary(ds):
    try:
        return np if isinstance(ds, np.ndarray) else cp if isinstance(ds, cp.ndarray) else None
    except NameError:
        return np if isinstance(ds, np.ndarray) else None


def getDataStructureLib(ds):
    """Get library underlying a data structure."""
    out: dict = {}

    # determine underling library and type
    arl = _get_array_libarary(ds)
    dfl = _get_df_library(ds)
    srsl = _get_series_library(ds)
    if arl:
        out['type'] = 'array'
        out['lib'] = arl
    elif dfl:
        out['type'] = 'dataframe'
        out['lib'] = dfl
    elif srsl:
        out['type'] = 'series'
        out['lib'] = srsl
    else:
        out['type'] = str(type(ds))
        out['lib'] = type(ds)

    # determine if it is a cpu or cpu library
    try:
        out['cpul'] = False if out['lib'] in [cudf, cp, dask_cudf] else True
    except NameError:
        out['cpul'] = True

    # determine if it is a dask distrubed lib
    try:
        out['distributedl'] = True if out['lib'] in [dd, dask_cudf] else False
    except NameError:
        out['distributedl'] = True if out['lib'] in [dd] else False

    return out


def apply_func(ds: any, func: callable, fillnaVal: object = None, coerce_to_Series: bool = False, labmdaf: bool = False, **kwargs):
    """
    Apply Funciton on a dataStructure.

    Parameters
    ----------
    ds : any
        Data Structure to apply funciton to.
    func : callable
        function to apply.
    kwargs : dict, optional
        Keywork arguments for the function. The default is {}.
    fillnaVal : object, optional
        Value to fillna values with before running. The default is None.
    coerce_to_Series : bool, optional
        force the underling datastructure into a series. The default is False.
    labmdaf : bool, optional
        apply function as a lambda function. The default is False.

    Raises
    ------
    Exception
        DESCRIPTION.

    Returns
    -------
    TYPE
        DESCRIPTION.

    """
    typedict = getDataStructureLib(ds)
    if coerce_to_Series and (typedict['type'] != 'series'):
        ds = pd.Series(ds) if typedict['cpul'] else cudf.Series(ds)

    if typedict['type'] == 'series':
        if fillnaVal is None:
            pass
        else:
            ds = ds.fillna(fillnaVal)

    # necessary for cudf apply only supports numeric types in current release
    if (typedict['type'] == 'dataframe') and (not typedict['cpul']):
        dtypes = ds.dtypes.unique()
        fd = str(dtypes[0])
        apply_func_columnwise: bool = True if ((len(dtypes) > 1) or (fd not in ['int64', 'float64'])) else False
    else:
        apply_func_columnwise: bool = False

    if ((kwargs.get('axis', 2) == 1) and typedict['distributedl']) or (pd.notnull(kwargs.get('axis', None)) and (not typedict['distributedl'])) and (not apply_func_columnwise):
        if labmdaf:
            return ds.apply(lambda x: func(x, **kwargs), axis=kwargs.pop('axis', None))
        return ds.apply(func, **kwargs)
    elif typedict['distributedl'] and (not apply_func_columnwise):
        if pd.isnull(kwargs.get('meta', None)):
            raise Exception('Meta must be provided for distributed functions accross partitions')
        else:
            kwargs.pop('axis')
            print(kwargs)
            return ds.map_partitions(func, **kwargs)
    elif apply_func_columnwise and (typedict['type'] != 'series'):
        for c in ds.columns:
            kwargs.pop('axis', None)
            ds[c] = func(ds[c], **kwargs)
        return ds

    return func(ds, **kwargs)


def convert_to_from_bytes(value: float, unit: str, to_bytes: bool = False) -> float:
    '''
    Function to convert file sizes

    Actions:
    --------------
    1. get conversion factor for unit and multiple or divide appropriately

    Parameters:
    --------------
    value: float
        -file size to convert

    unit: str
        -file size unit

    to_bytes: bool = False
        -whether or not to convert the file size to or from bytes

    Returns:
    --------------
    float which represents the converted file size


    Notes:
    --------------

    '''

    switcher = {'GB': 1024**3,
                'MB': 1024**2,
                'KB': 1024,
                "Bytes": 1
                }

    try:
        if to_bytes:
            return value * switcher.get(unit, None)
        else:
            return value / switcher.get(unit, None)

    except TypeError:
        raise Exception('Invalid unit: str, currently supported units are "GB", "MB","KB", or "Bytes"')


def convert_to_lib(ds, desired_lib: str = 'pandas', reset_index: bool = False):
    olib = getDataStructureLib(ds)['lib']
    olibstring = get_lib_as_string(olib)
    if olibstring == desired_lib:
        out = ds
    elif (desired_lib == 'pandas') and (olib == dd):
        out = ds.compute()
    elif desired_lib == 'pandas':
        out = ds.to_pandas()
    elif olibstring == 'pandas':
        out = get_lib_from_string(desired_lib).from_pandas(ds)
    elif desired_lib == 'cupy':
        out = cp.as_array(ds)
    else:
        raise Exception(f'Conversion from {olibstring} to {desired_lib} is currently not supported.')

    if reset_index:
        return out.reset_index(drop=True)

    return out


def get_lib_as_string(lib):
    return re.search(r"'[A-z.]+'", str(lib)).group(0)[1:-1]


def get_lib_from_string(libstr: str):
    if libstr == 'pandas':
        return pd
    elif libstr == 'cudf':
        return cudf
    elif libstr == 'cupy':
        return cp
    raise Exception(f'{libstr} not yet implemented')


def extract_batch_numbers(file_list: list, independent_sub_batches: bool) -> list:
    """Extract batch numbers from file list."""
    assert isinstance(file_list, list) or isinstance(file_list, str), f'file_list must be either a list or a string, but found a: {type(file_list)}'
    fc_list: list = [get_file_name_components(f) for f in (file_list if isinstance(file_list, list) else [file_list])]
    if independent_sub_batches:
        return list(set([fc.bs_sep.join([str(x) for x in fc.batch_numbers]) for fc in fc_list]))
    else:
        return list(set([str(fc.batch_numbers[0]) for fc in fc_list]))


def ensure_columns(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    """
    Compare cols to dataframe and create missing columns with a Null value.

    Parameters
    ----------
    df : pd.DataFrame
    cols : list
        list of columns that should be in the dataframe.

    Returns
    -------
    df : pd.DataFrame
        Original DataFrame after modification (if neccessary).

    """
    for c in pd.Index(cols).difference(df.columns).tolist():
        df[c] = None
    return df


def extract_num(input_str: str, return_pos: int = 0, abs_value: bool = False) -> str:
    """
    Extract number from specified position.

    Parameters
    ----------
    input_str : str
        string to parse.
    return_pos : int, optional
        Location in found numbers to return. The default is 0.
    abs_value : bool, optional
        Whether to force abolute value or not. The default is False.

    Returns
    -------
    str
        DESCRIPTION.

    """
    # check if blank
    if pd.isnull(input_str):
        return None
    else:
        input_str = str(input_str)

    # force absolute value if there is a letter preceeding a dash
    if bool(re.search(r'[A-z]\-[0-9]', input_str)):
        abs_value: bool = True

    # recognize dates and return None
    elif bool(re.search(r'[0-9]{4}\-[0-9]{2}\-[0-9]{2}|[0-9]{4}/[0-9]{2}/[0-9]{2}|[0-9]{2}\-[0-9]{2}\-[0-9]{4}|[0-9]{2}/[0-9]{2}/[0-9]{4}', input_str)):
        return None

    # extract number and operator
    nums = re.findall(r'(-|neg|neg\s|negative\s|negative|<\s|<|<=\s|<=|>\s|>|>=\s|>=|=\s|=|)([0-9][0-9,.]+[0-9]+|[0-9]+)', input_str, re.IGNORECASE)

    # return None if no numberser identified
    if len(nums) == 0:
        return None

    # average ranges
    elif bool(re.search(r'[0-9]\-[0-9]', input_str)):
        if len(nums) == 2:
            try:
                return (_format_number(nums[0], abs_value=abs_value) + _format_number(nums[1], abs_value=True)) / 2
            except:
                pass
        return None
    elif len(nums) > 3:
        return None
    else:
        return _format_number(nums[return_pos], abs_value=abs_value)


def _format_number(input_tuple: str, abs_value: bool = False):
    input_str: str = input_tuple[1].replace(',', '').replace('...', '.').replace('..', '.')
    multiplier: int = -1 if (('neg' in input_tuple[0].lower()) or (('-' in input_tuple[0]) and (not abs_value))) else 1
    try:
        return float(input_str) * multiplier
    except:
        return None


def get_column_type(series: Union[pd.Series, pd.DataFrame], one_hot_threshold: int, downcast_floats: bool = False, ignore_bools: bool = False):
    """
    Determine data type in pandas dataframe or series.

    Parameters
    ----------
    series : Union[pd.Series, pd.DataFrame]
        Pandas Series or DataFrame to be analyzed.
    one_hot_threshold : int
        number of unique values to seperate between one hot encoded variables and categorically encoded variables.
    downcast_floats : bool, optional
        downcast floats to integers when possible. The default is False.
    ignore_bools: bool, optional
        Ignore boolean inference and return either int or float in place of binary [1,0] or [1.0, 0.0]

    Returns
    -------
    Union[str, pd.Series]
        string describing datatype or pandas series of types with the column names as the index.

    """
    assert isinstance(series, pd.Series) or isinstance(series, pd.DataFrame), f'Input series must be a pandas series or dataframe; however it was found to be of type {type(series)}'

    if isinstance(series, pd.DataFrame):
        return series.apply(get_column_type, one_hot_threshold=one_hot_threshold, downcast_floats=downcast_floats, ignore_bools=ignore_bools, axis=0)

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

    base_type = re.search(r'int|float|object|datetime',
                          inferred_dtype).group(0)

    unique_values: int = pd.to_numeric(series.dropna()).nunique() if base_type in ['float', 'int'] else series.dropna().nunique()

    if base_type in ['float', 'int'] and unique_values == 2:
        return base_type if ignore_bools else 'binary'
    elif base_type == 'object' and unique_values > one_hot_threshold:
        return 'cat_embedding'
    elif base_type == 'object':
        return 'cat_one_hot'
    else:
        return base_type

def notnull(v: any) -> bool:
    return pd.notnull(v).any() if (isinstance(v, Iterable) and (not isinstance(v, (str, dict)))) else pd.notnull(v)


def coalesce(*values):
    """Return the first non-None value or None if all values are None"""
    return next((v for v in values if notnull(v)), None)
