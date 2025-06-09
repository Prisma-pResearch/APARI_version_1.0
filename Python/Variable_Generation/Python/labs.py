# -*- coding: utf-8 -*-
"""
Created on Fri May 21 10:38:59 2021.

@author: ruppert20
"""
# from Utils.func_utils import debug_inputs
from .Utilities.PreProcessing.clean_labs import clean_labs
from .Utilities.PreProcessing.data_format_and_manipulation import force_numeric
from .Utilities.Logging.log_messages import log_print_email_message as logm
import pandas as pd
import sqlite3 as sq
import re
from .Utilities.FileHandling.io import check_load_df
from .Utilities.Database.connect_to_database import execute_query_in_transaction


def extract_laboratory_variables(encounter_df: pd.DataFrame,
                                 labs_df: pd.DataFrame,
                                 reference_date_col: str,
                                 unique_index_col: str,
                                 pid: str,
                                 **logging_kwargs) -> pd.DataFrame:
    # debug_inputs(function=extract_laboratory_valiables, kwargs=locals(), dump_fp='labs.p')
    # cleanup lab generation template
    logm(message='Lab Variable Generation Started', **logging_kwargs)
    if len(labs_df.columns.intersection(['generate_categorical', 'generate_numeric',
                                         'generate_binary', 'var_abbrev', 'lab_type']).tolist()) != 5:
        labs_df = clean_labs(df=labs_df,
                             id_cols=[pid, unique_index_col], return_labs_only=True, **logging_kwargs)

    # filter labs
    labs_df = labs_df[labs_df.var_abbrev.notnull()]\
        .drop(columns=['operator_source_value', 'unit_source_value', 'normal_low', 'normal_high',
                       'result_datetime', 'order_datetime', 'proc_code', 'operator_concept_id',
                       'source_unit_concept_id', 'standard_unit_concept_id',
                       'lab_id', 'imputed_unit', 'intraop_y_n', 'loinc', 'loinc_desc', 'to_unit', 'coefficient',
                       'range_min', 'range_max', 'cleaned_row', 'converted_value'], errors='ignore')

    # create in memory sqlite database
    conn = sq.connect(':memory:')

    # format reference_date_col as datetime if necessary
    encounter_df = check_load_df(input_v=encounter_df, ds_type='pandas',
                                 parse_dates=[reference_date_col], preserve_case=True, **logging_kwargs)

    # insert dataframes into db
    logm(message='Loading Labs into temporary sqlite database', **logging_kwargs)
    check_load_df(input_v=labs_df, ds_type='pandas', parse_dates=['measurement_datetime']).to_sql('labs', con=conn, index=True)
    labs_df = labs_df[['generate_categorical', 'generate_numeric',
                       'generate_binary', 'var_abbrev', 'lab_type']].drop_duplicates(subset=['var_abbrev'])
    encounter_df[[unique_index_col, pid, reference_date_col]].to_sql('enc_df', con=conn, index=False)

    # filter to relevant dates
    execute_query_in_transaction(engine=conn,
                                 query=f'''DELETE FROM labs
                                        WHERE
                                            [index] NOT IN (SELECT DISTINCT
                                                              l.[index]
                                                           FROM
                                                               enc_df e LEFT JOIN labs l on (l.{pid} = e.{pid}
                                                                                             AND
                                                                                             julianday(l.measurement_datetime) BETWEEN julianday(datetime(e.{reference_date_col}, '-365 days')) AND julianday(e.{reference_date_col}))
                                                           WHERE
                                                           l.[index] IS NOT NULL);''')

    # create indexes to improve performance
    execute_query_in_transaction(engine=conn, query=f'CREATE INDEX idx_pat ON labs ({pid});')
    execute_query_in_transaction(engine=conn, query='CREATE INDEX idx_dt ON labs (measurement_datetime);')
    execute_query_in_transaction(engine=conn, query='CREATE INDEX idx_loinc ON labs (var_abbrev);')
    execute_query_in_transaction(engine=conn, query=f'CREATE INDEX idx_pat_e ON enc_df ({pid});')
    execute_query_in_transaction(engine=conn, query=f'CREATE INDEX idx_dt_e ON enc_df ({reference_date_col});')

    output: pd.DataFrame = encounter_df[[unique_index_col]].copy()

    loinc_col: str = 'var_abbrev'

    for var_type in ['binary', 'categorical', 'numeric']:
        for lab_type in labs_df.loc[labs_df[f'generate_{var_type}'].astype(float) == 1, 'lab_type'].unique():
            for tf in [[0, 7], [8, 365]]:

                logm(message=f'extracting {lab_type} {tf}', **logging_kwargs)
                logm(message=f'before: {output.shape}', log_name=logging_kwargs.get('log_name'))
                output = _apply_extraction(filter_df=_filter_labs(conn=conn,
                                                                  pid=pid,
                                                                  var_type=var_type,
                                                                  reference_date_col=reference_date_col,
                                                                  loinc_col=loinc_col,
                                                                  min_time_delta=f'-{tf[1]} days',
                                                                  max_time_delta=f'-{tf[0]} days',
                                                                  loincs=labs_df.loc[labs_df.lab_type == lab_type, loinc_col].unique().tolist(),
                                                                  unique_index_col=unique_index_col),
                                           extraction_func=_check_presence if var_type == 'binary' else _calculate_lab_stats if var_type == 'numeric' else _extract_categorical_vars,
                                           output=output,
                                           tf=tf,
                                           label=labs_df.loc[labs_df.lab_type == lab_type, 'var_abbrev'].iloc[0],
                                           var_type=var_type,
                                           unique_index_col=unique_index_col)
                logm(message=f'after: {output.shape}', log_name=logging_kwargs.get('log_name'))
    for tf in [[0, 7], [8, 365]]:
        lab_type: str = 'UNCR'.lower()
        logm(message=f'extracting {lab_type} {tf}', **logging_kwargs)
        logm(message=f'before: {output.shape}', log_name=logging_kwargs.get('log_name'))
        if (labs_df.lab_type == lab_type).any():
            output = _apply_extraction(filter_df=_filter_labs(conn=conn,
                                                              pid=pid,
                                                              var_type=var_type,
                                                              reference_date_col=reference_date_col,
                                                              loinc_col=loinc_col,
                                                              min_time_delta=f'-{tf[1]} days',
                                                              max_time_delta=f'-{tf[0]} days',
                                                              loincs=labs_df.loc[labs_df.lab_type == lab_type, loinc_col].unique().tolist(),
                                                              unique_index_col=unique_index_col,
                                                              num_ratio_loincs=labs_df.loc[labs_df.lab_type == 'bun', loinc_col].unique().tolist(),
                                                              denom_ration_loincs=labs_df.loc[labs_df.lab_type == 'creatinine', loinc_col].unique().tolist()),
                                       extraction_func=_calculate_lab_stats,
                                       output=output,
                                       tf=tf,
                                       label=labs_df.loc[labs_df.lab_type == lab_type, 'var_abbrev'].iloc[0],
                                       var_type=var_type,
                                       unique_index_col=unique_index_col)
        logm(message=f'after: {output.shape}', log_name=logging_kwargs.get('log_name'))

    for lab_type in ['rbc_ur', 'uap_cat']:
        tf = [0, 365]
        label: str = 'rbcur' if lab_type == 'rbc_ur' else 'UAP' if lab_type == 'uap_cat' else 'Errror'
        logm(message=f'extracting {lab_type} {tf}', **logging_kwargs)
        logm(message=f'before: {output.shape}', log_name=logging_kwargs.get('log_name'))
        if (labs_df.lab_type == lab_type).any():
            output = _apply_extraction(filter_df=_filter_labs(conn=conn,
                                                              pid=pid,
                                                              var_type=var_type,
                                                              reference_date_col=reference_date_col,
                                                              loinc_col=loinc_col,
                                                              min_time_delta=f'-{tf[1]} days',
                                                              max_time_delta=f'-{tf[0]} days',
                                                              loincs=labs_df.loc[labs_df.lab_type == lab_type, loinc_col].unique().tolist(),
                                                              unique_index_col=unique_index_col),
                                       extraction_func=_extract_categorical_vars,
                                       output=output,
                                       tf=tf,
                                       label=label,  # labs_df.loc[labs_df.lab_type == lab_type, 'var_abbrev'].iloc[0],
                                       var_type=var_type,
                                       unique_index_col=unique_index_col)

        logm(message=f'after: {output.shape}', log_name=logging_kwargs.get('log_name'))

    for col in [x for x in output.columns if bool(re.search(r'^count_|_present_', x, flags=re.IGNORECASE))]:
        output[col].fillna(0, inplace=True)

    return encounter_df.merge(output.fillna('missing'), on=[unique_index_col], how='left')


def _filter_labs(conn,
                 pid: str,
                 reference_date_col: str,
                 loinc_col: str,
                 min_time_delta: str,
                 max_time_delta: str,
                 loincs: list,
                 var_type: str,
                 unique_index_col: str,
                 num_ratio_loincs: list = None,
                 denom_ration_loincs: list = None) -> pd.DataFrame:

    loinc_str: str = "','".join(loincs)

    result_str: str = 'l.value_as_concept as lab_result' if var_type == 'categorical' else 'l.value_as_number as lab_result' if var_type == 'numeric' else 'l.value_source_value as lab_result'

    if isinstance(num_ratio_loincs, list):

        num_ratio_loinc_str: str = "','".join(num_ratio_loincs)

        denom_ration_loinc_str: str = "','".join(denom_ration_loincs)

        # pull matching loincs
        tmp: pd.DataFrame = pd.read_sql(f"""SELECT
                                             e.{unique_index_col},
                                             CASE WHEN l.{loinc_col} IN ('{denom_ration_loinc_str}') THEN 'denom'
                                                 WHEN l.{loinc_col} IN ('{num_ratio_loinc_str}') THEN 'num'
                                                 ELSE 'ratio' END as lab_type,
                                            {result_str},
                                            l.measurement_datetime
                                        FROM
                                            enc_df e
                                                LEFT JOIN labs l on (l.{pid} = e.{pid}
                                                    AND
                                                    julianday(l.measurement_datetime) BETWEEN julianday(datetime(e.{reference_date_col}, '{min_time_delta}')) AND julianday(datetime(e.{reference_date_col}, '{max_time_delta}'))
                                                    AND
                                                    l.{loinc_col} IN ('{denom_ration_loinc_str}', '{num_ratio_loinc_str}', '{loinc_str}'))
                                        WHERE
                                            l.value_as_number IS NOT NULL
                                            AND
                                            l.non_standard_unit = 0
                                            AND
                                            l.out_of_range_flag = 0;""", con=conn)

        # add numerator loincs to temporary table
        tmp[tmp.lab_type == 'num'].to_sql(name='tmp_num', con=conn, index=False, if_exists='replace')

        # add denominator loincs to temporary table
        tmp[tmp.lab_type == 'den'].to_sql(name='tmp_den', con=conn, index=False, if_exists='replace')

        # pull ratios
        tmp_ratio = pd.read_sql(f'''SELECT
                                        n.{unique_index_col},
                                        (CAST(n.lab_result AS REAL) / CAST(d.lab_result AS REAL)) as lab_result
                                    FROM
                                        tmp_num n INNER JOIN tmp_den d on (n.{unique_index_col} = d.{unique_index_col}
                                                                           AND
                                                                           (abs(julianday(n.measurement_datetime) - julianday(d.measurement_datetime)) <= 1))
                                    WHERE
                                        n.lab_result IS NOT NULL
                                        AND
                                        d.lab_result IS NOT NULL;''', con=conn)

        # return the original ratios and created ones
        return pd.concat([tmp[tmp.lab_type == 'ratio'].drop(columns=['lab_type', 'measurement_datetime']),
                         tmp_ratio], axis=0, sort=False)

    return pd.read_sql(f"""SELECT
                               e.{unique_index_col},
                               {result_str}
                          FROM
                              enc_df e LEFT JOIN labs l on (l.{pid} = e.{pid}
                                                            AND
                                                            julianday(l.measurement_datetime) BETWEEN julianday(datetime(e.{reference_date_col}, '{min_time_delta}')) AND julianday(datetime(e.{reference_date_col}, '{max_time_delta}'))
                                                            AND
                                                            l.{loinc_col} IN ('{loinc_str}'))
                         WHERE
                             e.{reference_date_col} IS NOT NULL
                             AND
                             l.value_source_value IS NOT NULL
                             AND
                             l.non_standard_unit = 0
                             AND
                             l.out_of_range_flag = 0;""", con=conn)


def _apply_extraction(filter_df: pd.DataFrame, extraction_func: callable, output: pd.DataFrame, tf: list, label: str, var_type: str, unique_index_col: str) -> pd.DataFrame:

    if filter_df.shape[0] != 0:
        return output.merge(filter_df.groupby(unique_index_col)
                            .apply(extraction_func,
                                   label=label,
                                   time_window=f'{tf[0]}_{tf[1]}')
                            .reset_index(),
                            how='left',
                            on=unique_index_col)
    else:
        tmp = output.iloc[0: 1, output.columns.get_loc(unique_index_col): output.columns.get_loc(unique_index_col) + 1].copy()
        tmp['lab_result'] = None
        tmp = tmp.groupby(unique_index_col)\
            .apply(extraction_func,
                   label=label,
                   time_window=f'{tf[0]}_{tf[1]}')\
            .reset_index()

        for col in [x for x in tmp.columns if x not in output.columns]:
            output[col] = tmp.iloc[0, tmp.columns.get_loc(col)]

        return output


def _calculate_lab_stats(df: pd.DataFrame, label: str, time_window: str) -> pd.Series:

    result = force_numeric(df.lab_result).dropna()

    out: pd.Series = pd.Series({f'{label}_min_{time_window}': round(result.min(), 3) if result.shape[0] > 0 else None,
                                f'{label}_max_{time_window}': round(result.max(), 3) if result.shape[0] > 0 else None,
                               f'{label}_mean_{time_window}': round(result.mean(), 3) if result.shape[0] > 0 else None,
                                f'{label}_var_{time_window}': round(result.var(), 3) if result.shape[0] > 1 else 0 if result.shape[0] == 1 else None,
                                f'count_{label}_{time_window}': result.shape[0]})

    return out


def _check_presence(df: pd.DataFrame, label: str, time_window: str) -> pd.Series:

    return pd.Series({f'{label}_present_{time_window}': 1 if df.lab_result.dropna().shape[0] > 0 else 0})


def _extract_categorical_vars(df: pd.DataFrame, label: str, time_window: str) -> pd.Series:

    out: pd.Series = pd.Series({f'count_{label}n_{time_window}': df.lab_result.dropna().shape[0],
                                f'{label}_{time_window}': None})

    tp: pd.Series = df.lab_result.astype(str)

    if tp.str.contains("Large", regex=False, case=False, na=False).any():
        return out.fillna("Large")
    elif tp.str.contains("Moderate", regex=False, case=False, na=False).any():
        return out.fillna("Moderate")
    elif tp.str.contains("Small", regex=False, case=False, na=False).any():
        return out.fillna("Small")
    elif tp.str.contains("Negative", regex=False, case=False, na=False).any():
        return out.fillna("Negative")

    return out
