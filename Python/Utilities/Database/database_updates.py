# -*- coding: utf-8 -*-
"""
Module to log modifications to a database.

Created on Mon Oct  4 09:57:43 2021

@author: ruppert20
"""
import pandas as pd
from datetime import datetime as dt
from sqlalchemy.engine.base import Engine
from typing import Union
from .connect_to_database import execute_query_in_transaction
from ..Logging.log_messages import log_print_email_message as logm
from .connect_to_database import omop_engine_bundle
import math


def log_database_update(file_name: str, batch: str, dest_table: str, note: str, engine: Union[Engine, omop_engine_bundle],
                        schema: str = None, table_name: str = None, execute_query: bool = False, min_id: str = None, max_id: str = None,
                        process_start: str = None, process_end: str = None, upload_start: str = None, status_message: str = None,
                        upload_end: str = None, github_release: str = None, crud_query: str = None, stop_if_exists: bool = False,
                        exception_to_raise: str = 'Error Update already logged', new_note: str = None,
                        raise_exception_with_status_message: bool = False, **logging_kwargs):
    """
    Log Database Updates to increase DBMS transparency.

    Parameters
    ----------
    file_name : str
        DESCRIPTION.
    batch : str
        DESCRIPTION.
    dest_table : str
        DESCRIPTION.
    note : str
        DESCRIPTION.
    engine : Engine
        DESCRIPTION.
    schema : str
        DESCRIPTION.
    table_name : str
        DESCRIPTION.
    execute_query : bool, optional
        DESCRIPTION. The default is False.
    min_id : str, optional
        DESCRIPTION. The default is None.
    max_id : str, optional
        DESCRIPTION. The default is None.
    process_start : str, optional
        DESCRIPTION. The default is None.
    process_end : str, optional
        DESCRIPTION. The default is None.
    upload_start : str, optional
        DESCRIPTION. The default is None.
    status_message : str, optional
        DESCRIPTION. The default is None.
    upload_end : str, optional
        DESCRIPTION. The default is None.
    github_release : str, optional
        DESCRIPTION. The default is None.
    crud_query : str, optional
        DESCRIPTION. The default is None.
    stop_if_exists: bool, optional
        Function will raise exception if the primary key value set already exists in the database. Default is False.
    exception_to_raise: str, optional
        Error message to display if the valueset already exists in the update table. The default is 'Error Update already logged'.

    Returns
    -------
    None.

    """
    if isinstance(engine, omop_engine_bundle):
        schema: str = engine.operational_schema
        table_name: str = engine.database_update_table
        engine: Engine = engine.engine

    assert isinstance(table_name, str)
    assert isinstance(schema, str)

    if pd.read_sql(f'''SELECT
                        file_name
                    FROM
                        {schema}.{table_name}
                    WHERE
                        file_name = '{file_name}'
                        AND
                        batch = '{batch}'
                        AND
                        dest_table = '{dest_table}'
                        AND
                        note = '{note}';''', con=engine).shape[0] == 0:

        pd.DataFrame({'file_name': [file_name],
                      'batch': [batch],
                      'dest_table': [dest_table],
                      'note': [note],
                      'process_start': [process_start if isinstance(process_start, str)
                                        else (process_start if isinstance(process_start, dt) else dt.now()).strftime("%Y-%m-%d %H:%M:%S")],
                      'process_end': [process_end if (isinstance(process_end, str) or pd.isnull(process_end))
                                      else process_end.strftime("%Y-%m-%d %H:%M:%S") if isinstance(process_end, dt) else None],
                      'upload_start': [upload_start if (isinstance(upload_start, str) or pd.isnull(upload_start))
                                       else upload_start.strftime("%Y-%m-%d %H:%M:%S") if isinstance(upload_start, dt) else None],
                      'upload_end': [upload_end if (isinstance(upload_end, str) or pd.isnull(upload_end))
                                     else upload_end.strftime("%Y-%m-%d %H:%M:%S") if isinstance(upload_end, dt) else None],
                      'github_release': [github_release],
                      'crud_query': [crud_query if len(str(crud_query)) < 3000 / 4 else crud_query[:math.floor(3000 / 4)]],
                      'status_message': [status_message if isinstance(status_message, str) or (not execute_query) else 'RUNNING'],
                      'min_id': [min_id],
                      'max_id': [max_id]})\
            .to_sql(name=table_name,
                    con=engine,
                    schema=schema,
                    if_exists='append',
                    index=False)

        if execute_query and isinstance(crud_query, str):
            update_type: str = 'exec'
        else:
            update_type: str = None
    elif stop_if_exists:
        raise Exception(exception_to_raise)
    else:
        update_type: str = 'update'

    if update_type == 'update':

        update_vals: dict = {'process_end': process_end,
                             'upload_start': upload_start,
                             'min_id': min_id,
                             'max_id': max_id,
                             'upload_end': upload_end,
                             'status_message': status_message,
                             'note': new_note}

        update_q: str = ', '.join([f"{k} = '{v}'" for k, v in update_vals.items() if pd.notnull(v)])

    elif update_type == 'exec':

        err_m = execute_query_in_transaction(engine=engine, query=crud_query, raise_exceptions=False, **logging_kwargs)

        update_q: str = f'''status_message = '{err_m.replace("'", '"') if isinstance(err_m, str) else "Success"}',
                            process_end = '{dt.now().strftime("%Y-%m-%d %H:%M:%S")}'
                            {", note = CONCAT('Failed: ', note)" if isinstance(err_m, str) else ""}'''
    else:
        return

    if len(update_q) == 0:
        logm('No Items to update', raise_exception=True)

    error_message: str = execute_query_in_transaction(engine=engine,
                                                      query=f'''UPDATE
                                                                       {schema}.{table_name}
                                                                   SET
                                                                       {update_q}
                                                                   WHERE
                                                                       file_name = '{file_name}'
                                                                       AND
                                                                       batch = '{batch}'
                                                                       AND
                                                                       dest_table = '{dest_table}'
                                                                       AND
                                                                       note = '{note}';''',
                                                      **logging_kwargs)

    if raise_exception_with_status_message or (error_message is not None):
        raise Exception(error_message if (error_message is not None) else status_message)


def get_identity_column_for_table(engine: Engine, table: str, schema: str = None) -> str:
    """
    Retrieve Identify column from specified table.

    Parameters
    ----------
    engine : Engine
        DESCRIPTION.
    schema : str
        DESCRIPTION.
    table : str
        DESCRIPTION.

    Returns
    -------
    str
        DESCRIPTION.

    """
    if pd.isnull(schema) and '.' in table:
        schema: str = '.'.join(table.split('.')[:-1])
        table: str = table.split('.')[-1]

    assert isinstance(schema, str)

    if engine.name == 'mssql':
        if '.' in schema:
            database: str = schema.split('.')[0] + '.'
            schema: str = schema.split('.')[1]
        else:
            database: str = ''
        table_info = pd.read_sql(f'''SELECT
                                        inf.COLUMN_NAME
                                    FROM
                                        {database}sys.tables
                                        INNER JOIN {database}sys.columns ON tables.object_id = columns.object_id
                                        INNER JOIN {database}sys.types ON types.user_type_id = columns.user_type_id
                                        INNER JOIN {database}sys.schemas ON schemas.schema_id = tables.schema_id
                                        INNER JOIN {database}INFORMATION_SCHEMA.COLUMNS inf ON (inf.TABLE_SCHEMA=schemas.name
                                                                                      AND inf.TABLE_NAME=tables.name AND inf.COLUMN_NAME=columns.name)
                                    WHERE
                                       inf.table_name = '{table}'
                                       AND
                                       inf.TABLE_SCHEMA = '{schema}'
                                       AND
                                       columns.is_identity = 1;''', engine)
    elif engine.name == 'mysql':
        table_info = pd.read_sql(f'''SELECT
                                         COLUMN_NAME
                                    FROM
                                        INFORMATION_SCHEMA.COLUMNS
                                    WHERE
                                        TABLE_NAME = '{table}'
                                        AND
                                        TABLE_SCHEMA = '{schema}'
                                        AND
                                        EXTRA LIKE '%%auto_increment%%';''', engine)
    elif engine.name == 'postgresql':
        table_info = pd.read_sql(f'''SELECT
                                         column_name as "COLUMN_NAME"
                                     from
                                         information_schema.columns
                                     where
                                         table_name = '{table}'
                                         and
                                         table_schema = '{schema}'
                                         and
                                         is_identity = 'YES';''', engine)

    if table_info.shape[0] == 0:
        return None

    return table_info.COLUMN_NAME.iloc[0]


def get_min_max_id_from_table(engine: Engine, table: str, schema: str = None, id_type: str = 'min', add_one: bool = False) -> int:
    """
    Get either the min or max value for an identity column from the specified table.

    Parameters
    ----------
    engine : Engine
        DESCRIPTION.
    table : str
        DESCRIPTION.
    schema : str
        DESCRIPTION.
    id_type : str, optional
        accepts "min", "max". The default is 'min'.

    Returns
    -------
    int
        DESCRIPTION.

    """
    if pd.isnull(schema) and '.' in table:
        schema: str = '.'.join(table.split('.')[:-1])
        table: str = table.split('.')[-1]

    assert isinstance(schema, str)

    id_col: str = get_identity_column_for_table(engine=engine, schema=schema, table=table)

    if isinstance(id_col, str):
        if id_type.lower() in ['min', 'max']:
            if add_one:
                return pd.read_sql(f'SELECT COALESCE({id_type.upper()}({id_col}), 0) as {id_type.lower()}_id FROM {schema}.{table}', engine).iloc[0, 0] + 1
            return pd.read_sql(f'SELECT COALESCE({id_type.upper()}({id_col}), 0) as {id_type.lower()}_id FROM {schema}.{table}', engine).iloc[0, 0]
        raise Exception(f'Error: {id_type} is not supported. Only min and max are supported.')
    return None
