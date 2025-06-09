# -*- coding: utf-8 -*-
"""
Module for connecting to a database and running tasks inside transactions on a database.

Created on Wed Oct 30 17:03:03 2019.

@author: ruppert20
"""
import sqlalchemy
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import URL
import pandas as pd
import os
import re
import numpy as np
from ..Encryption.file_encryption import load_encrypted_file
import math
from collections import namedtuple
from ..Logging.log_messages import log_print_email_message as logm


def get_credentials_v2(encrypted_dict_file_path: str, database: str, private_key_dir: str):
    """
    Function to load a dictionary of private paths and variables

    Actions:
        1. Loads and decrypts an encrypted json file into a dictionary
        2. returns the database credentials necessary to make a connection

    Parameters:
    -------------
    encrypted_dict_file_path: str
        -file path to an AES 256 bit encrypted JSON Dictionary

    database: str
        -Databse short name to be connected to options are 'cassandra', 'IdealistClean', 'IdealistRaw', 'AKIEPICRAW', 'AKIEPICCLEAN'

    private_key_dir: str
        -folder where the key.key file resides necessary to decrypted the JSON file

    Returns:
    ----------
        tuple of credentials
    """
    private_keys: dict = load_encrypted_file(fp=encrypted_dict_file_path, key_fp=private_key_dir, force_dict=True)

    if database.lower() == 'cassandra':
        private_keys = private_keys.get('db', private_keys).get('cassandra', private_keys)
        return private_keys['cassandraHost'], private_keys['cassandraPort'], private_keys['cassandraUserName'], private_keys['cassandraPassword']
    elif database.lower() in ['idealistclean', 'idealistraw', 'akiepicraw', 'akiepicclean', 'mysql07', 'mysql']:
        private_keys = private_keys.get('db', private_keys).get('mysql', private_keys)
        return private_keys.get('mySQL07HostName', private_keys.get('host')),\
            private_keys.get('username', private_keys.get('user')),\
            private_keys.get('mySQL07Password', private_keys.get('pass')),\
            (database if not bool(re.search('mysql|idealist', database, re.IGNORECASE)) else 'prismap_idealist_clean'),\
            'MYSQL',\
            False,\
            private_keys.get('driver', 'mysql+pymysql')
    elif database.lower() in ['idealist_omop', 'msr_data_sql_server', 'aki_epic_ii_omop', 'idealist', 'prismap_multimodal_ai']:
        private_keys = private_keys.get('db', private_keys).get('sql_server_dev', private_keys)
        return private_keys.get('host'),\
            private_keys.get('user'),\
            private_keys.get('pass'),\
            database.upper(),\
            'SQL_SERVER',\
            False,\
            private_keys.get('driver', 'ODBC Driver 17 for SQL Server')
    # elif database.lower() in ['navigate', 'one_florida_all_admissions_final', 'idealist_omop_prod', 'idealist_beta', 'aki_epic_ii', 'one_florida_covid',
    #                           'virus', 'mimic_iv', 'covid_omop', 'nav2', 'omop_idealist', 'omop_idealist_beta']:
    #     return private_keys['mysurgeryrisk-prod'], private_keys['dev_sql_server_user'], private_keys['dev_sql_server_password'], database.upper(), 'SQL_SERVER', False
    elif database.lower() in ['one_florida_all_admissions', 'prismap_idealist']:
        private_keys = private_keys.get('db', private_keys).get('sql_server_prod', private_keys)
        return private_keys.get('host'),\
            private_keys.get('user'),\
            private_keys.get('pass'),\
            database.lower(),\
            'SQL_SERVER',\
            True,\
            private_keys.get('driver', 'ODBC Driver 17 for SQL Server')
    else:
        raise Exception('Invalid Database')


def get_SQL_database_connection_v2(database: str,
                                   username: str = None,
                                   password: str = None,
                                   hostname: str = None,
                                   dialect: str = None,
                                   encrypted_dict_file_path: str = None,
                                   private_key_dir: str = None,
                                   upload: str = False,
                                   return_database: bool = False,
                                   fast_engine: bool = True):
    """
    Generate SQLAlchemy engine connection to SQL Database.

    Parameters
    ----------
    database : str
        datbase to connect to.
    username : str, optional
        username for the database. The default is None. This is only required if an encrypted_dict_file_path is not provided.
    password : str, optional
        password for the database. The default is None. This is only required if an encrypted_dict_file_path is not provided.
    hostname : str, optional
        hostname for the database. The default is None. This is only required if an encrypted_dict_file_path is not provided.
    dialect : str, optional
        dialect for the database. Supported Dialects include ['SQL_SERVER', 'MYSQL']. The default is None. This is only required if an encrypted_dict_file_path is not provided.
    encrypted_dict_file_path : str, optional
        file path to an encrypted dictionary. The default is None.
    private_key_dir : str, optional
        directory for the key.key decrpytion file. The default is None.
    upload : str, optional
        Whether local-infile support should be added for mysql connections. The default is False.
    return_database : bool, optional
        whether the name of the database should be returned. The default is False.
    fast_engine : bool, optional
        Whether fast/stream execution should be enabled. The default is True.

    Raises
    ------
    Exception
        DESCRIPTION.

    Returns
    -------
    TYPE
        DESCRIPTION.

    """

    if isinstance(encrypted_dict_file_path or os.environ.get('CONFIG_PATH'), str) and isinstance(private_key_dir or os.environ.get('CONFIG_KEY'), str):
        hostname, username, password, database, dialect, windows_authentication, driver = get_credentials_v2(encrypted_dict_file_path=encrypted_dict_file_path,
                                                                                                             database=database,
                                                                                                             private_key_dir=private_key_dir)

    elif not (isinstance(username, str) and isinstance(password, str) and isinstance(hostname, str) and isinstance(database, str)):
        raise Exception('''Credentials incomplete, please provide one of the following combinations of information:
                            A. an encrypted_dict_file_path and private_key_dir
                                       OR
                            B. username, password, hostname, database)''')

    if dialect == 'SQL_SERVER':
        if windows_authentication:
            connection_str: str = 'DRIVER={' + driver + '};SERVER=' + hostname + ';DATABASE=' + database + ';Trusted_Connection=yes'
        else:
            connection_str: str = 'DRIVER={' + driver + '};SERVER=' + hostname + ';DATABASE=' + database + ';UID=' + username + ';PWD={' + password + '}'

        connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_str})

        if fast_engine:
            engine = sqlalchemy.create_engine(connection_url, fast_executemany=True, execution_options={"stream_results": True})
        else:
            engine = sqlalchemy.create_engine(connection_url, fast_executemany=False, execution_options={"stream_results": True})

    elif dialect == 'MYSQL':
        engine_str: str = f'{driver}://' + username + ':' + password + '@' + hostname + '/' + database

        if upload:
            engine_str += '?local_infile=1'

        engine = sqlalchemy.create_engine(engine_str, pool_size=10, max_overflow=10, server_side_cursors=True, execution_options={"stream_results": True})

    else:
        raise Exception(f'Invalid Dialect: {dialect}')

    if return_database:
        return engine, database

    return engine


def get_cassandra_database_connection(encrypted_dict_file_path: str, private_key_dir: str):
    '''
    Create a connection with cassandra database based on provided encrypted credentials.

    Parameters:
    -------------
    encrypted_dict_file_path: str
        -file path to JSON encrypted dictionary with database credentials

    private_key_dir: str
        -file path to encryption key

    Returns:
    ------------
    Cassandra Cluster

    '''
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider
    host, port, username, password = get_credentials_v2(encrypted_dict_file_path=encrypted_dict_file_path,
                                                        database='cassandra',
                                                        private_key_dir=private_key_dir)
    ap = PlainTextAuthProvider(username=username, password=password)
    return Cluster([host], port=port, auth_provider=ap, protocol_version=5)


def fetch_data_from_cassandra(query: str, encrypted_dict_file_path: str, private_key_dir: str) -> pd.DataFrame:
    '''
    Query cassandra database and return the patient id and encounter id columns in standard format.

    Actions:
        1. Connect to database using encrypted credentials
        2. Run query which returns a pandas dataframe
        3. format dataframe based on naming conventions
        4. return result as pandas dataframe

    Parameters:
    -------------
    query: str
        -CQL query

    encrypted_dict_file_path: str
        -file path to JSON encrypted dictionary with database credentials


    private_key_dir: str
        -file path to encryption key

    Returns:
        pandas DataFrame

    '''
    cluster = get_cassandra_database_connection(encrypted_dict_file_path=encrypted_dict_file_path, private_key_dir=private_key_dir)
    session = cluster.connect('prisma1')
    session.row_factory = _pandas_factory
    session.default_fetch_size = None
    session.default_timeout = 60
    state1 = session.prepare(query)
    results = session.execute(state1)
    out = results._current_rows
    cluster.shutdown()

    # format patient and encounter ids
    if 'encounter_id' in out.columns:
        out.rename(columns={'encounter_id': 'encounter_deiden_id'}, inplace=True)

    if 'patient_id' in out.columns:
        out.rename(columns={'patient_id': 'patient_deiden_id'}, inplace=True)
    elif 'patientid' in out.columns:
        out.rename(columns={'patientid': 'patient_deiden_id'}, inplace=True)

    # format docotr ids
    if 'docid' in out.columns:
        out.rename(columns={'docid': 'doc_id'}, inplace=True)

    if 'doc_id' in out.columns:
        out.doc_id = pd.to_numeric(out.doc_id, errors='coerce')

    # replace 'null' with None
    try:
        out.replace({'null': None, 'none': None, 'None': None, '': None, 'nan': None}, inplace=True, regex=False)
    except TypeError:
        pass

    return out


def insert_dataframe_into_cassandra(df: pd.DataFrame, table: str,
                                    encrypted_dict_file_path: str, private_key_dir: str,
                                    columns: list = None, batch_size: int = 100,
                                    keep_original_data_types: bool = False):
    '''
    Function to insert contents of a pandas dataframe into cassandra

    Parameters:
    ------------
    df: pd.DataFrame
        -DataFrame to be inserted

    table: str
        -Table in cassandra to be inserted into

    encrypted_dict_file_path: str
        -File path to json file which contains database host name and port

    columns: list = None
        -Subset of columns to upload

    batch_size: int = 100
        -Number of rows to insert at a time

    keep_original_data_types: bool = False
        -Whether or not all of the values should be coverted to strings before insertion or left as is

    Actions:
        1. Split the dataframe into pieces based on the batch size
        2. Upload each row to cassandra treating all values as strings

    Reuturns:
        None

    Notes:
        ****All values in dataframe are converted to strings by default***

    '''
    from cassandra.query import BatchStatement
    from cassandra import ConsistencyLevel
    if df.shape[0] == 0:
        return
    # Connect with Cassandra Database
    cluster = get_cassandra_database_connection(encrypted_dict_file_path=encrypted_dict_file_path,
                                                private_key_dir=private_key_dir)

    # establish session
    session = cluster.connect('prisma1')

    if isinstance(columns, list):
        columns_to_insert = columns
    else:
        columns_to_insert = list(df.columns)

    # split into batches based on the batch size
    split_df = np.array_split(df, math.ceil(df.shape[0] / batch_size))

    # prepare Insertion Query
    insertData = session.prepare('INSERT INTO {} ({}) VALUES ({});'.format(table, ', '.join([str(x) for x in columns_to_insert]), ', '.join([str(x) for x in ['?'] * len(columns_to_insert)])))

    for i, temp_df in enumerate(split_df):
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        for row in range(temp_df.shape[0]):
            if keep_original_data_types:
                values = tuple(temp_df.iloc[row, :].tolist())
            else:
                values = tuple(temp_df.iloc[row, :].apply(str).tolist())
            try:
                batch.add(insertData, values)
            except Exception as e:
                print('Cassndra raise an Error : {}'.format(e))
        session.execute(batch)
        print('batch {} of {} complete'.format((i + 1), len(split_df)))

    print('Uploaded {} rows to {}'.format(df.shape[0], table))

    cluster.shutdown()


def _pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)


def read_sql_file(s_dir_sql: str, s_file_sql: str = None) -> str:
    '''
    Reads SQL file into a string

    Parameters:
        s_dir_sql: str
            folder to load sql string from

        s_file_sql: str
            sql file name to read

    Returns:
        str

    '''
    file_path: str = os.path.join(s_dir_sql, s_file_sql) if isinstance(s_file_sql, str) else s_dir_sql
    with open(file_path, newline=None) as f:
        sql = f.read()
    f.close()

    return sql


def get_encounter_info_from_encounter_id_list(encounter_ids: list, engine: Engine):
    """
    Function to get encounter info for list of encounters from idealist database
    """

    query = read_sql_file('SQL Queries', 'get_encounter_info_using_encounter_id.sql')

    encounter_ids = ','.join([str(int(x)) for x in encounter_ids])
    query = re.sub('XXXXX', encounter_ids, query)
    df_result = pd.read_sql(query, engine)

    temp = df_result.drop_duplicates()

    return temp[pd.notna(temp['admit_datetime']) & pd.notna(temp['dischg_datetime'])]


def execute_query_in_transaction(engine: Engine, query: str, raise_exceptions: bool = False, **logging_kwargs):
    """
    Execute Query in Transaction.

    Parameters
    ----------
    engine : Engine
        sqlalchemy conneciton engine.
    query : str
        query to execute.
    raise_exceptions : bool, optional
        whether to raise an exception if there is a failure or just log it. The default is False.
    **logging_kwargs : TYPE
        keywords to pass to the logm function.

    Raises
    ------
    Exception
        DESCRIPTION.

    Returns
    -------
    str
        None if no exceptions raised otherwise string.

    """
    exception_raised: str = ''

    if str(type(engine)) == "<class 'sqlite3.Connection'>":
        c = engine.cursor()
        c.execute("begin")
        try:
            c.execute(query)
            c.execute('commit')
            logm('executed successfully', **logging_kwargs)
        except engine.Error as e:
            logm("failed!", **logging_kwargs)
            exception_raised = str(e)
            c.execute("rollback")
        finally:
            c.close()
    else:
        if engine.name == 'mysql':
            query = query.replace('[', '').replace(']', '')

        Session = sessionmaker(bind=engine)

        session = Session()
        try:
            session.execute(query)
            session.commit()
        except Exception as e:
            exception_raised = str(e)
            logm(message=query, **logging_kwargs)
            logm(message=e, **logging_kwargs)
            session.rollback()
        finally:
            session.close()

    if raise_exceptions and (exception_raised != ''):
        raise Exception(f'Transaction Failure: {exception_raised}')

    return None if (exception_raised == '') else exception_raised


omop_engine_bundle = namedtuple('omop_engine_bundle', 'engine database vocab_schema data_schema lookup_schema results_schema operational_schema database_update_table lookup_table drug_lookup_table')

if __name__ == '__main__':

    pass
