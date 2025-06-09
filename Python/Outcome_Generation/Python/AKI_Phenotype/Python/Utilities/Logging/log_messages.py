# -*- coding: utf-8 -*-
# pylint: disable=line-too-long
"""
Module to write organized logs to file.

Created on Wed Feb 12 09:42:47 2020

@author: ruppert20
"""
import sys
import os
from datetime import datetime as dt
import traceback
from .send_email import send_email
import logging


def start_logging_to_file(directory: str, file_name: str = None):
    """
    Configure log file.

    Actions:
    --------------
    1. creates a filepath using the directory provided, filename (if provided), and current datetime
    2. opens the filepath with sys.stderr
    3. writes process start

    Parameters:
    --------------
    directory: str
        -directory to save the log file in

    file_name: str = None (optional)
        -optional name of the log file

    Returns:
    --------------
        None


    Notes:
    --------------

    """
    if isinstance(file_name, str):
        file_path = os.path.join(directory, file_name + '_{}.log'.format(dt.now().strftime('%Y-%m-%d_%H-%M-%S')))
    else:
        file_path = os.path.join(directory, 'log_{}.log'.format(dt.now().strftime('%Y-%m-%d_%H.%M.%S')))

    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    if ((sys.version_info.minor >= 9) and (sys.version_info.major >= 3)):
        logging.basicConfig(filename=file_path, encoding='utf-8', level=logging.DEBUG,
                            format='%(name)s; %(levelname)s:%(asctime)s: %(message)s',
                            datefmt='%m/%d/%Y %H:%M:%S')
    else:
        logging.basicConfig(filename=file_path, level=logging.DEBUG,
                            format='%(name)s; %(levelname)s:%(asctime)s: %(message)s',
                            datefmt='%m/%d/%Y %H:%M:%S')
    logging.captureWarnings(True)

    return file_path


def create_log_handler(log_name: str, directory: str = None):
    logger = logging.getLogger(name=log_name)
    logger.setLevel(logging.DEBUG)

    if isinstance(directory, str):
        fileHandler = logging.FileHandler(os.path.join(directory, '{}.log'.format(log_name)))
        fileHandler.setLevel(logging.ERROR)
        logger.addHandler(fileHandler)
        formatter = logging.Formatter(format='%(name)s; %(levelname)s:%(asctime)s: %(message)s',
                                      datefmt='%m/%d/%Y %H:%M:%S')
        fileHandler.setFormatter(formatter)

    return logger


def log_print_email_message(message: str,
                            log_name: str = None,
                            log_dir: str = None,
                            display: bool = False,
                            debug: bool = False,
                            email: bool = False,
                            error: bool = False,
                            fatal_error: bool = False,
                            encrypted_dict_file_path: str = None,
                            private_key_dir: str = None,
                            log: bool = True, subject: str = 'Data Processing Update',
                            recipients: list = ['ruppert20@ufl.edu'],
                            inside_parallel_process: bool = False,
                            raise_exception: bool = False,
                            reflect_input: bool = False,
                            warning: bool = False):
    """
    Function to log informaiton, display it, and email it

    Actions:
    --------------
    1. adds a timestamp to message
    2. prints message if display = True
    2. runs a traceback if it was an error
    3. sends an email if send_email is set to true or fatal_errror = True
    4. returns all of the log messages if it is inside a parallel process as tasks in a processpoolexecutor cannot write to sys.stderr


    Parameters:
    --------------
    message: str
        -text to be logged

    display: bool = False (optional)
        -whether or not to print the message to the console

    email: bool = False (optional)
        -whether or not to send the email via idealist system account

    error: bool = False (optional)
        -Whether or not this message is associated with an error. This will trigger a traceback

    fatal_error: bool = False (optional)
        -whether or not the message is associated with a fatal error (one that will cause the program to crash), this will trigger a traceback and an email

    encrypted_dict_file_path: str = '/Idealist_data_pipeline/private_keys_encrypted.json'
        -file path to encrypted dictionary with email credentials

    private_key_dir: str = '/Idealist_data_pipeline'
        -file path to encryption key for dictionary

    log: bool = True
        -whether or not to save the message to the log

    subject: str = 'Data Processing Update'
        -
    recipients: list = ['ruppert20@ufl.edu']
        -email recipients

    inside_parallel_process: bool = False
        -whether or not to write the formatted messages to the sys.stderr or return the message to be written later

    Returns:
    --------------
    formatted string (if inside_parallel_process)


    Notes:
    --------------

    """

    formatted_message = str(dt.now()) + ': {}\n'.format(message)

    if isinstance(log_name, str):
        logger = create_log_handler(log_name=log_name, directory=log_dir)

        if log:
            logger.info(message)
        elif warning:
            logger.warning(message)
        elif debug:
            logger.debug(message)
        elif error or fatal_error:
            logger.error(message)
            logger.error(str(traceback.format_exc().splitlines()))
    else:
        if log:
            logging.info(message)
        elif warning:
            logging.warning(message)
        elif debug:
            logger.debug(message)
        elif error or fatal_error:
            logging.error(message)
            logging.error(str(traceback.format_exc().splitlines()))

    if display:
        print(formatted_message)

    if email or fatal_error:
        if fatal_error:
            subject = 'Fatal Error'
        try:
            send_email(body=formatted_message, subject=subject, recipients=recipients,
                       encrypted_dict_file_path=encrypted_dict_file_path, private_key_dir=private_key_dir)
        except Exception as e:
            print('message send failure, see log for details')
            logging.error(e)

    if raise_exception:
        raise Exception(message)

    if fatal_error:
        raise SystemExit

    if reflect_input:
        return message
