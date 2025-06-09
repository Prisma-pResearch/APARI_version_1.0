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
from typing import Dict, Union


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


def create_log_handler(log_name: str, directory: str = None, levelName: Union[str, None] = 'DEBUG', levelNum: Union[int, None] = None):
    logger = logging.getLogger(name=log_name)
    logger.setLevel(levelName or levelNum or 'DEBUG')

    if isinstance(directory, str):
        fileHandler = logging.FileHandler(os.path.join(directory, '{}.log'.format(log_name)))
        fileHandler.setLevel(logging.ERROR)
        logger.addHandler(fileHandler)
        formatter = logging.Formatter(fmt='%(name)s; %(levelname)s:%(asctime)s: %(message)s',
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
                            log: bool = False,
                            subject: str = 'Data Processing Update',
                            recipients: list = ['ruppert20@ufl.edu'],
                            raise_exception: bool = False,
                            reflect_input: bool = False,
                            warning: bool = False,
                            messageLevelName: Union[str, None] = 'INFO',
                            messageLevelNum: Union[int, None] = None,
                            logLevelName: str = 'DEBUG',
                            logLevelNum: Union[int, None] = None,
                            display_level: Union[str, int] = 'WARNING'):
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

    system_log_levels: Dict[str, int] = {'CRITICAL': 50,
                                         'ERROR': 40,
                                         'WARNING': 30,
                                         'INFO': 20,
                                         'DEBUG': 10,
                                         'NOTSET': 0}

    if log:
        messageLevelName: str = 'INFO'
    elif warning:
        levmessageLevelNameelName: str = 'WARNING'
    elif debug:
        messageLevelName: str = 'DEBUG'
    elif fatal_error:
        messageLevelName: str = 'CRITICAL'
    elif error:
        messageLevelName: str = 'ERROR'

    if messageLevelName not in logging._nameToLevel:
        assert isinstance(messageLevelNum, int), f'A messageLevelNum is required for new Logging Levels'
        assert messageLevelNum not in list(system_log_levels), f'Invalid logging level. It already exists in the system levels: {system_log_levels}'
        addLoggingLevel(levelName=messageLevelName, levelNum=messageLevelNum)

    if isinstance(log_name, str):
        logger = create_log_handler(log_name=log_name, directory=log_dir, levelName=logLevelName, levelNum=logging._nameToLevel.get(logLevelName, logLevelNum))
    else:
        logger = logging

    logger.log(logging._nameToLevel.get(messageLevelName), message)
    if messageLevelName in ['CRITICAL', 'ERROR']:
        logger.log(logging._nameToLevel.get(messageLevelName), str(traceback.format_exc().splitlines()))

    if isinstance(display_level, str):
        display_level: int = logging._nameToLevel.get(display_level)

    if display or (display_level <= logging._nameToLevel.get(messageLevelName)):
        print(formatted_message)

    if email or (messageLevelName == 'CRITICAL'):
        if messageLevelName == 'CRITICAL':
            subject = 'CRITICAL Error'
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


def addLoggingLevel(levelName: str, levelNum: int, methodName: Union[str, None] = None):
    """
    Comprehensively adds a new logging level to the `logging` module and the
    currently configured logging class.

    `levelName` becomes an attribute of the `logging` module with the value
    `levelNum`. `methodName` becomes a convenience method for both `logging`
    itself and the class returned by `logging.getLoggerClass()` (usually just
    `logging.Logger`). If `methodName` is not specified, `levelName.lower()` is
    used.

    To avoid accidental clobberings of existing attributes, this method will
    raise an `AttributeError` if the level name is already an attribute of the
    `logging` module or if the method name is already present 

    Example
    -------
    >>> addLoggingLevel('TRACE', logging.DEBUG - 5)
    >>> logging.getLogger(__name__).setLevel("TRACE")
    >>> logging.getLogger(__name__).trace('that worked')
    >>> logging.trace('so did this')
    >>> logging.TRACE
    5

    """
    if not methodName:
        methodName = levelName.lower()

    if hasattr(logging, levelName):
        raise AttributeError('{} already defined in logging module'.format(levelName))
    if hasattr(logging, methodName):
        raise AttributeError('{} already defined in logging module'.format(methodName))
    if hasattr(logging.getLoggerClass(), methodName):
        raise AttributeError('{} already defined in logger class'.format(methodName))

    # This method was inspired by the answers to Stack Overflow post
    # http://stackoverflow.com/q/2183233/2988730, especially
    # http://stackoverflow.com/a/13638084/2988730
    def logForLevel(self, message, *args, **kwargs):
        if self.isEnabledFor(levelNum):
            self._log(levelNum, message, args, **kwargs)

    def logToRoot(message, *args, **kwargs):
        logging.log(levelNum, message, *args, **kwargs)

    logging.addLevelName(levelNum, levelName)
    setattr(logging, levelName, levelNum)
    setattr(logging.getLoggerClass(), methodName, logForLevel)
    setattr(logging, methodName, logToRoot)
