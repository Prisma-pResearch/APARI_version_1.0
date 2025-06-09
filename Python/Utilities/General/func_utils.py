# -*- coding: utf-8 -*-
"""
General Function Utilities.

Created on Tue Feb  1 12:51:50 2022

@author: ruppert20
"""
import importlib
import yaml
import json
import re
import inspect
import pickle


def get_func(input_str: str):
    """
    Retrieve function based on string name.

    Parameters
    ----------
    input_str : str
        name of function (if in global or local namespace) or module.function if not.
        e.g. Utils.io.load_data or load_data

    Returns
    -------
    function

    """
    if '.' in input_str:
        mod_name, func_name = input_str.rsplit('.', 1)

        return getattr(
            locals().get(mod_name)
            or globals().get(mod_name)
            or importlib.import_module(mod_name),
            func_name)
    else:
        return locals().get(input_str) or globals().get(input_str)


def format_kwargs(config_fp: str, config_key: str, default_key: str = 'defaults', allow_missing_keys: bool = False, kwarg_priority: bool = True, **kwargs) -> dict:
    """
    Format Kwargs for function.

    Parameters
    ----------
    config_fp : str
        file_path to a JSON or YAML file.
        **Note**:
            Structure must be like this:
                defaults:
                    key1: key1val
                    key2: key2val
                config_key1:
                    config_key_paramter1: config_key_parameter1_value
    config_key : str
        key to used in config_file.
    default_key : str, optional
        name for the defaults key, (if in config file). The default is 'defaults'. It will create an empyt dict if the key is not found.
    allow_missing_keys : bool, optional
        whether to throw an assertion error if the config_key is missing from the config file. The default is False.
    kwarg_priority : bool, optional
        Whether config_arguments from the config_key or kwargs provided to this function take priority. The default is True in which kwargs will overwrite config_key based arguments.
    **kwargs : any
        kwargs to add to output.

    Returns
    -------
    dict
        DESCRIPTION.

    """
    assert bool(re.search(r'\.json$|\.yaml$|\.yml$', config_fp, re.IGNORECASE)), 'Only YAML and JSON config files are supported at this time'
    config = json.load(open(config_fp, 'r')) if bool(re.search(r'\.json$', config_fp, re.IGNORECASE)) else yaml.safe_load(open(config_fp, 'r'))
    out: dict = config.get(default_key or 'xxxdefaultsxxxxxx', {})

    if not allow_missing_keys:
        assert config_key in config, f'The key: {config_key} was not found in {config_fp}. Options include: {config.keys()}'
    c_values = config.get(config_key, {})
    assert isinstance(c_values, dict), f'The value for the key: {config_key} must be a dict, but found one of type: {type(c_values)}'

    if kwarg_priority:
        out.update(c_values)
        out.update(kwargs)
    else:
        out.update(kwargs)
        out.update(c_values)

    return out


def convert_func_to_string(func: callable) -> str:
    return '.'.join([func.__module__, func.__name__])


def debug_inputs(function: callable, kwargs: dict, dump_fp: str = None, skip_engines: bool = True) -> dict:
    """
    Function debugging Utility.

    Parameters
    ----------
    function : callable
        Function to be debugged.
    kwargs : dict
        Keyword arguments. it is recomended to use "locals()" to grab them all.
    dump_fp : str, optional
        File path where the kwargs will be dumped. The default is None.
    skip_engines : bool, optional
        To skip the pickling of the engines. The default is True.

    Returns
    -------
    dict
        DESCRIPTION.

    """
    if isinstance(dump_fp, str):
        pickle.dump(debug_inputs(function=function, dump_fp=None, kwargs=kwargs), open(dump_fp, 'wb'))
    sig, vard = inspect.signature(function), kwargs  # locals()
    return {param.name: vard[param.name] for param in sig.parameters.values() if not (('engine' in param.name) and skip_engines)}


def module_exists(module_name):
    try:
        __import__(module_name)
    except ImportError:
        return False
    else:
        return True


if __name__ == '__main__':
    pass
