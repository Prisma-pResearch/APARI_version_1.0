# -*- coding: utf-8 -*-
"""
Module for reconciling two dictionaries.

Created on Thu Dec  2 11:47:59 2021

@author: ruppert20
"""


def update_dict(source_dict: dict, update: dict, modify_existing_keys: bool = False) -> dict:

    if modify_existing_keys:
        return {**source_dict, **update}
    else:
        return {**update, **source_dict}


def dict_union(dict1: dict, dict2: dict) -> dict:
    """
    Create Union of Two dictionaries that may or may not share any keys or nested keys.

    Parameters
    ----------
    dict1 : dict
        Dictionary.
    dict2 : dict
        Dictionary.

    Note
    ----
    The values of the dictionaries must be of type list, dict, str, float, or int

    Returns
    -------
    dict
        Union of two dictionaries.

    """
    out: dict = dict1

    for k, v in dict2.items():
        if k not in out:
            out[k] = v
        else:
            if isinstance(out[k], (float, int, str)) or (out[k] is None):
                if isinstance(v, (float, int, str)):
                    if (out[k] != v):
                        if (out[k] is not None):
                            out[k] = list(set([out[k], v]))
                        else:
                            out[k] = v
                elif isinstance(v, list):
                    if (out[k] not in v) and (out[k] is not None):
                        out[k] = [out[k]] + v
                    else:
                        out[k] = v
                elif isinstance(v, dict):
                    if (out[k] not in v) and (out[k] is not None):
                        out[k] = {**{out[k]: None}, **v}
                    else:
                        out[k] = v
                elif v is None:
                    pass
                else:
                    print(f'Unsupported value_type: {type(v)}, skipping value for key: {k}')
            elif isinstance(out[k], list):
                if isinstance(v, (float, int, str)):
                    if v not in out[k]:
                        out[k] = out[k] + [v]
                elif isinstance(v, list):
                    out[k] = list(set(out[k] + v))
                elif isinstance(v, dict):
                    out[k] = {**{x: None for x in out[k]}, **v}
                elif v is None:
                    pass
                else:
                    print(f'Unsupported value_type: {type(v)}, skipping value for key: {k}')
            elif isinstance(out[k], dict):
                if isinstance(v, (float, int, str)):
                    if v not in out[k]:
                        out[k] = {**out[k], **{v: None}}
                elif isinstance(v, list):
                    out[k] = {**{x: None for x in v}, **out[k]}
                elif isinstance(v, dict):
                    out[k] = dict_union(dict1=out[k], dict2=v)
                elif v is None:
                    pass
                else:
                    print(f'Unsupported value_type: {type(v)}, skipping value for key: {k}')
            else:
                print(f'Unsupported value_type: {type(out[k])}, skipping value for key: {k}')

    return out
