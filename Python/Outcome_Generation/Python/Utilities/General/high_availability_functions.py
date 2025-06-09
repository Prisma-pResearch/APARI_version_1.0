# -*- coding: utf-8 -*-
"""
Created on Wed Mar 24 20:22:15 2021

@author: ruppert20
"""
import pandas as pd
import os
import re


from ..FileHandling.io import check_load_df


def run_function_cache_result(func: callable, input_df_fp, cache_fp: str, long_term_fp: str = None, **kwargs) -> pd.DataFrame:
    """
    Facilitate high availablity and high speed performance of funciton by dynamically loading, evaluating, and caching results.

    Parameters
    ----------
    func : callable
        DESCRIPTION.
    input_df_fp : TYPE
        DESCRIPTION.
    cache_fp : str
        DESCRIPTION.
    long_term_fp : str, optional
        DESCRIPTION. The default is None.
    **kwargs : TYPE
        DESCRIPTION.

    Returns
    -------
    TYPE
        DESCRIPTION.

    """
    # try to load from pickle
    if os.path.exists(cache_fp):
        try:
            return pd.read_pickle(cache_fp)
        except:
            pass

    # try to load from long term storage format
    if isinstance(long_term_fp, str):
        if os.path.exists(long_term_fp):
            try:
                return check_load_df(long_term_fp, id_col=kwargs.get('pid', None), encounter_col=kwargs.get('eid', None))
            except:
                pass

    # evaluate function
    out: pd.DataFrame = func(input_df_fp, **kwargs)

    # save pickle
    out.to_pickle(cache_fp)

    # save to long term storage
    if isinstance(long_term_fp, str):
        if bool(re.search(r'\.csv$', long_term_fp)):
            out.to_csv(long_term_fp, index=False)
        elif bool(re.search(r'\.xlsx$', long_term_fp)):
            out.to_excel(long_term_fp)

    return out