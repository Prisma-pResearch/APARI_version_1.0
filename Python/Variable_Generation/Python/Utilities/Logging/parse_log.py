# -*- coding: utf-8 -*-
"""
Module for parsing Logs into a pandas dataframe.

Created on Tue Dec 21 16:06:10 2021

@author: ruppert20
"""
import pandas as pd
from ..FileHandling.io import load_data


def parse_log(fp: str):

    log = pd.DataFrame({'raw_line': load_data(fp)})

    log.raw_line = log.raw_line.str.strip().replace({'': None})

    log.dropna(inplace=True)

    log[['source', 'type', 'timestamp', 'message']] = log.raw_line.str.extract(r'([A-z_0-9]+);\s([A-Z]+):([0-9]{2}/[0-9]{2}/[0-9]{4}\s[0-9]{2}:[0-9]{2}:[0-9]{2}):\s(.*)')

    log.loc[:, 'timestamp'] = pd.to_datetime(log['timestamp'].values)

    return log


if __name__ == '__main__':
    pass
