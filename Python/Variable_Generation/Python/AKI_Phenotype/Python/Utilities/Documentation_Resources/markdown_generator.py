# -*- coding: utf-8 -*-
"""
Created on Sun Nov 27 21:13:27 2022

@author: ruppert20

Adapted From: https://github.com/nilansaha/mdgenerator/blob/master/mdgenerator/mdgenerator.py
"""
from typing import Union
import os
import re
import numpy as np
import pandas as pd


def generate_file_structure(target_dir: str,
                            output_dir: str = os.getcwd(),
                            max_levels: int = None,
                            dirs_to_ignore: list = ['.git', '__pycache__'],
                            files_to_ignore: list = ['.gitignore', '.gitattributes'],
                            file_types_to_ignore: list = ['.pyc']):
    code = "```\n"
    current_dir: str = os.getcwd()
    os.chdir(target_dir)
    for root, dirs, files in os.walk("."):
        path = root.split(os.sep)
        if (any([x in dirs_to_ignore for x in path]) and (len(path) > 1)) or (all([x in files_to_ignore for x in files]) and (len(files) > 1)):
            print(f'{path}: {len(path)}: {files}')
            continue
        elif isinstance(max_levels, int):
            if len(path) > max_levels:
                continue
        if not os.path.basename(root) == ".":
            code += ((len(path) - 2) * '|   ') + '├── ' + os.path.basename(root) + '\n'
        for file in files:
            if (file not in files_to_ignore) and (not bool(re.search(r'$|'.join(file_types_to_ignore) + r'$', file, flags=re.IGNORECASE))):
                code += ((len(path) - 1) * '|   ') + '├── ' + file + '\n'
    code += "```"
    os.chdir(current_dir)

    if os.path.isdir(output_dir):
        f = open(os.path.join(output_dir, "file-structure.md"), "w+")
        f.write(code)
        f.close()


def generate_table(data: Union[pd.DataFrame, np.ndarray, list]) -> str:
    if isinstance(data, pd.DataFrame):
        pass
    elif isinstance(data, np.ndarray) and (data.ndim == 2):
        data = pd.DataFrame(data[1:], columns=data[0])
    elif isinstance(data, list) and (np.array(data).ndim == 2):
        data = pd.DataFrame(data[1:], columns=data[0])
    else:
        raise ValueError('Please provide either a pandas dataframe or a 2-D numpy array or a 2-D python list')
    table = '|' + '|'.join(data.columns) + '|'
    table += '\n' + '|---' * len(data.columns) + '|\n'
    table += '\n'.join(data.apply(lambda x: '|' + '|'.join(x) + '|', axis=1))
    return table
