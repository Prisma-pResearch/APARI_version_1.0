# -*- coding: utf-8 -*-
"""
Created on Fri Jul  7 13:36:09 2023

@author: ruppert20
"""
import re
from typing import List, Union
import pandas as pd
from ..FileHandling.io import find_files, check_load_df, save_data
from ..PreProcessing.data_format_and_manipulation import get_file_name_components, deduplicate_and_join
import h5py


def retrieve_augment_variable_specification(spec_fp: str,
                                            project_name: str,
                                            generated_data_dir: str,
                                            recursive: bool = True,
                                            regex: bool = True,
                                            patterns: List[str] = [r'_[0-9]+_chunk_[0-9]+\.csv', r'_[0-9]+\.csv', r'\.h5'],
                                            exclusion_patterns: Union[None, List[str]] = None,
                                            save_fp: Union[str, None] = None) -> Union[pd.DataFrame, None]:
    r"""
    Augment Variable Specification with additional file_names and columns automatically.

    Parameters
    ----------
    spec_fp : str
        File path to the Variable Specification Document.
    project_name : str
        Project Name to fill in blanks in the project name category.
    generated_data_dir : str
        Directory where generated data is stored.
    recursive : bool, optional
        Whether the search for files should be recursive or not. The default is True.
    regex : bool, optional
        Whether the patterns are regular expressions or not. The default is True.
    patterns : List[str], optional
        List of patterns to look for. The default is [r'_[0-9]+_chunk_[0-9]+\.csv', r'_[0-9]+\.csv', r'\.h5'].
    exclusion_patterns : Union[None, List[str]], optional
        List of patterns to exclude. The default is None.
    save_fp : Union[str, None], optional
        File path to save the result. The default is None.

    Returns
    -------
    out : pd.DataFrame
        Resultant dataframe.

    """
    orig_generated_data_spec: pd.DataFrame = check_load_df(spec_fp, sheet_name='Generated_Variables (Do Not Mod')

    file_df = pd.DataFrame({'source_file_path': find_files(directory=generated_data_dir, patterns=patterns, regex=regex, agg_results=True,
                                                           recursive=recursive, exclusion_patterns=exclusion_patterns)})

    file_df['file_name'] = file_df.source_file_path.apply(lambda x: get_file_name_components(x).file_name)
    file_df['cohort'] = file_df.source_file_path.apply(lambda x: get_file_name_components(x).batch_numbers[0] if len(get_file_name_components(x).batch_numbers) > 0 else None)

    file_df = file_df.groupby('file_name').agg({'cohort': deduplicate_and_join, 'source_file_path': 'first'}).reset_index(drop=False)

    file_df[['column_name', 'key', 'column_type']] = None

    rows: List[pd.Series] = []

    for i, row in file_df.iterrows():
        print(row.file_name)
        row_c = row.copy(deep=True)
        if re.search(r'\.csv$', row.source_file_path):
            row_c['column_name'] = pd.read_csv(row.source_file_path, nrows=0, low_memory=False).columns.to_list()
            row_c['key'] = 'N/A'
            rows.append(row_c)
        elif re.search(r'\.h5$', row.source_file_path):

            with h5py.File(row.source_file_path, 'r') as f:

                for g in f.keys():
                    row_c: pd.Series = row.copy(deep=True)
                    if 'pandas_version' in f[g].attrs:
                        info: pd.DataFrame = check_load_df(row.source_file_path, dataset=g, nrows=0)
                        row_c['column_name'] = info.columns.to_list()
                        row_c['column_type'] = info.dtypes.to_list()
                        row_c['key'] = g
                        rows.append(row_c)
                    elif 'columns' in f[g].attrs:
                        row_c['column_name'] = list(f[g].attrs['columns'])
                        row_c['column_type'] = list(f[g].attrs['column_dtypes'])
                        row_c['key'] = g
                        rows.append(row_c)
                    elif isinstance(f[g], h5py._hl.dataset.Dataset):
                        pass
                    else:
                        for k in f[g].keys():
                            ds = f[g][k]
                            row_c = row.copy(deep=True)
                            if 'pandas_version' in ds.attrs:
                                info: pd.DataFrame = check_load_df(row.source_file_path, group=g, dataset=k, nrows=0)
                                row_c['column_name'] = info.columns.to_list()
                                row_c['column_type'] = info.dtypes.to_list()
                                row_c['key'] = {g: k}
                                rows.append(row_c)
                            elif 'columns' in ds.attrs:
                                row_c['column_name'] = list(ds.attrs['columns'])
                                row_c['column_type'] = list(f[g].attrs['column_dtypes'])
                                row_c['key'] = {g: k}
                                rows.append(row_c)

    file_df: pd.DataFrame = pd.concat([x.to_frame().T for x in rows], axis=0, ignore_index=True)
    file_df: pd.DataFrame = pd.concat([file_df[file_df.column_type.notnull()].explode(['column_name', 'column_type']),
                                       file_df[file_df.column_type.isnull()].explode('column_name')], axis=0, ignore_index=True)\
        .rename(columns={'file_name': 'source_file'})\
        .merge(orig_generated_data_spec,
               on=['source_file', 'column_name'],
               how='left')
    file_df.project_name.fillna(project_name, inplace=True)

    has_y_type: pd.Series = file_df.column_type_y.notnull()
    if has_y_type.any():
        file_df.loc[has_y_type, 'column_type_x'] = file_df.loc[has_y_type, 'column_type_y'].values

    out: pd.DataFrame = file_df[['project_name', 'column_name', 'column_type_x', 'parent_category', 'child_category',
                                 'grand_child_category', 'source_file', 'cohort', 'key']].rename(columnns={'column_type_x': 'column_type'})

    save_data(df=out, out_path=save_fp)

    return out
