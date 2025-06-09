# -*- coding: utf-8 -*-
"""
Module for Writiting and reading h5 files.

Created on Tue Dec 28 09:18:23 2021.

@author: ruppert20

Updated .loc for pandas 2.X compatability
"""
import h5py
from typing import Union
import pandas as pd
import re
from ..Logging.log_messages import log_print_email_message as logm
import numpy as np
from tqdm import tqdm
from ..PreProcessing.data_format_and_manipulation import get_column_type


def write_h5(fp: str, group: str = None, dataset: str = None, dataframe: pd.DataFrame = None,
             group_dataset_df_dict_list: list = None, replace_groups: bool = False,
             group_attrs_dict: dict = {}, use_pandas: bool = True,
             replace_datasets: bool = True, index: bool = True,
             **logging_kwargs):
    """
    Write to .h5 file.

    Parameters
    ----------
    fp : str
        Location of .h5 file on the file system.
    group : str, optional
        Group the df is to be written to. The default is None.
        *NOTE*: group is overwritten when a group_dataset_df_dict_list is provided.
    dataset : str, optional
        Name of the dataset to be written. The default is None.
        *NOTE*: dataset is overwritten when a group_dataset_df_dict_list is provided.
    dataframe : pd.DataFrame, optional
        Dataframe to save. The default is None.
        *NOTE*: The dataframe is not used when a group_dataset_df_dict_list is provided.
    group_dataset_df_dict_list : list, optional
        list of dictionaries to be saved to the .h5 file.
        The format should be as follows [{'group': group, 'dataset': dataset, 'df': dataframe, 'attrs': dict},
                                         {'group': group2, 'dataset': dataset2, 'df': dataframe2, 'attrs': dict},
                                         ...]
    replace_groups : bool, optional
        Whether to replace existing groups by the given name or not. The default is False.
    group_attrs_dict : dict, optional
        dictionary of attributes to be added to newly created groups.
        The format must be {groupname1(str): {attr_name(str): attr_values(list)}} e.g. {'dogs': {'breeds': ['wolf', 'german shepard']}}
        The default is {}.
    replace_datasets : bool, optional
        Whether to replace existing datasets by the given name or throw an exception. The default is True.
    index : bool, optional
        Whether to preserve the dataframe index. The default is False.
    **logging_kwargs
        kwargs to be passed to the log_print_email_message from Utils.log_messages

    Raises
    ------
    Exception
        Will throw an exception if a dataset already exists and the overwrite flag is set to False.

    Returns
    -------
    None.

    """
    assert (isinstance(group_dataset_df_dict_list, list) or (isinstance('dataset', str) and isinstance(dataframe, pd.DataFrame))
            ), 'A dataframe and dataset must be defined using either the keywors or a dictionary'

    group_dataset_df_dict_list = group_dataset_df_dict_list if isinstance(group_dataset_df_dict_list, list) else [{'group': group, 'dataset': dataset, 'df': dataframe}]

    groups_created: list = []

    for ddict in tqdm(group_dataset_df_dict_list):

        g = ddict.get('group')
        d = ddict.get('dataset')
        df = ddict.get('df')
        attrs = ddict.get('attrs') or {}
        attrs.pop('columns', None)
        attrs.pop('index', None)
        attrs.pop('column_dtypes', None)
        attrs.pop('index_dtypes', None)
        attrs.pop('index_names', None)

        logm(f'Writing: {d}{" to group: " + g if isinstance(g, str) else ""}', **logging_kwargs)

        with h5py.File(fp, 'a', libver='latest') as f:

            if isinstance(g, str):
                grp, tg = _check_make_group(f, group=g, replace_groups=replace_groups,
                                            group_attrs=group_attrs_dict.get(g, {}),
                                            groups_created=groups_created)
                groups_created += tg
            else:
                grp = f

            assert isinstance(d, str), 'The dataset name must be provided as a string value'
            if d in grp:
                if replace_datasets:
                    del grp[d]
                else:
                    raise Exception(f'{d} already exists in {grp.name}')

        if use_pandas:
            df.to_hdf(fp, mode='a', key=f'{g}/{d}' if g is not None else d)
        else:

            if isinstance(df, pd.DataFrame):
                df_dtype: str = get_column_type(series=df, one_hot_threshold=10, downcast_floats=True, ignore_bools=True)
                downcast_type: str = 'integer' if df_dtype.astype(str).str.contains(r'^int', case=False).all() else 'float' if df_dtype.astype(str).str.contains(r'^float|^int', case=False).all() else None
                # downcast_type = 'integer' if df.dtypes.astype(str).str.contains(r'^int', case=False).all() else 'float' if df.dtypes.astype(str).str.contains(r'^float|^int', case=False).all() else None
                attrs['columns'] = df.columns.astype(str).tolist()
                type_dict: dict = (df_dtype if downcast_type else df.dtypes).astype(str).to_dict()
                attrs['column_dtypes'] = [type_dict.get(c) for c in df.columns.tolist()]
                index_df: pd.DataFrame = df[[]].reset_index(drop=False)
                attrs['index'] = index_df.values if index_df.dtypes.astype(str).str.contains(r'^float|^int', case=False).all() else index_df.astype(str).values
                index_types: dict = index_df.dtypes.astype(str).to_dict()
                attrs['index_names'] = index_df.columns.tolist()
                attrs['index_dtypes'] = [index_types.get(c) for c in index_df.columns.tolist()]
                del index_df, type_dict, index_types
                if downcast_type is not None:
                    df = df.apply(pd.to_numeric, downcast=downcast_type)
                    type_dict: dict = df.dtypes.astype(str).to_dict()
                    attrs['column_dtypes'] = [type_dict.get(c) for c in df.columns.tolist()]
                    df = df.values
                else:
                    df = df.fillna('-9999999').astype(str).values

            elif isinstance(df, pd.Series):
                downcast_type = 'integer' if bool(re.search(r'^int', str(df.dtype))) else 'float' if bool(re.search(r'^float', str(df.dtype))) else None
                attrs['columns'] = [str(df.name)]
                attrs['column_dtypes'] = [str(df.dtype)]
                index_df: pd.DataFrame = df.reset_index(drop=False).drop(columns=[df.name])
                attrs['index'] = index_df.values if index_df.dtypes.astype(str).str.contains(r'^float|^int', case=False).all() else index_df.astype(str).values
                index_types: dict = index_df.dtypes.astype(str).to_dict()
                attrs['index_names'] = index_df.columns.tolist()
                attrs['index_dtypes'] = [index_types.get(c) for c in index_df.columns.tolist()]
                del index_df, index_types
                if downcast_type is not None:
                    df = pd.to_numeric(df, downcast=downcast_type)
                    type_dict: dict = df.dtypes.astype(str).to_dict() if isinstance(df, pd.DataFrame) else {df.name: str(df.dtype)}
                    attrs['column_dtypes'] = [str(df.dtype)]
                    del type_dict
                    df = df.values
                else:
                    df = df.fillna('-9999999').astype(str).values

            with h5py.File(fp, 'a', libver='latest') as f:
                ds = (f[g] if isinstance(g, str) else f).create_dataset(name=d, data=df)

                if len(attrs) > 0:
                    for an, av in attrs.items():
                        if av is not None:
                            ds.attrs[an] = av


def read_h5_dataset(fp: str, dataset: str, group: str = None, use_pandas: bool = False, convert_dtypes: bool = True, start: Union[int, None] = None, stop: Union[int, None] = None, columns: Union[list, None] = None, nrows: Union[int, None] = None) -> pd.DataFrame:
    """Return specified dataset from .h5 file as a pandas dataframe."""
    if isinstance(nrows, int):
        stop: int = nrows
        start: int = 0

    if use_pandas:
        return pd.read_hdf(fp, mode='r', key=dataset if group is None else f'{group}/{dataset}', start=start, stop=stop, columns=columns)
    with h5py.File(fp, 'r', libver='latest') as f:
        ds = f[group][dataset] if isinstance(group, str) else f[dataset]

        if 'pandas_version' in ds.attrs:
            return pd.read_hdf(fp, mode='r', key=dataset if group is None else f'{group}/{dataset}', start=start, stop=stop, columns=columns)

        file_columns: np.ndarray = ds.attrs.get('columns')
        column_dtypes: np.ndarray = ds.attrs.get('column_dtypes')
        index: np.ndarray = ds.attrs.get('index')

        if isinstance(columns, list):
            column_locs: list = sum([np.where(file_columns == x)[0].tolist() for x in columns], [])
            file_columns: np.ndarray = file_columns[column_locs]
            column_dtypes: np.ndarray = column_dtypes[column_locs]
        else:
            column_locs: list = list(range(0, len(file_columns)))

        if isinstance(start, int) and isinstance(stop, int):
            row_idx: list = list(range(start, stop))
        elif isinstance(start, int):
            row_idx: list = list(range(start, ds.shape[0]))
        elif isinstance(stop, int):
            row_idx: list = list(range(0, stop))
        else:
            row_idx: list = list(range(0, ds.shape[0]))

        array = ds[row_idx][:, column_locs]

        if index is not None:
            index: np.ndarray = index[row_idx]
            index_names: list = ds.attrs.get('index_names')
            index_dtypes: list = ds.attrs.get('index_dtypes')
            if index_names is None:
                pass
            elif len(index_names) == 1:
                index: pd.Index = pd.Index(pd.Series(index[:, 0], name=index_names[0]).astype(index_dtypes[0]))
            else:
                index: pd.DataFrame = pd.DataFrame(index, columns=index_names.tolist())

                for c, d in zip(index_names, index_dtypes):
                    if bool(re.search(r'^1\.', pd.__version__)):
                        index.loc[:, c] = index.loc[:, c].astype(d)
                    else:
                        index[c] = index.loc[:, c].astype(d)

                index: pd.MultiIndex = pd.MultiIndex.from_frame(index)

        df = pd.DataFrame(array, index=index, columns=file_columns)

        if isinstance(file_columns, np.ndarray) and isinstance(column_dtypes, np.ndarray):
            for c, d in zip(file_columns.tolist(), column_dtypes.tolist()):
                if bool(re.search(r'^1\.', pd.__version__)):
                    df.loc[:, c] = df.loc[:, c].str.decode('utf-8').replace({'-9999999': None}).astype(d) if type(df[c].iloc[0]) == bytes else df.loc[:, c].astype(d)
                else:
                    df[c] = df.loc[:, c].str.decode('utf-8').replace({'-9999999': None}).astype(d) if type(df[c].iloc[0]) == bytes else df.loc[:, c].astype(d)
    return df


def read_h5_group(fp: str, group: str) -> h5py._hl.group.Group:
    """Return specified group from .h5 file."""
    with h5py.File(fp, 'r', libver='latest') as f:
        return f[group]


def _check_make_group(f: h5py._hl.files.File, group: str, replace_groups: bool, group_attrs: dict, groups_created: list) -> list:
    if group in f:
        if (replace_groups and (group not in groups_created)):
            del f[group]
        else:
            return f[group], []

    grp = f.create_group(group)
    for k, v in group_attrs.items():
        grp.attrs[k] = v

    return grp, [group]


if __name__ == '__main__':
    pass
