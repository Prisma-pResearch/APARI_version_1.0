# -*- coding: utf-8 -*-
"""
Generic Dataset class for Modeling.

Created on Fri Mar 18 11:54:16 2022

@author: ruppert20
"""
from collections import OrderedDict
import torch
import h5py
import numpy as np
import torch.nn as nn
import pandas as pd
from sklearn.model_selection import KFold
from typing import Union, List, Tuple
from .Utilities.General.func_utils import get_func
from .Utilities.Logging.log_messages import log_print_email_message as logm


class Dataset:
    """Generic Dataset for AI Models."""

    def __init__(self,
                 cohort: str,
                 h5_file: str,
                 target_outcome_index: Union[int, str, List[str], List[int]] = None,
                 filter_columns: Union[dict, None, List[str]] = None,
                 batch_dset_map: OrderedDict = OrderedDict({'x_admit_binary': {'h5_ds_key': 'bin_admit', 'dtype': torch.FloatTensor},
                                                            'x_admit_continuous': {'h5_ds_key': 'num_admit', 'dtype': torch.FloatTensor},
                                                            'x_admit_embedding': {'h5_ds_key': 'emb_admit', 'dtype': torch.LongTensor},
                                                            'x_q4_continous': {'h5_ds_key': 'num_ts', 'dtype': torch.FloatTensor},
                                                            'x_q4_binary': {'h5_ds_key': 'bin_ts', 'dtype': torch.FloatTensor},
                                                            'y': {'h5_ds_key': 'y', 'dtype': torch.FloatTensor}}),
                 interpretable_keys: list = ['x_admit_binary',
                                             'x_admit_continuous',
                                             'x_admit_pre_embeded',
                                             'x_q4_continous',
                                             'x_q4_binary'],
                 h5_N_key: str = 'bin_admit',
                 cat_embedding_key: OrderedDict = OrderedDict({'x_admit_embedding': 'x_admit_pre_embeded'}),
                 variable_length_seq_keys: list = ['x_q4_continous', 'x_q4_binary', 'y'],
                 start_key: str = 'ts_start_idx',
                 seq_len_key: str = 'ts_seqlens',
                 precache_data: bool = False,
                 k_folds: int = None,
                 train_cohort: bool = False,
                 other_dsets_to_cache: list = None,
                 subset_to_use: List[int] = None):

        self.cohort: str = cohort
        self.h5_file: str = h5_file
        self.file: Union[dict, h5py._hl.files.File, None] = None
        self.batch_dset_map: OrderedDict = OrderedDict()
        self.interpretable_keys: list = interpretable_keys
        self.dims: dict = {}
        self.variable_length_seq_keys: list = variable_length_seq_keys or []
        self.start_key: str = start_key
        self.seq_len_key: str = seq_len_key
        self.cat_embedding_key: OrderedDict = cat_embedding_key or OrderedDict({})
        self.column_names: dict = {}
        self.collation_keys: List[str] = []
        self.ids: np.ndarray = None
        self.ID_Key: str = 'XXXidXXX'
        self.Max_SEQ_Len: int = None
        self.k_folds: dict = {}
        self.X_keys: list = []
        self.h5_batch_map: dict = {}
        self.filter_columns = filter_columns
        self.other_dsets_to_cache = other_dsets_to_cache or []
        self.column_indicies: dict = {}
        self.subset: List[int] = subset_to_use

        assert ('y' in batch_dset_map) or (not train_cohort), 'The outcome "y" must be in the batch_dset_map.'

        assert isinstance(target_outcome_index, (list, int, str)) or (
            target_outcome_index is None), f'Target outcome index must be None, an integer, or a list of integers; however it was found to be of type: {type(target_outcome_index)}'

        with h5py.File(self.h5_file, 'r', libver='latest') as f:
            try:
                self.N = len(subset_to_use) if isinstance(subset_to_use, list) else f[cohort][h5_N_key].shape[0]  # use subset size or the shape of the h5_N_Key for N
            except KeyError as e:
                logm(message=str(f[cohort].keys()), error=True)
                raise Exception(e)

            if 'y' in f[cohort]:
                outcome_names: list = f[cohort]['y'].attrs['columns']

                if target_outcome_index is None:
                    self.n_targets = f[cohort]['y'].shape[1]
                    self.target_outcome_index = list(range(self.n_targets))
                elif isinstance(target_outcome_index, (str, int)):
                    self.n_targets = 1
                    # Get Feature Index if name is passed
                    if isinstance(target_outcome_index, str):
                        target_outcome_index: int = np.where(outcome_names == target_outcome_index)[0][0]
                    self.target_outcome_index = [target_outcome_index]
                else:
                    self.n_targets = len(target_outcome_index)
                    # Get Feature Index if names are passed
                    if isinstance(target_outcome_index[0], str):
                        target_outcome_index: List[int] = sum([np.where(outcome_names == x)[0].tolist() for x in target_outcome_index], [])  # [outcome_names.index(x) for x in target_outcome_index]
                    self.target_outcome_index = target_outcome_index

                # load y
                y = f[cohort]['y'][:, self.target_outcome_index]

                if isinstance(subset_to_use, list):  # subset the y if necessary
                    if y.shape[0] != f[cohort][h5_N_key].shape[0]:
                        raise NotImplementedError('There is currently no subset support for variable length sequence outcomes. Please create an Issue on the Github page if this is necessary for your project.')
                    y = y[subset_to_use]

                # check for invalid values
                invalid_y: np.array = np.array(np.where(y == -9))

                self.need_to_filter: bool = invalid_y.shape[1] > 0

                if self.need_to_filter:
                    if y.shape[0] != self.N:
                        raise NotImplementedError('There is currently no support for fixing variable length sequence outcomes with invalid values. Please create an Issue on Github if this is necessary for your project.')

                    invalid_y = np.sort(invalid_y[0])

                    valid_idx: np.array = np.setdiff1d(np.arange(y.shape[0]), invalid_y)

                    y = y[valid_idx]

                    self.valid_idx = valid_idx

                    new_N = y.shape[0]

                    precache_data: bool = True

                if 'index' in f[cohort]['y'].attrs:
                    self.ids = f[cohort]['y'].attrs['index']

                    if isinstance(subset_to_use, list):
                        self.ids = self.ids[subset_to_use]  # restrict index to the pre-selected subset and match shape of y from above

                    if self.need_to_filter:
                        self.ids = self.ids[self.valid_idx]

                    if 'index_names' in f[cohort]['y'].attrs:
                        self.column_names[self.ID_Key] = f[cohort]['y'].attrs.get('index_names')[:].tolist()

                weights = []
                for i in range(self.n_targets):
                    yi = np.array(y[:, i])
                    n_classes = len(np.unique(yi))
                    try:
                        w = [len(yi) / (n_classes * len(np.where(yi == label)[0])) for label in range(n_classes)]
                    except Exception:
                        if train_cohort:
                            logm(f'The binary outcome either has unexpected values or only one class. Unable to determine class weights given the following levels: {np.unique(yi)}',
                                 raise_exception=True)
                        else:
                            pass
                    weights.append(w)
                self.class_weights = weights

            else:
                if train_cohort:
                    raise Exception('y is required for training datasets')
                if 'index' in f[cohort][h5_N_key].attrs:
                    self.ids = f[cohort][h5_N_key].attrs['index']

                    if isinstance(subset_to_use, list):
                        self.ids = self.ids[subset_to_use]  # restrict index to the pre-selected subset

                    if 'index_names' in f[cohort][h5_N_key].attrs:
                        self.column_names[self.ID_Key] = f[cohort][h5_N_key].attrs.get('index_names')[:].tolist()
                self.need_to_filter = False
            keys_to_drop: list = []

            for k, v in batch_dset_map.items():
                if (v.get('h5_ds_key') not in f[cohort].keys()) and (not train_cohort):
                    keys_to_drop.append(k)
                    continue
                temp_shape = f[cohort][v.get('h5_ds_key')].shape
                self.dims[f'{k}_dim'] = self.n_targets if k == 'y' else temp_shape[1]

                self.h5_batch_map[v.get('h5_ds_key')] = k

                if (temp_shape[0] == f[cohort][h5_N_key].shape[0]) and (k != 'y'):
                    self.X_keys.append(v.get('h5_ds_key'))

                if (k == 'y'):
                    self.column_names[k] = f[cohort][v.get('h5_ds_key')].attrs.get('columns')[self.target_outcome_index].tolist()
                    self.column_indicies[k] = self.target_outcome_index
                else:
                    col_names: list = f[cohort][v.get('h5_ds_key')].attrs.get('columns')[:].tolist()

                    if filter_columns is not None:
                        cols_to_keep: list = filter_columns if isinstance(filter_columns, list)\
                            else filter_columns.get(k, filter_columns.get(v.get('h5_ds_key')))

                        col_indicies = pd.Series(col_names)[pd.Series(col_names).isin(cols_to_keep)]
                        self.column_indicies[k] = col_indicies.index.tolist()
                        col_names: list = col_indicies.tolist()
                        self.dims[f'{k}_dim'] = len(col_names)

                        if len(col_names) == 0:
                            keys_to_drop.append(k)
                    else:
                        self.column_indicies[k] = list(range(len(col_names)))
                    self.column_names[k] = col_names

                if k in self.cat_embedding_key:
                    self.dims[f'{k}_levels'] = f[cohort][v.get('h5_ds_key')].attrs.get('levels',
                                                                                       f[cohort][v.get('h5_ds_key')][:, self.column_indicies[k]].max(axis=0) + 1)[:].tolist()
                if k in self.variable_length_seq_keys:
                    if self.variable_length_seq_keys.index(k) == 0:
                        self.collation_keys.append('x_lens')
                        self.Max_SEQ_Len = f[cohort][seq_len_key][:].astype(int).max(axis=0)
                        if not isinstance(self.Max_SEQ_Len, (int, np.int8, np.int16, np.int32, np.int64)):
                            self.Max_SEQ_Len = self.Max_SEQ_Len[0]
                self.collation_keys.append(k)

            for k in keys_to_drop:
                try:
                    self.X_keys.remove(batch_dset_map[k].get('h5_ds_key'))
                except ValueError:
                    pass

                del batch_dset_map[k]
                try:
                    self.collation_keys.remove(k)
                    if k in self.cat_embedding_key:
                        del self.dims[f'{k}_levels']
                except ValueError:
                    pass

            for k, spec_dict in batch_dset_map.items():
                td: dict = {}
                for k2, v in spec_dict.items():
                    td[k2] = get_func(v) if k2 == 'dtype' else v

                self.batch_dset_map[k] = td

        if (len(set(self.variable_length_seq_keys).intersection(self.collation_keys)) == 0) and ('x_lens' in self.collation_keys):
            self.collation_keys.remove('x_lens')

        if self.need_to_filter:
            self.N = new_N

        self.interpretable_keys = [x for x in self.interpretable_keys if x in list(self.batch_dset_map.keys()) + list(self.cat_embedding_key.values())]

        if isinstance(k_folds, int):
            kfold = KFold(n_splits=k_folds, shuffle=True, random_state=42)

            for fold, (train, test) in enumerate(kfold.split(list(range(self.N)))):
                self.k_folds[fold] = {'train': train, 'test': test}

        if precache_data or (filter_columns is not None) or isinstance(subset_to_use, list):
            self.cache_data(subset=subset_to_use)

    def cache_data(self, subset: List[int] = None):
        """
        Cache data into memory to expedite traninig.

        subset: List[int], optional
            List of indexes to be used to restrict

        Returns
        -------
        None.

        """
        if not isinstance(self.file, dict):
            with h5py.File(self.h5_file, 'r', libver='latest') as f:
                logm('loading dataset into memory', display=True)

                self.file = OrderedDict()
                for k, v in self.batch_dset_map.items():

                    # load array
                    data_array: np.array = f[self.cohort][v.get('h5_ds_key')][:]

                    if isinstance(subset, list) and (k not in self.variable_length_seq_keys):
                        data_array = data_array[subset]

                    # filter columns to specification
                    if (self.filter_columns is not None) or k == 'y':
                        data_array: np.array = data_array[:, self.column_indicies.get(k)]

                    # don't filter var length sequences for it would require re-indexing
                    if self.need_to_filter and (k not in self.variable_length_seq_keys):
                        data_array = data_array[self.valid_idx]

                    # cache array
                    self.file[v.get('h5_ds_key')] = data_array
                # self.file = file
                # self.file = {v.get('h5_ds_key'): f[self.cohort][v.get('h5_ds_key')][:] for v in self.batch_dset_map.values()}
                if len(self.variable_length_seq_keys) > 0:

                    for sk in [self.start_key, self.seq_len_key]:
                        tp: np.array = f[self.cohort][sk][:]

                        if isinstance(subset, list):
                            tp = tp[subset]
                        if self.need_to_filter:
                            tp = tp[self.valid_idx]

                        self.file[sk] = tp

                if len(self.other_dsets_to_cache) > 0:
                    for k in self.other_dsets_to_cache:
                        col_names: list = f[self.cohort][k].attrs.get('columns')[:].tolist()

                        if self.filter_columns is not None:
                            cols_to_keep: list = self.filter_columns if isinstance(self.filter_columns, list)\
                                else self.filter_columns.get(k, self.filter_columns.get(v.get('h5_ds_key')))

                            col_indicies = pd.Series(col_names)[pd.Series(col_names).isin(cols_to_keep)]
                            self.column_indicies[k] = col_indicies.index.tolist()
                            col_names: list = col_indicies.tolist()

                        # load array and filter if necessary
                        data_array: np.array = f[self.cohort][k][:]
                        if isinstance(subset, list):
                            data_array = data_array[subset]
                        if self.need_to_filter:
                            data_array = data_array[self.valid_idx]

                        # filter columns to specification
                        if self.filter_columns is not None:
                            data_array: np.array = data_array[:, self.column_indicies.get(k)]

                        # cache array
                        self.file[k] = data_array
                        self.column_names[k] = col_names
                        self.X_keys.append(k)
        else:
            logm('data already cached', display=True)

    def get_class_weights(self):
        return self.class_weights

    def get_dim(self, key: str) -> int:
        return self.dims.get(f'{key}_dim')

    def get_levels(self, key: str) -> int:
        return self.dims.get(f'{key}_levels')

    def __len__(self):
        return self.N

    def get_start_stop(self, idx) -> tuple:
        if len(self.variable_length_seq_keys) > 0:
            start_idx = self.file[self.start_key][idx][0]
            seqlen = self.file[self.seq_len_key][idx][0]
            stop_idx = start_idx + seqlen
        else:
            start_idx = idx
            seqlen = 0
            stop_idx = idx + 1

        return start_idx, seqlen, stop_idx

    def __getitem__(self, idx, return_type: str = 'dict'):
        if self.file is None:
            self.file = h5py.File(self.h5_file, 'r', libver='latest')[self.cohort]

        start_idx, seqlen, stop_idx = self.get_start_stop(idx)

        out: dict = OrderedDict()

        for k, v in self.batch_dset_map.items():

            if k in self.variable_length_seq_keys:
                out[k] = v.get('dtype')(self.file[v.get('h5_ds_key')][start_idx: stop_idx, :])

                if self.variable_length_seq_keys.index(k) == 0:
                    out['x_lens'] = torch.LongTensor([seqlen])

            else:
                out[k] = v.get('dtype')(self.file[v.get('h5_ds_key')][idx, :])

            # if k == 'y':
            #     if 'y' in self.variable_length_seq_keys:
            #         out[k] = out[k][:, self.target_outcome_index]
            #     else:
            #         out[k] = out[k][self.target_outcome_index]

        if return_type == 'dict':
            return out
        else:
            return list(out.values)

    def collate_fn(self, samples):
        return OrderedDict({k: nn.utils.rnn.pad_sequence([sample[k] for sample in samples],
                                                         batch_first=True,
                                                         padding_value=-999) if k in self.variable_length_seq_keys else
                            torch.stack([sample[k] for sample in samples]) for k in self.collation_keys})

    def load_for_Interpretation(self, idx, model):
        assert len(self.cat_embedding_key) < 2, f'The function only supports up to one cat embedding, however; {len(self.cat_embedding_key)} were found.'
        orig_batch: dict = self.collate_fn([self.__getitem__(idx)])
        if len(self.cat_embedding_key) == 0:
            return orig_batch

        pre_embeded = model.embed(orig_batch)
        return list(OrderedDict({x: orig_batch.get(x, pre_embeded) for x in self.interpretable_keys}).values())

    def loader(self, n: Union[int, list, None] = None, fold: int = None, **kwargs):
        if isinstance(fold, int):
            assert len(self.k_folds) > 0, 'The "k_folds" parameter must be provided upon dataset initializtion in order to use this feature.'
            assert fold in self.k_folds, f'The fold: {fold} is not in the range of available folds. There are {len(self.k_folds)} folds available.'

            if isinstance(n, int):
                return torch.utils.data.DataLoader(dataset=torch.utils.data.Subset(self, indices=self.k_folds[fold].get('train')[:n]),
                                                   collate_fn=self.collate_fn,
                                                   **kwargs),\
                    torch.utils.data.DataLoader(dataset=torch.utils.data.Subset(self, indices=self.k_folds[fold].get('test')[:n]),
                                                collate_fn=self.collate_fn,
                                                **kwargs)

            return torch.utils.data.DataLoader(dataset=torch.utils.data.Subset(self, indices=self.k_folds[fold].get('train')),
                                               collate_fn=self.collate_fn,
                                               **kwargs),\
                torch.utils.data.DataLoader(dataset=torch.utils.data.Subset(self, indices=self.k_folds[fold].get('test')),
                                            collate_fn=self.collate_fn,
                                            **kwargs)

        if isinstance(n, int):
            return torch.utils.data.DataLoader(dataset=torch.utils.data.Subset(self, indices=range(n)),
                                               collate_fn=self.collate_fn,
                                               **kwargs)
        elif isinstance(n, list):
            return torch.utils.data.DataLoader(dataset=torch.utils.data.Subset(self, indices=n),
                                               collate_fn=self.collate_fn,
                                               **kwargs)
        else:
            return torch.utils.data.DataLoader(dataset=self, collate_fn=self.collate_fn,
                                               **kwargs)

    def get_item_as_series(self, idx, include_id: bool = False):
        out: pd.Series = pd.Series(dtype=float)

        for k, v in (self.get_item_with_id(idx) if include_id else self.__getitem__(idx)).items():
            if k in ['x_lens', 'y']:
                continue
            elif k == self.ID_Key:
                out = pd.concat([out,pd.Series(index=self.column_names.get(k),
                                           data=v if len(v.shape) == 1 else v[0, :])])
            else:
                out = pd.concat([out,pd.Series(index=self.column_names.get(k),
                                           data=v.numpy() if len(v.shape) == 1 else v.numpy().mean(axis=0))])

        return out.rename(idx)

    def get_item_with_id(self, idx):
        out: OrderedDict = self.__getitem__(idx)
        if self.ids is not None:
            start_idx, seqlen, stop_idx = self.get_start_stop(idx)

            if 'y' in self.variable_length_seq_keys:
                out[self.ID_Key] = self.ids[start_idx: stop_idx]
            else:
                out[self.ID_Key] = self.ids[idx]

        return out

    def dataset_as_pandas(self, keys: Union[list, None] = None, one_hot_embeded_categories: bool = False) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Return dataset as two pandas dataframes (X, y).

        Parameters
        ----------
        keys : Union[list, None], optional
            List of h5_keys to load as X. The default is None, which will use all datasets with same shape[0] as y.

        Returns
        -------
        X : pd.DataFrame
            Predictor dataframe.
        y : pd.DataFrame
            Target DataFrame.

        """
        if not isinstance(keys, list):
            keys: list = self.X_keys
        else:
            assert len(set(keys).difference(set(self.X_keys))) == 0, f'The following keys are not in the dataset: {set(keys).difference(set(self.X_keys))}'
        self.cache_data()

        X: pd.DataFrame = pd.concat([pd.DataFrame(data=self.file[k], index=self.ids[:, 0], columns=self.column_names.get(self.h5_batch_map.get(k, k))) for k in keys], axis=1)
        y: pd.DataFrame = pd.DataFrame(data=self.file['y'], index=self.ids[:, 0], columns=self.column_names.get('y'))

        if one_hot_embeded_categories:
            if len(self.cat_embedding_key) > 0:
                dset_cat_key: str = list(self.cat_embedding_key.keys())[0]
                if {v: k for k, v in self.h5_batch_map.items()}.get(dset_cat_key) in keys:
                    for col in self.column_names.get(dset_cat_key):
                        X = X.drop(columns=[col])\
                            .merge(pd.get_dummies(X[col].astype(int), prefix=col, prefix_sep='_'),
                                   left_index=True,
                                   right_index=True)

        return X, y

    def get_ids_as_index(self, n: Union[int, None] = None):
        if isinstance(n, int):
            return pd.MultiIndex.from_frame(pd.DataFrame(self.ids[:n], columns=self.column_names.get(self.ID_Key)))
        else:
            return pd.MultiIndex.from_frame(pd.DataFrame(self.ids, columns=self.column_names.get(self.ID_Key)))
