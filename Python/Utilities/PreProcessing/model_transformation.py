# -*- coding: utf-8 -*-
"""
Module to transform a pandas dataframe into a standardized numeric input.

This is a legacy Module for use with models that require log_ratio transformations for categorical values.

Created on Tue Jul  6 08:17:10 2021.

@author: ruppert20

Updated .loc for pandas 2.X compatability
"""
import numpy as np
import pandas as pd
import json
import re
from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import LabelEncoder
from pickle import dump, load
from datetime import datetime as dt
from ..FileHandling.file_operations import load_data, save_data, force_datetime, force_numeric


def transform_data(source_df: pd.DataFrame, transform_instruction_fp: str, y_cols: list,
                   y_type: type = float, fit: bool = False, min_max_scale_numeric: bool = False,
                   eid: str = 'merged_enc_id', pid: str = 'patient_deiden_id',
                   unique_row_col: str = 'or_case_key', Y_df: pd.DataFrame = None,
                   filter_quantiles: bool = False, quantile_filter: list = [0.025, 0.975],
                   quantile_filter_col: str = None, return_instructions: bool = False,
                   model_version: str = '1.0',
                   columns_to_fill_with_default_value: dict = {'attend_doc': 0},
                   columns_to_fill_with_in_range_random: list = []) -> pd.DataFrame:

    # load transformation instructions if necessary
    if isinstance(transform_instruction_fp, str):
        transform_instruction_df = load_data(transform_instruction_fp, preserve_decimals=True)

    # load source data if necessary
    if isinstance(source_df, str):
        source_df = load_data(source_df)

    # create id col list for convenience
    id_cols: list = source_df.columns.intersection([pid, eid, unique_row_col]).tolist()

    # replace missing and unavailable with None and reduce columns to the intersection of
    source_df\
        .replace({'missing': None,
                  'unavailable': None},
                 inplace=True)

    if fit:
        source_df = source_df[source_df.columns.intersection(transform_instruction_df.name[transform_instruction_df.type != 'X'].dropna().unique().tolist() + id_cols)]
    else:
        source_df = source_df[transform_instruction_df.name[transform_instruction_df[f'model_v{model_version}'].notnull()].tolist() + id_cols]

    # load y if necessary
    if isinstance(Y_df, str):
        Y_df = load_data(Y_df)

    # merge Y and X if necessary
    if isinstance(Y_df, pd.DataFrame):
        source_df = source_df.merge(Y_df[Y_df.columns.intersection(id_cols + y_cols)],
                                    how='inner',
                                    on=Y_df.columns.intersection(id_cols).tolist())

    # drop rows with missing Y's
    source_df.dropna(subset=y_cols, inplace=True, how='any')

    # format column types
    if y_type == float:
        if bool(re.search(r'^1\.', pd.__version__)):
            source_df.loc[:, y_cols] = source_df.loc[:, y_cols].apply(force_numeric).values
        else:
            source_df[y_cols] = source_df.loc[:, y_cols].apply(force_numeric).values
    elif y_type == 'cat':
        source_df[y_cols] = source_df[y_cols].astype("category")
    elif y_type == int:
        if bool(re.search(r'^1\.', pd.__version__)):
            source_df.loc[:, y_cols] = source_df.loc[:, y_cols].apply(force_numeric).astype(int).values
        else:
            source_df[y_cols] = source_df.loc[:, y_cols].apply(force_numeric).astype(int).values

    # filter quantiles
    if filter_quantiles and isinstance(quantile_filter_col, str) and (y_type in [float, int]):
        source_df = source_df[((source_df[quantile_filter_col] < source_df[quantile_filter_col].quantile(q=quantile_filter[1], interpolation='linear'))

                               & (source_df[quantile_filter_col] > source_df[quantile_filter_col].quantile(q=quantile_filter[0], interpolation='linear')))]

    # set id columns as the index
    source_df = source_df.set_index(id_cols)

    if fit:
        # drop features with more than 50% missing
        missingess: pd.DataFrame = (source_df.isnull().sum() * 100 / source_df.shape[0])\
            .reset_index().rename(columns={'index': 'index_col',
                                           0: 'percent_missingness'})
        source_df.drop(columns=missingess.loc[missingess.loc[:, 'percent_missingness'] > 50, 'index_col'].tolist(), inplace=True)

        transform_instruction_df = transform_instruction_df.merge(missingess,
                                                                  how='left',
                                                                  left_on='name',
                                                                  right_on='index_col').drop(columns=['index_col'])

        # ensure necessary columns are present
        for col in [x for x in ['cat_encoder', 'median'] if x not in transform_instruction_df.columns]:
            transform_instruction_df[col] = None

        # record column order prior to processing
        transform_instruction_df.loc[0, f'model_v{model_version}_columns'] = ','.join(source_df.columns.astype(str).tolist())

    else:
        # set column order prior to processing
        source_df = source_df[transform_instruction_df.loc[0, f'model_v{model_version}_columns'].split(',')]

    # keep track of scaled columns
    _scaled_cols: list = []

    # iterate through each feature and perform necessary pre-processing steps
    for f in [x for x in transform_instruction_df.name.dropna().unique() if x in source_df.columns]:
        print(f'Processing {f}')
        f_idx: pd.Series = (transform_instruction_df.name == f)
        f_type: str = transform_instruction_df.loc[f_idx, 'type'].iloc[0]
        cat_enc: str = transform_instruction_df.loc[f_idx, 'cat_encoder'].iloc[0]
        transform_instruction_df.loc[f_idx, f'model_v{model_version}'] = f_type
        med_v: float = force_numeric(transform_instruction_df.loc[f_idx, 'median']).iloc[0]

        # collect median information and fill missing for numerical features
        if f_type == 'num':
            if bool(re.search(r'^1\.', pd.__version__)):
                source_df.loc[:, f] = force_numeric(source_df.loc[:, f]).values
            else:
                source_df[f] = force_numeric(source_df.loc[:, f]).values

            if pd.isnull(med_v):
                med_v = source_df[f].median(skipna=True)
                transform_instruction_df.loc[f_idx, 'median'] = med_v

            source_df[f].fillna(med_v, inplace=True)
            _scaled_cols.append(f)

        # convert binary columns to 1 or 0 if necesssary, fill missing with zero
        elif f_type == 'bin':

            if len(source_df[f].unique()) > 2:
                raise Exception(f'More than 2 levels detected for binary column: The following levels were detected {source_df[f].unique()}')

            if bool(re.search(r'^1\.', pd.__version__)):
                if isinstance(cat_enc, str):
                    td: dict = json.loads(cat_enc)
                    source_df.loc[:, f] = source_df.loc[:, f].apply(lambda x: td.get(x, -1)).fillna('0').astype('category')
    
                else:
                    source_df.loc[:, f] = source_df.loc[:, f].fillna('0').astype(float).astype(int).astype('category')
            else:
                if isinstance(cat_enc, str):
                    td: dict = json.loads(cat_enc)
                    source_df[f] = source_df.loc[:, f].apply(lambda x: td.get(x, -1)).fillna('0').astype('category')
    
                else:
                    source_df[f] = source_df.loc[:, f].fillna('0').astype(float).astype(int).astype('category')

        # peform one hot encoding on column, and drop original
        elif f_type == 'one-hot':
            # fill na with 'missing'
            source_df[f].fillna('missing', inplace=True)

            # drop original column and merge with one_hot version
            source_df = pd.concat([source_df.drop(columns=[f]),
                                   pd.get_dummies(source_df[f], prefix=f)],
                                  axis=1, sort=False)

            one_hot_cols: list = [str(x) for x in source_df.columns if re.match(f, x)]
            if fit:
                transform_instruction_df.loc[f_idx, 'cat_encoder'] = ','.join(one_hot_cols)
            else:
                training_levels: list = cat_enc.split(',')
                # fill levels present in training, but absent from current source as 0
                for c in [x for x in training_levels if x not in one_hot_cols]:
                    source_df[c] = 0

                # attempt to set levels not found in training data to other or missing
                ot_col: str = None
                for l in training_levels:
                    if 'other' in l:
                        ot_col = l
                        break
                    elif 'missing' in l:
                        ot_col = l
                        break

                if pd.isnull(ot_col):
                    source_df.drop(columns=list(set(one_hot_cols) - set(training_levels)), errors='ignore', inplace=True)
                else:
                    new_levels: list = list(set(one_hot_cols) - set(training_levels))

                    if len(new_levels) > 0:
                        match_mask = source_df[new_levels].apply(lambda row: True if row.sum() > 0 else False, axis=1)

                        source_df.loc[match_mask, ot_col] = 1

        # convert datetime to time
        elif f_type == 'datetime-to-time':
            if bool(re.search(r'^1\.', pd.__version__)):
                source_df.loc[:, f] = force_numeric(force_datetime(source_df.loc[:, f]).dt.strftime('%H%M')).astype(int)
            else:
                source_df[f] = force_numeric(force_datetime(source_df.loc[:, f]).dt.strftime('%H%M')).astype(int)
            _scaled_cols.append(f)

        # convert categorical features to numbers
        elif f_type == 'embeded':
            # fill na with 'missing'
            source_df[f].fillna('missing', inplace=True)

            if isinstance(cat_enc, str) and not fit:
                td: dict = json.loads(cat_enc)

                _default_v: int = td.get('missing', td.get('OTHER', columns_to_fill_with_default_value.get(f, -1))) if f not in columns_to_fill_with_in_range_random else None

                source_df.loc[:, f] = source_df.loc[:, f].apply(lambda x: td.get(x, _default_v))

                if f in columns_to_fill_with_in_range_random:
                    _missing_m = source_df[f].isnull()
                    if any(_missing_m):
                        source_df.loc[_missing_m, f] = np.random.randint(low=0, high=3, size=30, dtype=int)
                
                if bool(re.search(r'^1\.', pd.__version__)):
                    source_df.loc[:, f] = source_df.loc[:, f].astype(int).astype('category')
                else:
                    source_df[f] = source_df.loc[:, f].astype(int).astype('category')
            else:
                if not fit:
                    raise Exception(f'No transformation information was provided for column: {f}')
                # create a label encoder instance
                l = LabelEncoder()

                # fit, transform, and convert to categorical
                if bool(re.search(r'^1\.', pd.__version__)):
                    source_df.loc[:, f] = l.fit_transform(source_df.loc[:, f])
                    source_df.loc[:, f] = source_df.loc[:, f].astype('category')
                else:
                    source_df[f] = l.fit_transform(source_df.loc[:, f])
                    source_df[f] = source_df.loc[:, f].astype('category')

                # save encoding info
                transform_instruction_df.loc[f_idx, 'cat_encoder'] = json.dumps(dict(zip(l.classes_, l.transform(l.classes_).tolist())))

                # save embeding size
                transform_instruction_df.loc[f_idx, 'emb_dim'] = f'{len(source_df[f].cat.categories)},{min(50, (len(source_df[f].cat.categories) + 1) // 2)}'
                del l
        else:
            raise Exception(f'{f_type} processing not implemented')

    if y_type in [float, int]:
        _scaled_cols += y_cols

    if fit:
        update_str = f'_updated_{dt.now().strftime("%Y_%m_%d")}.xlsx'

        if ('_updated_' in transform_instruction_fp):
            transform_instruction_fp = re.sub(r'_updated_[0-9]+_[0-9]+_[0-9]+\.xlsx', update_str, transform_instruction_fp)
        else:
            transform_instruction_fp = transform_instruction_fp.replace('_updated', '').replace('.xlsx', update_str)

        # record column order after processing
        transform_instruction_df.loc[1, f'model_v{model_version}_columns'] = ','.join(source_df.columns.astype(str).tolist())

        save_data(df=transform_instruction_df,
                  out_path=transform_instruction_fp)

    else:
        # set column order prior to processing
        source_df = source_df[transform_instruction_df.loc[1, f'model_v{model_version}_columns'].split(',')]

    # min max scale columns if necessary
    if len(_scaled_cols) > 0:

        scaler_fp: str = transform_instruction_fp.replace('.xlsx', '_scalar.pkl')

        if fit:
            scaler = MinMaxScaler()
            source_df[_scaled_cols] = scaler.fit_transform(source_df[_scaled_cols])
            dump(scaler, open(scaler_fp, 'wb'))
        else:
            scaler = load(open(scaler_fp, 'rb'))
            source_df[_scaled_cols] = scaler.transform(source_df[_scaled_cols])

    if return_instructions:
        return source_df, transform_instruction_df

    return source_df


if __name__ == '__main__':
    pass
