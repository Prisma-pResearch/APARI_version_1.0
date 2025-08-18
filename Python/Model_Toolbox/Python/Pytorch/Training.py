# -*- coding: utf-8 -*-
"""
Training and Evaluation Loops for Pytorch Models.

Created on Thu Apr  7 09:50:07 2022

@author: ruppert20

"""
import re
import os
# import h5py
import numpy as np
import pandas as pd
# from sklearn.metrics import roc_auc_score, average_precision_score, f1_score
import torch
import torch.nn as nn
# import torch.nn.functional as F
import pytorch_lightning as pl
from pytorch_lightning.callbacks import EarlyStopping, ModelCheckpoint
from pytorch_lightning import seed_everything
from datetime import datetime as dt
from ..DataSet import Dataset
from .Model_and_Layers import Model
from ..Utilities.FileHandling.io import check_load_df, make_if_not_exists, save_data
from ..Utilities.General.func_utils import get_func
import copy
from typing import List, Union
seed_everything(42, workers=True)


def train(config: dict, train_data: Dataset, val_data: Union[Dataset, List[Dataset]], test_data: Dataset = None):
    print('********************')
    for (k, v) in config.items():
        if (not k.startswith('dim')) and ((k not in ['class_weights', 'monitor', 'mode', 'dataset', 'save_dir'])
                                          and (not bool(re.search(r'^key_|label$', k)))):
            print('* %s = %s' % (k, v))
    print('********************')

    best_score: float = None
    best_checkpoint: str = None

    for fold in list(range(len(train_data.k_folds))) if len(train_data.k_folds) else [None]:
        if fold is not None:
            print(f'Training Fold: {fold}')
            train_loader, test_loader = train_data.loader(batch_size=config['batch_size'], n=config['n'], num_workers=config['num_workers'], fold=fold)
        else:
            train_loader = train_data.loader(batch_size=config['batch_size'], n=config['n'], num_workers=config['num_workers'])
            test_loader = test_data.loader(batch_size=config['batch_size'], n=config['n'], num_workers=config['num_workers'])

        dirpath = os.path.join(config['save_path'], f'fold_{fold}') if fold is not None else config['save_path']

        make_if_not_exists(dirpath)

        early_stopping = EarlyStopping(monitor=config['monitor'], patience=config['patience'], mode=config['mode'])
        checkpoint = ModelCheckpoint(dirpath=dirpath, filename=config['model_name'], monitor=config['monitor'],
                                     mode=config['mode'], save_top_k=1, verbose=False, save_last=False)

        trainer = pl.Trainer(accelerator='gpu' if config['gpus'] > 0 else 'cpu',
                             devices=config['gpus'] if config['gpus'] > 0 else "auto",
                             num_sanity_val_steps=0,
                             callbacks=[early_stopping, checkpoint],
                             max_epochs=config.get('max_epochs', 50),
                             strategy='ddp',
                             inference_mode=True
                             )

        model = Model(config=config)

        trainer.fit(model, train_loader, test_loader)
        print('Best model checkpoint = %s' % checkpoint.best_model_path)

        if (best_score is None):
            best_score: float = checkpoint.best_model_score
            best_checkpoint: str = checkpoint.best_model_path
        elif checkpoint.best_model_score > best_score:
            best_score: float = checkpoint.best_model_score
            best_checkpoint: str = checkpoint.best_model_path

        if config.get('one_fold_only', False):
            break

    if best_checkpoint != checkpoint.best_model_path:
        print('Loading Best Checkpoint: ', best_checkpoint)
        model: Model = Model.load_from_checkpoint(best_checkpoint)

    val_aurocs = trainer.test(model, dataloaders=[x.loader(batch_size=config['batch_size'],
                                                           n=config['n'],
                                                           shuffle=False,
                                                           num_workers=config['num_workers']) for x in val_data])

    for i, ds in enumerate(val_data):
        save_data(df=val_aurocs[i],
                  out_path=os.path.join(config['save_path'], f'{ds.cohort}_performance.json'))

    save_data(df=best_checkpoint,
              out_path=os.path.join(config['save_path'], 'best_model_checkpoint.txt'))

    return model, val_aurocs


def get_predictions(model, data: Dataset, config: dict, return_actual: bool = True):
    data_loader = data.loader(batch_size=config['batch_size'], n=config['n'], shuffle=False, num_workers=config['num_workers'])
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = model.to(device).eval()
    print(f"✅ Model is on: {next(model.parameters()).device}")

    y_true_list, y_pred_list = [], []
    with torch.no_grad():
        for batch in data_loader:
            # Move only tensor elements in batch to GPU/CPU
            batch = {key: val.to(device) if isinstance(val, torch.Tensor) else val
                      for key, val in batch.items()}
            outputs = model(batch, train=False, return_actual=return_actual)
            y_pred_list.append(outputs['y_pred'].cpu().numpy())
            if return_actual:
                y_true_list.append(outputs['y_true'].cpu().numpy())

    if return_actual:
        df_true = pd.DataFrame(np.concatenate(y_true_list, axis=0).astype('int'),
                               columns=['y_true_%s' % outcome for outcome in config['outcomes']])
    df_pred = pd.DataFrame(np.concatenate(y_pred_list, axis=0), columns=['y_pred_%s' % outcome for outcome in config['outcomes']])

    if return_actual:
        df = pd.concat([df_true, df_pred], axis=1)
    else:
        df = df_pred
    df = df[sorted(df.columns, key=lambda x:x[7:])]
    df.index = data.get_ids_as_index(n=config['n'])

    return df


def update_config(config: dict, train_data: Dataset):
    """
    Populate Layer Dimensions, Class Weights, and Outcome names from training data.

    Parameters
    ----------
    config : dict
        DESCRIPTION.
    train_data : Dataset
        DESCRIPTION.

    Returns
    -------
    config : TYPE
        DESCRIPTION.

    """
    config['n_targets'] = train_data.n_targets
    for k, v in copy.deepcopy(config).items():
        if 'key' in k:
            if v in list(config.get('dataset').get('batch_dset_map').keys()):
                config[re.sub(r'^key', 'dim', k)] = train_data.get_dim(v)

                if 'embedding' in k:
                    config[re.sub(r'^key_', '', k) + '_levels'] = train_data.get_levels(v)
            else:
                config.pop(k)

    config['class_weights'] = train_data.get_class_weights()
    config['outcomes'] = train_data.column_names.get('y')
    return config


def _config_subset(ds_config: dict, group: str) -> dict:
    """
    Configure the subset_to_use parameter for dataset kwargs.

    Parameters
    ----------
    ds_config : dict
        dictionary of kwargs for a dataset.
    group : str
        group to use subset from.

    Returns
    -------
    dict
        configured kwargs dict for dataset.

    """
    td: dict = copy.deepcopy(ds_config)
    td['subset_to_use'] = td.get(f'{group}_subset')
    for x in [k for k in td.keys() if '_subset' in k]:
        td.pop(x)

    return td


def load_data(config: dict):
    """
    Load Datasets for traning, testing, and/or validation.

    Parameters
    ----------
    config : dict
        Configuration dictionary for datasets.

        Required Keys:
            *train_group: str
            *dataset: dict
                *cohort: str,
                *h5_file: str

        Optional Keys:
            *test_group: str
            *val_group: str
            *val_group2: str

    Returns
    -------
    train_data : Dataset
        Training Dataset.
    val_data : Union[Dataset, None]
        Validation dataset(s) or None (Used to measure algorithm performance on unseen data).
    test_data : Union[Dataset, None]
        Testing Dataset or None (used to judge training progress).

    """
    train_data = Dataset(cohort=config.get('train_group'),
                         **_config_subset(ds_config=config.get('dataset'), group='train_group'),
                         train_cohort=True)

    try:
        test_data = Dataset(cohort=config.get('test_group'), **_config_subset(ds_config=config.get('dataset'), group='test_group'))
    except:
        test_data = None

    if isinstance(config.get('val_group'), str):
        val_data: Dataset = Dataset(cohort=config.get('val_group'), **_config_subset(ds_config=config.get('dataset'), group='val_group'))
    else:
        val_data = test_data or train_data

    if isinstance(config.get('val_group2'), str):
        val_data: List[Dataset] = [val_data, Dataset(cohort=config.get('val_group2'), **_config_subset(ds_config=config.get('dataset'), group='val_group2'))]
    else:
        val_data: List[Dataset] = [val_data]

    N_SAMPLES: int = train_data.N
    SeqLens: list = [train_data.Max_SEQ_Len]

    if test_data is not None:
        N_SAMPLES += test_data.N
        SeqLens.append(test_data.Max_SEQ_Len)

    valN = sum([ds.N for ds in val_data])
    SeqLens += [ds.Max_SEQ_Len for ds in val_data]
    N_SAMPLES += valN

    if config.get('verbose', True):
        print('# samples =', N_SAMPLES)
        print('    # train = {:,} ({:.1%})'.format(train_data.N, 100 * train_data.N / N_SAMPLES))
        print('    # val = {:,} ({:.1%})'.format(valN, 100 * valN / N_SAMPLES))
        if test_data is not None:
            print('    # test = {:,} ({:.1%})'.format(test_data.N, 100 * test_data.N / N_SAMPLES))
        print('# Outcomes =', train_data.n_targets)
        for outcome in train_data.column_names.get('y'):
            print('    ', outcome)
        try:
            print('# Max sequence length =', max(SeqLens))
        except TypeError:
            pass
    return train_data, val_data, test_data


def train_model(config: dict):
    assert os.path.exists(config.get('dataset').get('h5_file')), f"The datasource {config.get('h5_file')} could not be found!"
    assert config.get('which', 'static_and_time_series') in [
        'static_only', 'time_series_only', 'static_and_time_series'], f"Unsupported model type: {config.get('which')}, currently only 'static_only', 'time_series_only', 'static_and_time_series' are supported"
    assert isinstance(config.get('save_dir'), str), 'Save directory is required'

    config['train_group'] = config.get('train_group', 'train')
    config['val_group'] = config.get('val_group', config.get('train_group').replace('Development', 'Validation'))
    config['test_group'] = config.get('test_group', config.get('train_group').replace('Development', 'Test'))

    # Model
    config['which'] = config.get('which', 'static_and_time_series')
    config['hidden_dim'] = config.get('hidden_dim', 64)
    config['activation'] = config.get('activation', nn.ReLU)
    if isinstance(config['activation'], str):
        config['activation'] = get_func(config['activation'])
    config['n_intraop_time_series_seq_layers'] = config.get('n_intraop_time_series_seq_layers', 1)
    config['dataset']['target_outcome_index'] = config.get('target_idx', None)

    # Optimization and regularization
    config['lr'] = config.get('lr', 1e-3)
    config['dropout'] = config.get('dropout', 0.2)
    config['weight_decay'] = float(config.get('weight_decay', 1e-4))
    config['optimizer'] = config.get('optimizer', 'adam')

    # Training and early stopping
    config['batch_size'] = config.get('batch_size', 64)
    config['patience'] = config.get('patience', 3)
    config['monitor'] = config.get('monitor', 'val_auroc_mean')
    config['mode'] = config.get('mode', 'max')

    # For saving model
    config['model_name'] = config.get('model_name', 'model')
    # Path to where files should be saved. '.' is current directory.
    config['save_path'] = config.get('save_path', os.path.join(config.get('save_dir'), dt.now().strftime('%Y-%m-%d_%H-%M-%S')))

    # Misc
    config['gpus'] = config.get('gpus', 1)  # Number of GPUs to use
    config['n'] = config.get('n', None)  # To quickly debug, set n to an integer # of samples for quickly making sure everything works
    config['num_workers'] = min(os.cpu_count(), config.get('num_workers', 16))

    make_if_not_exists(config.get('save_path'))

    train_data, val_data, test_data = load_data(config=config)
    config = update_config(config, train_data)

    model, val_aurocs = train(config=config, train_data=train_data, val_data=val_data, test_data=test_data)

    for ds in val_data:
        get_predictions(model=model, data=ds, config=config)\
            .to_csv('%s/predictions_%s_%s.csv' % (config.get('save_path'), config.get('model_name'), ds.cohort), index=True)

    return config['save_path']


if __name__ == '__main__':
    # example only
    for lr in [1e-3, 1e-4, 5e-5]:
        for optim in ['adam']:
            for bs in [64, 128]:
                for wd in [1e-4, 5e-5, 1e-5]:
                    for dropout in [0.2, 0.3, 0.4, 0.5, 0.6]:
                        for nl in [1, 2]:
                            for hidden_dim in [64, 32, 74, 84, 94, 104, 128]:
                                for outcomes in [None]:  # 0, 1, 2, 3, 4, 5, 6, 7, 8]:
                                    for metric in ['auroc']:  # ['auprc', 'auroc']:
                                        td: dict = check_load_df('config yaml')
                                        config: dict = td.get('model')
                                        config['dataset'] = td.get('dataset')
                                        config.update({
                                            # Dataset
                                            'train_group': 'Development|Test_UF',  # 'Development_UF',
                                            # 'val_group2': 'Validation_UF_and_JAX',  # 'Validation_JAX',
                                            'val_group': 'Development|Test_UF',  # 'Validation_UF',
                                            'test_group': None,

                                            # Model
                                            # 'which': 'static_and_time_series',
                                            'hidden_dim': hidden_dim,
                                            'n_intraop_time_series_seq_layers': nl,
                                            'target_idx': outcomes,

                                            # Optimization and regularization
                                            'lr': lr,
                                            'dropout': dropout,
                                            'weight_decay': wd,
                                            'optimizer': optim,

                                            # Training and early stopping
                                            'batch_size': bs,
                                            'monitor': f'val_{metric}_mean',

                                            # Misc
                                            'gpus': 3,  # Number of GPUs to use
                                            'n': None,  # To quickly debug, set n to an integer # of samples for quickly making sure everything works
                                            'num_workers': 24,  # number of cpu cores to use for data loader
                                        })
                                        train_model(config)
