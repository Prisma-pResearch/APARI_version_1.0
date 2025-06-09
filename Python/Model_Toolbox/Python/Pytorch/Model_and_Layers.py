# -*- coding: utf-8 -*-
"""
Created on Wed Apr  6 12:22:02 2022

@author: ruppert20
"""
import os
import numpy as np
# import pandas as pd
from sklearn.metrics import roc_auc_score, average_precision_score, f1_score
import torch
import torch.nn as nn
import torch.nn.functional as F
import pytorch_lightning as pl
# from pytorch_lightning.callbacks import EarlyStopping, ModelCheckpoint
from pytorch_lightning import seed_everything
# from datetime import datetime as dt
from typing import Union
from collections import OrderedDict
from .utils import get_mask, unpad_sequence
import pickle
seed_everything(42)


class LinearLayer(nn.Module):
    def __init__(self, config: dict, in_dim: int, out_dim: int):
        super().__init__()
        self.linear = nn.Linear(in_dim, out_dim)
        self.activation = config['activation']()
        self.dropout = nn.Dropout(config['dropout'])

    def forward(self, x):
        x = self.linear(x)
        x = self.activation(x)
        x = self.dropout(x)
        return x


class CategoricalEmbedding(nn.Module):
    def __init__(self, config: dict, label: str):
        super().__init__()
        self.label = label
        self.levels = config.get(f'{label}_embedding_levels')

        self.embeddings = nn.ModuleList([
            nn.Embedding(level, config.get('hidden_dim'))
            for level in self.levels
        ])

        self.linear = LinearLayer(config, in_dim=config.get('hidden_dim') * len(self.levels), out_dim=config.get('hidden_dim'))

    def forward(self, x: torch.LongTensor):
        h = torch.cat([self.embeddings[i](x[:, i]) for i in range(len(self.embeddings))], dim=-1)
        h = self.linear(h)
        return h

    def pre_embed_for_interpretability(self, x: torch.LongTensor) -> torch.FloatTensor:
        return torch.cat([self.embeddings[i](x[:, i]) for i in range(len(self.embeddings))], dim=-1)

    def agg_pre_embed_for_interpretability(self, x: torch.FloatTensor) -> torch.FloatTensor:
        return self.linear(x)


class TimeSeriesAttention(nn.Module):
    def __init__(self, config: dict):
        super().__init__()
        self.W = nn.Linear(config.get('hidden_dim'), config.get('hidden_dim'))
        self.v = nn.Linear(config.get('hidden_dim'), 1)
        self.tanh = nn.Tanh()
        self.softmax = nn.Softmax(dim=1)
        self.continuous_prediction: bool = config.get('continuous_pred', False)

    def forward(self, x: torch.tensor, x_lens: torch.LongTensor):
        mask = get_mask(x, x_lens)
        scores = self.v(self.tanh(self.W(x)))
        if mask is not None:
            scores[mask == 0] = -float('inf')

        scores = self.softmax(scores)
        x_weighted = x * scores

        if not self.continuous_prediction:
            x_weighted = x_weighted.sum(dim=1)  # Sums contribution over time
        return x_weighted, scores


class MultiVariateLinearEncoders(nn.Module):
    def __init__(self, config: dict, label: str):
        super().__init__()

        self.label = label
        self.linear_continuous = LinearLayer(config, in_dim=config.get(f'dim_{label}_continuous'), out_dim=config.get('hidden_dim')) if ((f'dim_{label}_continuous' in config) and (config.get(f'dim_{label}_continuous', 0) > 0)) else None
        self.linear_continuous_indicator = LinearLayer(config, in_dim=config.get(f'dim_{label}_continuous_indicator'),
                                                       out_dim=config.get('hidden_dim')) if ((f'dim_{label}_continuous_indicator' in config) and (config.get(f'dim_{label}_continuous_indicator', 0) > 0)) else None
        self.linear_binary = LinearLayer(config, in_dim=config.get(f'dim_{label}_binary'), out_dim=config.get('hidden_dim')) if ((f'dim_{label}_binary' in config) and (config.get(f'dim_{label}_binary', 0) > 0)) else None
        self.linear_binary_indicator = LinearLayer(config, in_dim=config.get(f'dim_{label}_binary_indicator'), out_dim=config.get('hidden_dim')) if ((f'dim_{label}_binary_indicator' in config) and (config.get(f'dim_{label}_binary_indicator', 0) > 0)) else None

        self.categorical_embedding = CategoricalEmbedding(config, label=label) if ((f'{label}_embedding_levels' in config) and (len(config.get(f'{label}_embedding_levels', [])) > 0)) else None

        self.layer_count = int(sum([int((len(config.get(x, [])) > 0) if (x == f'{label}_embedding_levels') else (config.get(x, 0) > 0)) for x in [f'{label}_embedding_levels', f'dim_{label}_binary_indicator',
                                                                                                                                                  f'dim_{label}_binary', f'dim_{label}_continuous',
                                                                                                                                                  f'dim_{label}_continuous_indicator']]))

        self.batch_key_continuous = config.get(f'key_{label}_continuous')
        self.batch_key_continuous_indicator = config.get(f'key_{label}_continuous_indicator')
        self.batch_key_binary = config.get(f'key_{label}_binary')
        self.batch_key_binary_indicator = config.get(f'key_{label}_binary_indicator')
        self.batch_key_embedding = config.get(f'key_{label}_embedding')

    def forward(self, batch: dict, pre_embeded: bool = False):
        outputs: list = []

        for layer, key in OrderedDict({self.linear_continuous: self.batch_key_continuous,
                                       self.linear_continuous_indicator: self.batch_key_continuous_indicator,
                                       self.linear_binary: self.batch_key_binary,
                                       self.linear_binary_indicator: self.batch_key_binary_indicator,
                                       self.categorical_embedding: self.batch_key_embedding}).items():
            if key is not None:
                if layer is not None:
                    if (key == self.batch_key_embedding) and pre_embeded:
                        outputs.append(layer.agg_pre_embed_for_interpretability(batch[key]))
                    else:
                        outputs.append(layer(batch[key]))

        return torch.cat(outputs, dim=-1)


class StaticEncoder(nn.Module):
    def __init__(self, config: dict, label: str):
        super().__init__()
        self.label = label

        self.multivariate_encoders = MultiVariateLinearEncoders(config=config, label=label)

        self.linear_combined = LinearLayer(config, in_dim=self.multivariate_encoders.layer_count * config.get('hidden_dim'), out_dim=config.get('hidden_dim'))

    def forward(self, batch: dict, pre_embeded: bool = False):
        return self.linear_combined(self.multivariate_encoders(batch, pre_embeded=pre_embeded))


class VariableLengthTimeSeriesEncoder(nn.Module):
    def __init__(self, config: dict, time_series_label: str, static_label: str):
        super().__init__()

        self.static_label = static_label

        self.multivariate_time_series_encoders = MultiVariateLinearEncoders(config=config, label=time_series_label)

        self.multivariate_static_encoders = None if static_label is None else StaticEncoder(config=config, label=static_label)

        self.gru = nn.GRU(config.get('hidden_dim') * self.multivariate_time_series_encoders.layer_count, config.get('hidden_dim'), batch_first=True,
                          num_layers=config[f'n_{time_series_label}_layers'])

        self.atts = nn.ModuleList([TimeSeriesAttention(config) for _ in range(config.get('n_targets'))])

        self.concat_linears = nn.ModuleList(
            [LinearLayer(config, in_dim=config.get('hidden_dim') * (2 if static_label is not None else 1), out_dim=config.get('hidden_dim')) for _ in
             range(config.get('n_targets'))])

    def forward(self, batch: dict, pre_embeded: bool = False):
        self.gru.flatten_parameters()
        h_seq, _ = self.gru(self.multivariate_time_series_encoders(batch))

        h_static: Union[torch.FloatTensor, None] = None if self.static_label is None else self.multivariate_static_encoders(batch, pre_embeded=pre_embeded)

        task_contexts = []
        for (att, linear) in zip(self.atts, self.concat_linears):
            context, scores = att(h_seq, batch['x_lens'])

            if h_static is not None:
                context: torch.FloatTensor = torch.cat([context, h_static], dim=-1)

            task_contexts.append(linear(context))
        task_contexts = torch.stack(task_contexts, dim=1)

        return task_contexts


class EncounterEncoder(nn.Module):
    def __init__(self, config, static_label: str, time_series_seq_label: str, static_time_series_label: str):
        super().__init__()

        self.static_encoder = StaticEncoder(config, label=static_label)
        self.time_series_encoder = VariableLengthTimeSeriesEncoder(config, time_series_label=time_series_seq_label, static_label=static_time_series_label)
        self.linears = nn.ModuleList(
            [LinearLayer(config, in_dim=2 * config.get('hidden_dim'), out_dim=config.get('hidden_dim')) for _ in
             range(config.get('n_targets'))])
        self.continuous_pred: bool = config.get('continuous_pred', False)

    def forward(self, batch: dict, pre_embeded: bool = False):
        h_time_series_tasks = self.time_series_encoder(batch, pre_embeded=pre_embeded)

        h_static = self.static_encoder(batch, pre_embeded=pre_embeded)

        if self.continuous_pred:
            h_static = h_static.unsqueeze(dim=1).repeat(1, h_time_series_tasks.shape[2], 1)

            h_combined = torch.stack(
                [linear(torch.cat([h_static, h_time_series_tasks[:, i, :, :]], dim=-1)) for (i, linear) in enumerate(self.linears)],
                dim=2)
        else:
            h_combined = torch.stack(
                [linear(torch.cat([h_static, h_time_series_tasks[:, i, :]], dim=-1)) for (i, linear) in enumerate(self.linears)],
                dim=1)

        return h_combined


class Model(pl.LightningModule):
    """Dynamic Model with static and time series inputs that can generate continous or single predictions and automatically adjust configuration based on Dataset input."""

    def __init__(self, config):
        super().__init__()
        self.config = config
        self.class_weights_tensor = torch.FloatTensor(config['class_weights'])
        self.which = config['which']
        self.optim_name = config['optimizer']
        self.mutually_exclusive = config.get('mutually_exclusive', False)
        self.continuous_pred: bool = config.get('continuous_pred', False)

        if self.which == 'static_only':
            self.encoder = StaticEncoder(config, label=config.get('static_label'))
        elif self.which == 'time_series_only':
            self.encoder = VariableLengthTimeSeriesEncoder(config, time_series_label=config.get('seq_time_series_label'), static_label=config.get('static_time_series_label'))
        elif self.which == 'static_and_time_series':
            self.encoder = EncounterEncoder(config, static_label=config.get('static_label'),
                                            time_series_seq_label=config.get('seq_time_series_label'),
                                            static_time_series_label=config.get('static_time_series_label'))
        else:
            raise ValueError(f'{self.which} must be one of the following: static_only, time_series_only, static_and_time_series')

        self.outputs = nn.ModuleList([nn.Linear(config.get('hidden_dim'), 2) for _ in range(config.get('n_targets'))])

        self.save_hyperparameters()
        self.train_losses = []
        self.validation_step_outputs = []
        self.test_step_outputs = []

    def forward(self, batch: dict, pre_embeded: bool = False, train: bool = True, return_actual: bool = False):
        h = self.encoder(batch, pre_embeded=pre_embeded)

        if self.which == 'static_only':
            y = torch.stack([output(h) for output in self.outputs], dim=1)

        elif self.which in ['time_series_only', 'static_and_time_series']:
            if self.continuous_pred:
                y = unpad_sequence(torch.stack([output(h[:, :, i]) for (i, output) in enumerate(self.outputs)], dim=2),
                                   lengths=batch['x_lens'],
                                   batch_first=True,
                                   device=self.device)
            else:
                y = torch.stack([output(h[:, i, :]) for (i, output) in enumerate(self.outputs)], dim=1)
        else:
            raise ValueError

        if self.mutually_exclusive or (not train):
            if train:
                y = F.log_softmax(y, dim=1)
            else:
                y = F.softmax(y, dim=2)

        y: torch.FloatTensor = y[:, :, 1]

        out: dict = {'y_pred': y}

        if self.continuous_pred:
            out['y_lens'] = batch['x_lens']

        if return_actual:
            out['y_true'] = batch['y']

        return out

    def loss(self, outputs, batch):
        pos_weight = torch.FloatTensor([w[1] for w in self.class_weights_tensor]).to(self.device)

        if self.continuous_pred:
            y = unpad_sequence(batch['y'],
                               lengths=batch['x_lens'],
                               batch_first=True,
                               device=self.device).to(self.device)
        else:
            y = batch['y']

        y_hat = outputs['y_pred'].to(self.device)

        target = y.argmax(dim=1) if self.mutually_exclusive else y

        if self.mutually_exclusive:
            return F.nll_loss(input=input, target=target)

        return F.binary_cross_entropy_with_logits(input=y_hat,
                                                  target=target,
                                                  pos_weight=pos_weight)

    def configure_optimizers(self):
        if self.optim_name.lower() == 'adam':
            optimizer = torch.optim.Adam(self.parameters(), lr=self.config['lr'], weight_decay=self.config['weight_decay'])
        elif self.optim_name.lower() == 'adadelta':
            optimizer = torch.optim.Adadelta(self.parameters(), lr=self.config['lr'], weight_decay=self.config['weight_decay'])
        else:
            raise Exception(f'Unrecognized optimizer: {self.optim_name}')
        return optimizer

    def training_step(self, batch, batch_idx):
        outputs = self(batch)
        loss = self.loss(outputs, batch)
        self.train_losses.append(loss.item())

        return {
            'loss': loss
        }

    def eval_step(self, batch, batch_idx, which):
        outputs = self(batch)
        loss = self.loss(outputs, batch)
        self.log('%s_loss' % which, loss, sync_dist=True)
        if self.continuous_pred:
            return {
                '%s_loss' % which: loss,
                'y_pred': outputs['y_pred'],
                'y_true': batch['y'],
                'y_lens': batch['x_lens'],
            }
        return {
            '%s_loss' % which: loss,
            'y_pred': outputs['y_pred'],
            'y_true': batch['y'],
        }

    def eval_step_end(self, batch_parts, which):
        if self.continuous_pred:
            return {
                '%s_loss' % which: batch_parts['%s_loss' % which],
                'y_pred': batch_parts['y_pred'],
                'y_true': batch_parts['y_true'],
                'y_lens': batch_parts['y_lens']
            }
        return {
            '%s_loss' % which: batch_parts['%s_loss' % which],
            'y_pred': batch_parts['y_pred'],
            'y_true': batch_parts['y_true']
        }

    def eval_epoch_end(self, outputs, which):
        losses = [x['%s_loss' % which] for x in outputs]
        avg_loss = torch.cat(losses).mean() if len(losses[0].shape) > 0 else torch.stack(losses).mean()

        y_pred = torch.cat([x['y_pred'] for x in outputs], dim=0)

        if self.continuous_pred:
            y_true = torch.cat([unpad_sequence(x['y_true'],
                                               lengths=x['y_lens'],
                                               batch_first=True,
                                               device=self.device) for x in outputs], dim=0)
        else:
            y_true = torch.cat([x['y_true'] for x in outputs])

        y_pred = y_pred.cpu().numpy()
        y_true = y_true.long().cpu().numpy()

        if self.mutually_exclusive:
            y_true = y_true.argmax(axis=1)
            y_pred = y_pred.argmax(axis=1)

            f1_weighted: float = f1_score(y_true=y_true,
                                          y_pred=y_pred,
                                          average='weighted')
            f1s: np.ndarray = f1_score(y_true=y_true,
                                       y_pred=y_pred,
                                       average=None)

        else:

            if (self.config['target_idx'] is None) or isinstance(self.config['target_idx'], list):
                try:
                    aurocs = [roc_auc_score(y_true=y_true[:, i], y_score=y_pred[:, i])
                              for i in range(self.config.get('n_targets'))]
                    # auc_mean = np.mean(aurocs)

                    auprcs = [average_precision_score(y_true=y_true[:, i], y_score=y_pred[:, i])
                              for i in range(self.config.get('n_targets'))]
                    # auprc_mean = np.mean(auprcs)
                except Exception as e:
                    pickle.dump(outputs, open('outputs.p', 'wb'))
                    raise Exception(e)

            else:
                aurocs = [roc_auc_score(y_true=y_true[:, 0],
                                        y_score=y_pred[:, 0])]

                auprcs = [average_precision_score(y_true=y_true[:, 0],
                                                  y_score=y_pred[:, 0])]

        b = 'loss: %.3f  %s_loss: %.3f' % (np.mean(self.train_losses), which, avg_loss)

        for nm, v in ({'F1': f1s} if self.mutually_exclusive else {'auroc': aurocs, 'auprc': auprcs}).items():

            s = b + '  %s_%s_mean: %.3f' % (which, nm, f1_weighted if self.mutually_exclusive else np.mean(v))

            if (self.config['target_idx'] is None) or isinstance(self.config['target_idx'], list):
                s += f'   {which}_{nm}:'
                for i, auc in enumerate(v):
                    s += '  %.3f' % auc

            print('\n' + s)
            self.log(f'{which}_{nm}_mean', f1_weighted if self.mutually_exclusive else np.mean(v), on_epoch=True, sync_dist=True)
            for i, auc in enumerate(v):
                self.log('%s_%s_%d' % (which, nm, i), auc, sync_dist=True)

    ################# Boilerplate (Don't modify) #################
    def validation_step(self, batch, batch_idx):
        self.validation_step_outputs.append(self.eval_step(batch, batch_idx, 'val'))
        # return out

    def test_step(self, batch, batch_idx):
        self.test_step_outputs.append(self.eval_step(batch, batch_idx, 'test'))

    def validation_step_end(self, batch_parts):
        return self.eval_step_end(batch_parts, 'val')

    def test_step_end(self, batch_parts):
        return self.eval_step_end(batch_parts, 'test')

    def on_validation_epoch_end(self):
        self.eval_epoch_end(self.validation_step_outputs, 'val')
        self.validation_step_outputs.clear()
        # return out

    def on_test_epoch_end(self):
        self.eval_epoch_end(self.test_step_outputs, 'test')
        self.test_step_outputs.clear()


class Interpretable_Model(nn.Module):
    """Custom Model Version for use by interpretabilty packages which require a list of float tensors as input and output."""

    def __init__(self, model: Model = None, checkpoint: str = None):
        super().__init__()

        if isinstance(checkpoint, str):
            assert os.path.exists(checkpoint), f'Unable to find checkpoint: {checkpoint}'
            self.model = Model.load_from_checkpoint(checkpoint)
        elif model is not None:
            assert isinstance(model, Model), f'The provided object for the parameter model, was of type: {type(model)}, however; an input of type Model was expected.'
            self.model = model
        else:
            raise Exception('Either a model or a checkpoint file path is required.')

        self.model.eval()

    def embed(self, batch: dict):
        """
        Perform the admit embedding in preparation for running the Interpretable_Model.

        Parameters
        ----------
        batch : dict
            Dictionary of tensors.

        Returns
        -------
        embeded_tensor

        """
        if self.model.which == 'static_and_time_series':
            embedding_mod = self.model.encoder.static_encoder.multivariate_encoders.categorical_embedding
            embedding_key: str = self.model.encoder.static_encoder.multivariate_encoders.batch_key_embedding
        elif self.model.which == 'static_only':
            embedding_mod = self.model.encoder.multivariate_encoders.categorical_embedding
            embedding_key: str = self.model.encoder.multivariate_encoders.batch_key_embedding
        else:
            embedding_key: str = None

        if embedding_mod is None:
            return
        else:
            embeding_func = embedding_mod.pre_embed_for_interpretability

        if embedding_key is None:
            return

        with torch.no_grad():
            return embeding_func(batch[embedding_key])

    # TODO: this needs to be modified for other models based on input tensors, this is only provided as a template
    # def forward(self,
    #             preoperative_static_continuous: torch.Tensor,
    #             preoperative_static_continuous_indicators: torch.Tensor,
    #             preoperative_static_binary: torch.Tensor
    #             ):

    #     batch: dict = {
    #         'preoperative_static_continuous': preoperative_static_continuous,
    #         'preoperative_static_continuous_indicators': preoperative_static_continuous_indicators,
    #         'preoperative_static_binary': preoperative_static_binary,
    #         'x_lens': torch.LongTensor([preoperative_static_binary.shape[1]]).repeat(preoperative_static_binary.shape[0], 1)
    #     }

    #     return self.model(batch, pre_embeded=True, train=False)['y_pred']
    
    def forward(self, *inputs):
        """
        Modify forward method to handle tuple input from SHAP by reconstructing
        the dictionary before passing it to the model.
        """

    # Convert tuple back to dictionary format
        batch = {
            "static_numeric": inputs[0],   # Static continuous features
            "static_binary": inputs[1],    # Static binary features
            "time_series_numeric": inputs[2]  # Time-series numeric features
        }
        if "static_cat_embedding" in self.model.hparams["config"]["dataset"]["batch_dset_map"]:
            if len(inputs) > 3:  # Check if it's actually provided
                batch["static_cat_embedding"] = inputs[3]
            else:
                print("⚠️ Warning: static_cat_embedding expected but not found in inputs. Using zeros.")
                batch["static_cat_embedding"] = torch.zeros((inputs[0].shape[0], 2), dtype=torch.long)  # Placeholder
     
        # Ensure sequence length tensor `x_lens` is included
        batch["x_lens"] = torch.LongTensor([batch["time_series_numeric"].shape[1]]).repeat(batch["time_series_numeric"].shape[0], 1)
     
        return self.model(batch, pre_embeded=True, train=False)['y_pred']
    
       