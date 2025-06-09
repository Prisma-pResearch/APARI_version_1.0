# -*- coding: utf-8 -*-
"""
Model Utilities for handling Padded Time Series Tensors.

Created on Wed Apr  6 12:22:32 2022

@author: ruppert20
"""
import os
import torch


def get_mask(x, x_lens):
    mask = (torch.arange(x.size(1), device=x.device)[None, :] < x_lens[:, None]).float()
    mask = mask.transpose(-1, -2)
    return mask


def get_last_nonpadded(x, x_lens):
    n_batch, n_time, n_dim = x.shape
    idx = x_lens - 1
    idx = idx.view(-1, 1).expand(n_batch, n_dim).unsqueeze(1)
    return x.gather(1, idx).squeeze(1)


def unpad_sequence(padded_sequences, lengths, batch_first=False, device=None):
    r"""Unpad padded Tensor into a list of variable length Tensors

    ``unpad_sequence`` unstacks padded Tensor into a list of variable length Tensors.

    Example:
        >>> from torch.nn.utils.rnn import pad_sequence, unpad_sequence
        >>> a = torch.ones(25, 300)
        >>> b = torch.ones(22, 300)
        >>> c = torch.ones(15, 300)
        >>> sequences = [a, b, c]
        >>> padded_sequences = pad_sequence(sequences)
        >>> lengths = torch.as_tensor([v.size(0) for v in sequences])
        >>> unpadded_sequences = unpad_sequence(padded_sequences, lengths)
        >>> torch.allclose(sequences[0], unpadded_sequences[0])
        True
        >>> torch.allclose(sequences[1], unpadded_sequences[1])
        True
        >>> torch.allclose(sequences[2], unpadded_sequences[2])
        True

    Args:
        padded_sequences (Tensor): padded sequences.
        lengths (Tensor): length of original (unpadded) sequences.
        batch_first (bool, optional): whether batch dimension first or not. Default: False.

    Returns:
        a list of :class:`Tensor` objects
    """

    unpadded_sequences = []

    if not batch_first:
        padded_sequences.transpose_(0, 1)

    max_length = padded_sequences.shape[1]
    idx = torch.arange(max_length, device=device)

    # print(padded_sequences.shape)

    for seq, length in zip(padded_sequences, lengths):
        mask = idx < length
        unpacked_seq = seq[mask]
        unpadded_sequences.append(unpacked_seq)

    # print(unpadded_sequences[0].shape)

    return torch.cat(unpadded_sequences, dim=0)
