# -*- coding: utf-8 -*-
"""
Determine Optimal Batch size to ensure files are less than given size threshold.

Created on Tue May 26 16:52:17 2020

@author: ruppert20
"""

import os
import math


def calculate_optimal_batch_size(source_dir: str, size_threshold_mb: int = 500) -> int:
    """
    Determine Optimal Batch size to ensure files are less than given size threshold.

    Parameters
    ----------
    source_dir : str
        Directory to be scanned.
    size_threshold_mb : int, optional
        File Size Limit in megabytes. The default is 500.

    Returns
    -------
    int
        recomended number of batches.

    """
    max_size: int = 0

    for file in os.listdir(source_dir):
        temp_size = os.path.getsize(os.path.join(source_dir, file))

        if temp_size > max_size:
            max_size = temp_size

    return math.ceil(float(max_size) / float(size_threshold_mb * 1e6))
