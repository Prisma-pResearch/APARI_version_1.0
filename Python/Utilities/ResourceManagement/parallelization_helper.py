# -*- coding: utf-8 -*-
"""
Module to Faciliate the running of code in the background using either a ProcessPoolExecutor or ThreadPoolExecutor from concurrent.futures.

Created on Thu Jun 11 12:04:03 2020

@author: ruppert20
"""
from ..Logging.log_messages import log_print_email_message as logm
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from datetime import datetime as dt
import time
import pandas as pd
import traceback
from tqdm import tqdm
import multiprocessing


def done_cb(future, return_results: bool):
    """Log whether futures finish successfully or not."""
    try:
        if return_results:
            return future.result()
        future.result()
    except Exception as e:
        logm(message=e, error=True, log_name=future.kwargs.get('log_name'))


def run_function_in_parallel_v2(function,
                                kwargs_list: list,
                                max_workers: int,
                                update_interval: int = 10,
                                disp_updates: bool = True,
                                list_running_futures: bool = True,
                                log_name: str = None,
                                executor_type: str = 'ProcessPool',
                                return_results: bool = False,
                                show_progress_bar: bool = True,
                                debug: bool = False) -> int:
    """
    Execute Function safely in parallel.

    Parameters
    ----------
    function : function
        Function to execute.
    kwargs_list : list
        list of kwargs to pass to the function.
    max_workers : int
        max number of simultaneous processes.
    update_interval: int, optional
        The number of seconds between updates on pending tasks. The defaul is 10 seconds.
    disp_updates: bool, optional
        Whether updates on pending tasks should be printed to the console. The defaul is True.
    list_running_futures: bool, optional
        Whether running/pending futures should be talied in logs/console The deafault is True.
    log_name: str, optional
        The log name that should be used when logging information. The deafault is None.
    executor_type: str, optional
        Whether a ProcessPool (using physical cores) or ThreadPool (using threads) should be used to execute the tasks.
        The default is 'ProcessPool'. ThreadPool is recomended for IO tasks, while ProcessPool is recomended for compuational tasks.
    return_results: bool, optional
        Whether results should be returned from the futures. The deaful is False, which returns nothing.
    debug : bool, optional
        Whether to run the process in Serial i.e. debug mode or in parallel using a process pool executor. The default is False.

    Returns
    -------
    None or List[dict] (if return_results is True)

    """
    assert executor_type in ['ProcessPool', 'ThreadPool'], f"The executor_type: {executor_type} is invalid. Only ['ProcessPool', 'ThreadPool'] are currently supported."
    if multiprocessing.parent_process() is not None:
        executor_type: str = 'ThreadPool'
    stime = dt.now()
    if return_results:
        out: list = []

    if debug:
        for kwargs in tqdm(kwargs_list, desc=log_name, disable=not show_progress_bar):
            if return_results:
                t_kws: dict = kwargs
                t_kws['future_result'] = function(**kwargs)
                out.append(t_kws)
            else:
                function(**kwargs)
        logm(message=f'All Tasks have completed in {dt.now() - stime}',
             display=False, log_name=log_name)
    else:
        logm(message=f'using {executor_type} executor', display=False, log_name=log_name)

        with tqdm(total=len(kwargs_list), desc=log_name, disable=not show_progress_bar) as pbar:

            with (ProcessPoolExecutor(max_workers=max_workers) if executor_type == 'ProcessPool' else ThreadPoolExecutor(max_workers=max_workers)) as executor:
                futures: pd.Series = pd.Series(dtype=object)
                for kwargs in kwargs_list:
                    future = executor.submit(run_function_safely, function, **kwargs)
                    future.kwargs = kwargs
                    # future.add_done_callback(done_cb)
                    futures = pd.concat([futures, pd.Series([future])], ignore_index=True)

                total_tasks: int = futures.shape[0]
                count: int = 0
                while futures.shape[0] > 0:
                    time.sleep(update_interval / 1000)
                    count += 1
                    completed_mask: pd.Series = futures.apply(lambda x: x.done())

                    if completed_mask.any():
                        for result in futures[completed_mask]:
                            t_kws = result.kwargs
                            t_kws['future_result'] = done_cb(result, return_results=return_results)
                            if return_results:
                                out.append(t_kws)
                        pbar.update(completed_mask.sum())

                    futures = futures[~completed_mask]
                    if futures.shape[0] == 0:
                        logm(message=f'All Tasks have completed in {dt.now() - stime}',
                             display=False, log_name=log_name)
                        if return_results:
                            return out
                        return
                    else:
                        if count == 1000:
                            count: int = 0
                            logm(message=f'{futures.shape[0]} of {total_tasks} are still running',
                                 display=False, log_name=log_name)

                            if list_running_futures:
                                running_mask: pd.Series = futures.apply(lambda x: x.running())
                                if running_mask.any():
                                    futures[running_mask].apply(lambda x: logm(message=x.kwargs.get('log_name', f'{log_name or "Unknown"} Task') + ' is Still Running',
                                                                               display=disp_updates))

    if return_results:
        return out


def run_function_safely(function, **kwargs):
    """Execute function inside a try except block, returning either the function output or the error message plus traceback."""
    try:
        return function(**kwargs)
    except Exception as e:
        try:
            logm(message=e, error=True, log_name=kwargs.get('log_name'), log_dir=kwargs.get('log_dir'))
            return str(e) + str(traceback.format_exc().splitlines())
        except:
            return str(e)
