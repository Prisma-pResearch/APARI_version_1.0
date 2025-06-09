# -*- coding: utf-8 -*-
"""
Module to check if tasks are complete or not.

This is a legacy Module.

Created on Tue Oct 15 15:02:00 2019.

@author: ruppert20
"""
complete_notifications: list = []


def _check_complete(thread_pool_task_list: list, name: str = ''):
    """Check if task list is completed or not."""
    if name in complete_notifications:
        return True

    thread_pool_task_list_num_finished: int = 0
    for task in thread_pool_task_list:
        if task[1].running():
            return False
        if task[1].done():
            thread_pool_task_list_num_finished += 1

    if len(thread_pool_task_list) == thread_pool_task_list_num_finished:
        complete_notifications.append(name)
        print('{}_task_list complete'.format(name))
        return True

    return False


def should_we_stop(task_lists: list, name_list: list) -> bool:
    """
    Check if all tasks are complete or not.

    Parameters
    ----------
    task_lists : list
        List of running taks.
    name_list : list
        Name of tasks.

    Returns
    -------
    bool
        Whether all tasks are complete or not.

    """
    status_list: list = []

    for tup in zip(task_lists, name_list):
        status_list.append(_check_complete(thread_pool_task_list=tup[0], name=tup[1]))

    if False not in status_list:
        return True

    return False
