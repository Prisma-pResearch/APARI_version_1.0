"""
Module for calculating file sizes and estimating memory consumption of tasks.

Created by Ruppert20.

on 06-02-2020

"""
import os
import psutil
import re
import math
from ..FileHandling.io import find_files
from ..PreProcessing.data_format_and_manipulation import convert_to_from_bytes
from ..Logging.log_messages import log_print_email_message as logm


def caculate_available_memory(unit: str = 'Bytes', mem_fp: str = '/sys/fs/cgroup/memory/memory.limit_in_bytes') -> float:
    """
    Calculates the amount of available memory in bytes, GB or MB

    Actions:
    --------------
    1. gets the amount of ram available
    2. converts it to the unit of your choice

    Parameters:
    --------------
    unit: str = 'Bytes' (optional)
        -unit to convert the amount of memory to

    Returns:
    --------------
    float which represents available memory in unit or your specification


    Notes:
    --------------

    """

    mem_avail = psutil.virtual_memory().available

    if os.path.exists(mem_fp):
        with open(mem_fp) as limit:
            mem_avail = min(int(limit.read()), mem_avail)

    return convert_to_from_bytes(value=mem_avail, unit=unit)


def calcuate_largest_file(dir: str, pattern: str = None) -> int:
    """
    Calculates the largest file size in a directory in bytes

    Actions:
    --------------
    1. lists all files in a directory
    2. compares the pattern against each file name to determine if its size should count
    3. returns size of largest file in bytes

    Parameters:
    --------------
    dir: str
        -directory to look for files

    pattern: str = None (optional)
        -pattern of filenames to consider in calculation


    Returns:
    --------------
    size of largest file in bytes: float


    Notes:
    --------------

    """

    largest_file_size: int = 0

    for file_name in os.listdir(dir):

        eligible_file: bool = False

        if isinstance(pattern, str):
            if bool(re.search(pattern=pattern, string=file_name, flags=re.IGNORECASE)):
                eligible_file: bool = True
        else:
            eligible_file = True

        if eligible_file:
            temp_file_size = os.path.getsize(os.path.join(dir, file_name))

            if temp_file_size > largest_file_size:
                largest_file_size = temp_file_size

    return largest_file_size


def calculate_file_size(files: list, directory: str, saftey_margin: int = 3, patterns: list = None) -> int:
    """
    Calculates the cumalitive file size for files matching pattern in a directory

    Actions:
    --------------
    1. lists all files in a directory
    2. compares the pattern against each file name to determine if its size should count
    3. returns size of all files matching pattern multiplied by the saftey margin (overhead used to load the data)

    Parameters:
    --------------
    files: list
        -list of files to look for

    directory: str
        -directory to look for the files

    saftey_margin: int = 3 (optional)
        -overestimated overhead used to load data into memory

    patterns: list = None (optional)
        -file suffixes to search for


    Returns:
    --------------
    cumalitive size of files in directory


    Notes:
    --------------

    """
    out: int = 0

    if isinstance(patterns, list):
        for file in files:
            for file_path in find_files(directory=directory, patterns=patterns, recursive=False):
                out += os.path.getsize(file_path)
    else:
        for file in files:
            out += os.path.getsize(os.path.join(directory, file))

    return out * saftey_margin


def calculate_number_of_batches(dir_path: str) -> int:
    """
    Calculate the number of batches in a given directory.

    Actions:
    --------------
    1. searches a directory for batch numbers and determines how many consective batches there are

    Parameters:
    --------------
    dir_path: str
        -directory to look for files


    Returns:
    --------------
    int (number of batches in directory)


    Notes:
    --------------

    """
    listOfFiles = os.listdir(dir_path)

    file_list = []
    for entry in listOfFiles:
        if bool(re.search(r'_[0-9]+.csv', entry)):
            file_list.append(int(re.sub('_', '', str(re.search('_[0-9]+', entry).group(0)))))
    if len(file_list) == 0:
        file_list.append(0)

    return max(file_list) + 1


def calculate_optimal_workers(function_name: str, data_directory: str = None,
                              max_cpu_cores: int = None, max_memory_GB: int = None,
                              max_cpu_core_percent: float = 1, num_batches: int = None,
                              estimate_for_batch: bool = True, file_list: list = None,
                              patterns: list = None, encounter_file_name: str = 'encounters',
                              or_case_file_name: str = 'or_case_schedule', **logging_kwargs) -> int:
    """
    Calculates optimal max_workers for a parallel job

    Actions:
    --------------
    1. determines the memory requirements of the data required for the function to perform its duties
    2. deterimines how much memory is avialable in the system
    3. determines how many CPU cores are present in the system
    4. determines the optimal number of simultaneious processes to run in a process pool executioner


    Parameters:
    --------------
    function_name: str
        -function to be run in thread pool executioner

    data_directory: str = None (optional)
        -directory where the data is stored

    max_cpu_cores: int = None (optional)
        -manual maximum number of cpu cores

    max_memory_GB: int = None (optional)
        -manual threshold on maximum memory consumption

    max_cpu_core_percent: float = 1
        -max percentage of cores to be dedicated to the task e.g (75% of 4 cores would be 3)

    num_batches: int = None (optional if data_directory is given, otherwise required)
        -number of batches of files in directory to be proccessed

    estimate_for_batch: bool = True (optional)
        Whether to estimate based on one batch or all of the batches

    file_list: list = None (optional)
        -list of files to be considered in size estimation

    patterns: list = None
        -list of file suffix patterns to look for

    Returns:
    --------------
    optimal number of simultaneous workers for a process pool executioner: int


    Notes:
    --------------

    """

    # determines a conservative memory requriement per batch
    if isinstance(data_directory, str):

        num_batches = calculate_number_of_batches(dir_path=data_directory)

        if function_name == 'create_lookup_tables':

            temp_enc_file_size = calcuate_largest_file(dir=data_directory, pattern=encounter_file_name)
            logm(message='largest_encounters_file is: {} MB'.format(convert_to_from_bytes(value=temp_enc_file_size, unit='MB')),
                 **logging_kwargs)

            temp_or_case_file_size = calcuate_largest_file(dir=data_directory, pattern=or_case_file_name)
            logm(message='largest_or_case_file is: {} MB'.format(convert_to_from_bytes(value=temp_or_case_file_size, unit='MB')),
                 **logging_kwargs)

            conservative_estimate: int = (temp_enc_file_size * 3) + (temp_or_case_file_size * 3)

        elif function_name == 'label_batches_with_merged_ids':
            conservative_estimate: int = calcuate_largest_file(dir=data_directory, pattern=None) * 4

        elif function_name == 'generate_outcomes':
            conservative_estimate: int = calculate_file_size(files=file_list, directory=data_directory, patterns=patterns)
        else:
            raise Exception('this function has yet to be implemented for use with the {} function'.format(function_name))
    else:

        if not isinstance(num_batches, int):
            raise Exception('num batches is a required parameter, when no data_directory is provided')

        logm(message='using very conservative estimate for file sizes, for no directory was provided', **logging_kwargs)
        if function_name == 'create_lookup_tables':

            conservative_estimate: int = 4 * (1024**3)

        elif function_name == 'label_batches_with_merged_ids':

            conservative_estimate: int = 10 * (1024**3)

        else:
            raise Exception('this function has yet to be implemented for use with the {} function'.format(function_name))

    logm(message='the conservative estimate for memory per batch is {} GB'.format(convert_to_from_bytes(value=conservative_estimate, unit='GB')))

    mem_avail = caculate_available_memory()

    logm(message='There is {} GB of available memory'.format(convert_to_from_bytes(value=mem_avail, unit='GB')))

    if isinstance(max_memory_GB, int):

        if max_memory_GB < (convert_to_from_bytes(value=mem_avail, unit='GB')):
            mem_avail = convert_to_from_bytes(value=max_memory_GB, unit='GB', to_bytes=True)

    # get number of hardware cpu cores
    system_cores: int = psutil.cpu_count(logical=False)

    if estimate_for_batch:
        potential_batches: int = math.floor(float(mem_avail) / float(conservative_estimate))
    else:
        potential_batches: int = math.floor(float(mem_avail) / (float(conservative_estimate) / (float(system_cores) * max_cpu_core_percent)))

    logm(message=f'Theoretically {potential_batches} batches could be run in parallel', **logging_kwargs)

    if isinstance(max_cpu_cores, int):

        # check it is not greater than number of threads available, if so sets to the max threshold
        if max_cpu_cores > system_cores:

            max_cpu_cores: int = math.floor(system_cores * max_cpu_core_percent)

        else:
            logm(message='using user defined limit of {} threads'.format(max_cpu_cores), **logging_kwargs)
    else:
        # if no max threads is provided, sets to max threads cpu threshold
        max_cpu_cores: int = math.floor(system_cores * max_cpu_core_percent)

        logm(message='The for a {} percent utilization, there are {} cores available'.format(max_cpu_core_percent * 100, max_cpu_cores), **logging_kwargs)

    if (num_batches < potential_batches) and (function_name == 'create_lookup_tables'):
        logm(message='there are more cores avaialble then batches that can be run simultaneously, running all batches simultaneously', **logging_kwargs)
        return num_batches

    if max_cpu_cores < potential_batches:
        logm(message=f'There are less cores available then potential_batches, will use {max_cpu_cores} cores', **logging_kwargs)
        return max_cpu_cores

    logm(message=f'There is sufficient capacity to run {potential_batches} cores simultaneously', **logging_kwargs)
    return potential_batches


if __name__ == '__main__':
    pass
