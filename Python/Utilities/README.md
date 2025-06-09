
<p align="center">
  <a href="https://example.com/">
    <img src="https://via.placeholder.com/72" alt="Logo" width=72 height=72>
  </a>

  <h3 align="center">Logo</h3>

  <p align="center">
    Module Name Here
    <br>
    <a href="https://github.com/Prisma-pResearch/Utilities/issues/new?template=bug.md">Report bug</a>
    ·
    <a href="https://github.com/Prisma-pResearch/Utilities/issues/new?template=feature.md&labels=feature">Request feature</a>
  </p>
</p>


## Table of contents

- [Quick start](#quick-start)
- [Status](#status)
- [Change Log](#change-log)
- [What's included](#whats-included)
- [Overview](#overview)
- [Bugs and feature requests](#bugs-and-feature-requests)
- [Contributing](#contributing)
- [Requirements](#module-requirements)
- [Creators](#creators)
- [Thanks](#thanks)
- [Copyright and license](#copyright-and-license)


## Quick start

This is a general module designed to faciliate the building of other modules.

This module specializes in generalizable and standardized IO from/to Files and/or SQL databases, logging, and dataformatting.

## Status

Production (Complete documentation for some functions may still be needed)

## Change log

(2023-07-07)
* Addition of Variable_Specification Utilities for downloading, pre-processing, and summarizing data see [setup_OMOP_project_and_download_data](https://github.com/Prisma-pResearch/Utilities/blob/main/ProjectManagement/setup_project.py?plain=1#L27), [load_variables_from_var_spec](https://github.com/Prisma-pResearch/Utilities/blob/main/FileHandling/variable_specification_utiliites.py), and [make_tables_from_var_spec](https://github.com/Prisma-pResearch/Utilities/blob/main/Reporting/make_tables_from_var_spec.py).
* Added [omop_engine_bundle](https://github.com/Prisma-pResearch/Utilities/blob/main/Database/connect_to_database.py?plain=1#L450) which is used to organize OMOP based projects and keep track of locations of tables. Especially useful for multicenter collaborations.
* Added a [Standardized Data Manager](https://github.com/Prisma-pResearch/Utilities/blob/main/PreProcessing/Standardized_data.py) faciliting data standardization, comparision of raw and standardized data, and creation of .h5 files for AI/ML datasets
* Added Pandas Parallelization helper to take advantage of parallel processing in pandas dataframe or pandas series apply statements by just changing .apply to .apply_parallel
* Bug fixes for edge cases in io.py (fixed errors relating to subbatch determination from existing list and improved consistency of loading files)
* Bug fixes for edge cases in process_df_v2 (in cases where columns were forced, but were blank or very sparse)
* Bug fixes for cohort splitting (fixed error where name was repeated in temporal split with different dates)
* OMOP folder has been restructured for easier use
* Have added the ability to mute tqdm progress bars in Parrellization helper
* Updated [requirements.txt](https://github.com/Prisma-pResearch/Utilities/blob/master/requirements.txt)
* Added [enviroment.yaml](https://github.com/Prisma-pResearch/Utilities/blob/master/enviroment.yaml) for buildling enviroment in anaconda

## What's included

<!-- Use Utilities.Documentation.markdown_generator.generate_file_structure to update the below tree -->

```text
├── requirements.txt
├── README.md
├── file-structure.md
├── Encryption
|   ├── file_encryption.py
├── Database
|   ├── connect_to_database.py
|   ├── database_updates.py
├── Reporting
|   ├── create_nih_demographic_table.py
|   ├── calculate_AUROC_CI.py
|   ├── statistical_comparisons.py
├── docker
|   ├── requirements.txt
|   ├── dockerfile
├── General
|   ├── func_utils.py
|   ├── dict_helper.py
|   ├── high_availability_functions.py
├── OMOP
|   ├── Python
|   |   ├── lab_utils.py
|   |   ├── clean_med_lookup.py
|   ├── SQL
|   |   ├── Condition.md
|   |   ├── create_lookup_table.sql
|   |   ├── General.md
|   |   ├── Drug_Era.md
|   |   ├── Payer_Plan.md
|   |   ├── Condition_Era.md
|   |   ├── Condition_Occurrence_Combinations.md
|   |   ├── Person.md
|   |   ├── Condition_Occurence.md
|   |   ├── Observation.md
|   |   ├── Drug.md
|   |   ├── Procedure.md
|   |   ├── Drug_Exposure.md
|   |   ├── Drug_Cost.md
|   |   ├── Care_Site.md
|   |   ├── Obervation_Period.md
├── Documentation_Resources
|   ├── python_style_guide.md
|   ├── markdown_generator.py
|   ├── README_Template.md
|   ├── Contributing_guidelines.md
├── ResourceManagement
|   ├── parallelization_helper.py
|   ├── memory_optimization.py
|   ├── parallel_processing.py
|   ├── calculate_optimal_batch_size.py
├── PreProcessing
|   ├── resample_meds.py
|   ├── standardization_functions.py
|   ├── compute_stats.py
|   ├── time_intervals.py
|   ├── aggregation_functions.py
|   ├── data_format_and_manipulation.py
|   ├── clean_labs.py
|   ├── get_most_recent_values.py
|   ├── model_transformation.py
├── Resource_Files
|   ├── lab_unit_standard_map_do_not_edit_with_excel.csv
|   ├── conf_example.yaml
|   ├── lab_lookup.xlsx
├── ProjectManagement
|   ├── setup_project.py
|   ├── cohort_splitting.py
|   ├── Populate_OMOP_Cohort.py
|   ├── completion_monitoring.py
├── FileHandling
|   ├── io.py
|   ├── h5_helper.py
├── Logging
|   ├── parse_log.py
|   ├── send_email.py
|   ├── log_messages.py
```

## Overview
- [ProjectManagement/setup_project.py](https://github.com/Prisma-pResearch/Utilities/blob/main/ProjectManagement/setup_project.py)
  * setup_OMOP_project_and_download_data (setup the folder structure of a project. Run the eligibiltity criteria SQL query and populate the result into the Results.COHORT and Results.COHORT_Definition tables after splitting into train/test/validation by the method of your chosing. SQL queries for auditing the data elements and for downloading the data are generated based on your variable specification.xlsx document. The data is then downloaded in the most query efficient method possible based on the data requested. The specification document also contains the instructions necessary to geenrate summary tables at the end of the project.)
  * setup_project_and_download_data (setup the folder structure for a project. Run eligibility criteria SQL query on database or file system, and download data matching that query. This is probably one of the most useful functions in the Module.)

- [PreProcessing/standardization_functions.py](https://github.com/Prisma-pResearch/Utilities/blob/main/PreProcessing/standardization_functions.py)
	* process_df_v2 (function to standardize pandas dataframes in a reproducable manner. This is probably one of the most useful functions in the Module.)

- [ResourceManagement/parallelization_helper.py](https://github.com/Prisma-pResearch/Utilities/blob/main/ResourceManagement/parallelization_helper.py)
	* run_function_in_parallel_v2 (Execute any python function in parallel. Very Useful)

  - [PreProcessing/parallelization_helper.py](https://github.com/Prisma-pResearch/Utilities/blob/main/PreProcessing/Standardized_data.py)
	* Standardized_data (class for managing raw and processessed data. Very Useful)
  * build_dataset (function to build .h5 files for AI/ML workflows. This integrates with the DataSet class from the ML Toolbox Repo.)

*[FileHandling.io.py](https://github.com/Prisma-pResearch/Utilities/blob/main/FileHandling/io.py)*
* check_load_df (Load data from file (.csv, .txt, .sql, .xlsx, .h5, .parquet, .pickle, .feather, .sas7bdat, .log, .json, .yaml) or SQL database with options to ensure datatypes and datastructure. *Most useful function*)
* save_data (save data to file or SQL database. *Most useful function*)
* find_files (find files matching pattern(s) in a directory)
* get_batches_from_directory (get list of batches from a directory)
* get_column_names (get all of the column names for files in a directory matching a specified pattern)
* detect_file_names (detect file names in a directory)

*[FileHandling.variable_specification_utiliites.py](https://github.com/Prisma-pResearch/Utilities/blob/main/FileHandling/variable_specification_utiliites.py)*
* load_variables_from_var_spec (load variables that were downloaded based on the SQL queries autogenerated by the setup_OMOP_project_and_download_data function)

*[Reporting.make_tables_from_var_spec.py](https://github.com/Prisma-pResearch/Utilities/blob/main/Reporting/make_tables_from_var_spec.py)*
* make_tables_from_var_spec (load variables that were downloaded based on the SQL queries autogenerated by the setup_OMOP_project_and_download_data function or by your subsequent data generation codes into tables for publication *very helpful*)

*[OMOP.lookup_tables](https://github.com/Prisma-pResearch/Utilities/blob/main/OMOP/lookup_tables)*
* create_drug_dose_lookup_table (database table for mapping ingredient level dosages from meds in OMOP Database. You may submit pull requests or github issues to extend the med list)
* create_lookup_table (database table for mapping variables to their many equivalents/descendents. It also a quick way to find specific data in the database)

*[OMOP.omop_tables](https://github.com/Prisma-pResearch/Utilities/blob/main/OMOP/omop_tables)*
* contains Queries for each OMOP table that can be helpful in finding and understanding data and relationships in the CDM.

*Reporting/statistical_comparisons.py*
* summarize_groups (Summarize groups and differences accros/between groups. This will do most of the work for you in formatting manuscript tables for cohort comparisons. This is probably one of the most useful functions in the Module.)
* chi2_crosstab (Perfrom chi-squared crosstab analyses and generate a dataframe containing results.)
* fisher_crosstab (Perfrom Fishers Exact test crosstab analyses and generate a dataframe containing results.)
* kruskal_wallace (Perfrom kruskal_wallace crosstab analyses and generate a dataframe containing results.)
* test_normality (Perform the Shapiro Wilk test on a list of continuous variables in a pandas dataframe)

*Reporting/calculate_AUROC_CI.py*
* calculate_AUROC_confidence_intervals (Calcuate AUC/AUPRC confidence intervals for binary outcomes using bootstrapping. This is probably one of the most useful functions in the Module.)


*PreProcessing/get_most_recent_values.py*
* get_most_recent_or_earliest_values (Get the most recent or earliest observation of each specified level.)


*PreProcessing/data_format_and_manipulation.py*
* stack_df (Convert a wide dataframe to a long dataframe in a safe manner by aggregating multiple values that share the same index_keys.)
* check_format_series (check if a series object is of the specified type, if not, convert it to that type in a coersive manner.)
* get_file_name_components (Extract directory, filename, filetype, etc. from a filepath)
* apply_date_censor (Filter a dataframe for all observations to be before a given date)
* keep_top_n (keep the top n values from a series)
* force_datetime (coerce a column to datetime)
* sanatize_columns (format column names to not interfere with sql databases and standadize capitalization)
* prepare_table_for_upload (prepare dataframe for upload to a SQL dataframe by pre-formatting the data to the desired data type)
* move_cols_to_front_back_sort (Organize dataframe columns by placing certain columns at the start, others at the end, and optionally sorting the middle columns alphabetically.)
* create_dict (create a dictionary based on specification. This is very usefull for specifying aggregation functions on a dataframe)
* deduplicate_and_join (de-duplicate and join values within a group)
* get_column_type (Determine the datatype of a pandas dataframe or series)

*PreProcessing.aggregation_functions.py*
* A collection of useful aggregation functions for numeric and text based variables such as CAM scores, mobility, AKI stages, etc.

*PreProcessing.clean_labs.py*
* clean_labs (Clean lab variables by standardizing units, extracting concepts, and numerical values. Note this has already been performed on cleaned data/data in the OMOP database.)

*ResourceManagement/memory_optimization.py*
* calculate_available_memory (calculate available device memory)
* calculate_largest_file (Find the size of the largest file in a directory)
* calculate_file_size (calculate the cumulative file size of all files matching a specified pattern in a directory)
* calculate_optimal_workers (calculate optimal number of works for IDR data PIpeline)

*PreProcessing/time_intervals.py*
* condense_overlapping_segments (resolve overlapping time intervals in a pandas dataframe)
* resample_and_condense (floor start times to the bottom of the hour and celing end times to the top of the hour, then apply condense overlapping segements function)

*ProjectManagement/Populate_OMOP_Cohort.py*
* populate_omop_cohort (populate OMOP cohort and cohort definition table. This module is used in the setup_cohort and download data module already and does not need to be called seperately.)


*ProjectManagement/completion_monitoring.py*
* check_complete (check if all of the expected files were processed or not. This is a very useful function for any work done in parallel to ensure everything completed correctly.)

*ProjectManagement/cohort_splitting.py*
* split_development_validation (Split a cohort into train, test, validation through time or randomly.)


*PreProcessing/compute_stats.py*
* compute_lab_ratio (score a lab result based on one or more thresholds. This is very useful in things like SOFA score calculations and other threshold based scoring systems.)
* median_deviation (calculate the median deviation)
* outlier_detection_and_imputation (compute statistics on a pandas series)


*Logging.send_email.py*
* send_email (send emails using smtp. This is handy for notifications on long running jobs.)

*Logging.parse_log.py*
* parse_log (parse log into a pandas dataframe)

*Logging.log_messages.py*
* start_logging_to_file (setup a log file and ensure that it is the only log reciever.)
* log_print_email_message (Log messages to file, console, and/or email)

*General.func_utils.py*
* debug_inputs (tool for debugging functions by preserving inputs provided to them.)
* convert_func_to_string (convert a function to a string for later import)
* get_func (convert a string to a funtion call)

*Encryption.file_encryption.py*
* encrypt_dict (encrypt or decrypt a dictionary)
* load_encrypted_file (decrypt an entire dictionary and return)
* CryptoYAML (class for encrypting and managing encrypted information)


*Database.database_updates.py*
* log_database_update (Utility for logging modifications to a database inside the database.)

*Database.connect_to_database.py*
* get_SQL_database_connection_v2 (get SQLAlchemy connection engine connection to a database from an encrypted dictionary. This is useful for ensuring credentials are not inadvertantly stored insecurely in code.)
* execute_query_in_transaction (execute a query in a transaction.)
* insert_dataframe_into_cassandra (tool to upload data to cassandra)
* fetch_data_from_cassandra (tool to fetch data from a cassandra database)

*FileHandling.h5_helper.py*
* write_h5 (tool for writing h5 files in a standardized manner while preserving pandas metadata. This is used by the save_data_function)
* read_h5_dataset (read dataset from h5 file to a pandas dataframe or numpy array)

## Bugs and feature requests

Have a bug or a feature request? Please first read the [issue guidelines](https://github.com/Prisma-pResearch/Utilities/blob/master/CONTRIBUTING.md) and search for existing and closed issues. If your problem or idea is not addressed yet, [please open a new issue](https://github.com/Prisma-pResearch/Utilities/issues/new).

## Contributing

Please read through our [contributing guidelines](https://github.com/Prisma-pResearch/Utilities/blob/master/CONTRIBUTING.md). Included are directions for opening issues, coding standards, and notes on development.

Moreover, all HTML and CSS should conform to the [Code Guide](https://github.com/mdo/code-guide), maintained by [Main author](https://github.com/ruppert20).

Editor preferences are available in the [editor config](https://github.com/Prisma-pResearch/Utilities/blob/master/.editorconfig) for easy use in common text editors. Read more and download plugins at <https://editorconfig.org/>.

## Module requirements
* Please read [requirements.txt](https://github.com/Prisma-pResearch/Utilities/blob/master/requirements.txt) for required python packages if installing via pip
* Alternatively, the enviroment can be configured via create a conda enviroment according the the [environment.yaml](https://github.com/Prisma-pResearch/Utilities/blob/master/enviroment.yaml) file.

## Creators

**Main Author**
- [Matthew Ruppert](https://github.com/ruppert20) <br />

**Maintainer** <br />
- [Matthew Ruppert](https://github.com/ruppert20)

## Thanks

Some Text

## Copyright and license

Code and documentation copyright 2011-2022 IC3. Code released under the [MIT License](https://github.com/Prisma-pResearch/Utilities/blob/master/LICENSE).

