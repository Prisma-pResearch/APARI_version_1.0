# -*- coding: utf-8 -*-
"""
Created on Wed Jun 21 15:09:57 2023

@author: ruppert20
"""
import argparse
import os
from dotenv import load_dotenv
from Python.run_apari_v2 import run_APARI


def parse_args():
    """
    Automatcially configure apari project.

    Returns
    -------
    TYPE
        DESCRIPTION.

    """
    desc = "Configure/RUN APARI PROJECT"
    parser = argparse.ArgumentParser(description=desc)

    parser.add_argument('-r', '--root_project_dir', type=str,
                        help='The folder path where the project data will be stored', required=False)

    parser.add_argument('--project_name', type=str, default='APARI',
                        help='The Name of the project.')

    parser.add_argument('--database_host_name', type=str, default='', help='The Host address of the server where the OMOP data is located')

    parser.add_argument('-d', '--database', type=str, default='',
                        help='The name of the database where the OMOP Data is located.')

    parser.add_argument('-u', '--database_username', type=str, default='',
                        help='The username for the database')

    parser.add_argument('-p', '--database_password', type=str, default='',
                        help='The passowrd for the username to access the database')

    parser.add_argument('-m', '--mode', type=str,
                        default='both',
                        help='Whether the project is in audit mode (only runs audit to check compatability) or data_retrieval (download data and train model) or both',
                        choices=['audit', 'both', 'data_retrieval', 'run_pretrained'])

    parser.add_argument('-v', '--vocab_schema', type=str, default='VOCAB',
                        help='The schema or the database_schema combination to where the OMOP vocabulary tables downloaded from ATHENA are stored. Example: db_name.VOCAB or VOCAB')

    parser.add_argument('-c', '--data_schema', type=str, default='CDM',
                        help='The schema or the database_schema combination to where the patient data in OMOP format are stored. Example: db_name.CDM or CDM')

    parser.add_argument('-o', '--operational_schema', type=str, default='dbo',
                        help='The schema or the database_schema combination to where the temporary tables may be written. **NOTE***: The user must have write permission for this schema. Example: db_name.dbo or dbo')

    parser.add_argument('-l', '--lookup_schema', type=str, default='dbo',
                        help='The schema or the database_schema combination to where the drug and variable lookup tables are located. Example: db_name.dbo or dbo')

    parser.add_argument('--results_schema', type=str, required=False,
                        help='The database_schema combination to where the COHORT and COHORT DEFINITION tables are located. Example: db_name.RESULTS  **NOTE** The database and the schema must both be present')

    parser.add_argument('-z', '--default_facility_zip', type=str, required=False,
                        help='5 digit zip code for the main facilty')

    parser.add_argument('-g', '--n_gpus', type=int, default=0,
                        help='The number of GPUS allowed for training and testing the model')

    parser.add_argument('--dset_name', type=str, default='APARI_dataset_v1.0.h5',
                        help='The name of the resultant .h5 file')

    parser.add_argument('--lookup_table', type=str, default='IC3_Variable_Lookup_Table',
                        help='The name of the variable lookup table to use. The default is IC3_Variable_Lookup_Table. **NOTE** This table must exists in the lookup schema. Do not include the schema name here')

    parser.add_argument('--drug_lookup_table', type=str, default='IC3_DRUG_LOOKUP_TABLE',
                        help='The name of the drug lookup table to use. The default is IC3_DRUG_LOOKUP_TABLE. **NOTE** This table must exists in the lookup schema. Do not include the schema name here')

    parser.add_argument('-w', '--max_workers', type=int, default=4,
                        help='The number of cpu cores to use in parallel variable generation and data_loading for the GPU')

    parser.add_argument('-s', '--serial_variable_generation', type=bool, default=False,
                        help='Whether variables should be generated in Series rather than parallel')

    parser.add_argument('--display_logs', type=bool, default=False,
                        help='Whether enhanced logs should be displayed to the console/terminal window')

    parser.add_argument('-e', '--load_from_env', type=bool, default=False,
                        help='Load parameters from ENVIROMENT')

    parser.add_argument('-n', '--n_samples', type=int, default=None,
                        help='Select up to n surgeries to experiment with')
    
    parser.add_argument('--cdm_version', type=str, default='5.4',
                        help='OMOP CDM version your database parameter is referencing',
                        choices=['5.4', '5.3'])
    
    parser.add_argument('--subject_id_mode', type=str, default='procedure_occurrence',
                        help='Whether the index for surgeries is based on a visit detail instance or a procedure occurrence',
                        choices=['procedure_occurrence', 'visit_detail'])

    args = parser.parse_args()

    if args.load_from_env:
        load_dotenv("/backend/APARI_AIM2_DEE/.env.bak")
        for name, value in os.environ.items():
            if 'XXXAPARIXXX' in name:
                setattr(args, name.replace('XXXAPARIXXX', '').lower(), value)

    assert '.' in args.results_schema, 'You must specify both the database and the schema for the results_schema parameter'
    assert len(str(args.default_facility_zip)) == 5
    assert args.dset_name[-3:] == '.h5'
    assert '.' not in args.lookup_table
    assert '.' not in args.drug_lookup_table
    assert isinstance(args.root_project_dir, str)
    assert os.path.exists(args.root_project_dir)
    assert os.path.isdir(args.root_project_dir)

    assert args.cdm_version in ['5.4', '5.3'], f'Unsupported CDM Version: {args.cdm_version}. Please use an OMOP CDM 5.4 (preffered) or 5.3 database'

    run_APARI(root_project_dir=args.root_project_dir,
              project_name=args.project_name,
              database_host_name=args.database_host_name,
              database=args.database,
              database_username=args.database_username,
              database_password=args.database_password,
              vocab_schema=args.vocab_schema,
              data_schema=args.data_schema,
              operational_schema=args.operational_schema,
              lookup_schema=args.lookup_schema,
              results_schema=args.results_schema,
              default_facility_zip=args.default_facility_zip,
              n_gpus=args.n_gpus,
              cdm_version=args.cdm_version,
              dset_name=args.dset_name,
              mode=args.mode,
              subject_id_mode=args.subject_id_mode,
              lookup_table=args.lookup_table,
              drug_lookup_table=args.drug_lookup_table,
              max_workers=int(float(args.max_workers)),
              n_samples=args.n_samples,
              display_logs=args.display_logs,
              serial_variable_generation=args.serial_variable_generation,
              engine_override=isinstance(os.environ.get('CONFIG_KEY', False), str))


if __name__ == '__main__':
    parse_args()
