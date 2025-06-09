# -*- coding: utf-8 -*-
"""
Created on Tue Sep 15 17:02:33 2020.

@author: ruppert20
"""
import pandas as pd
import os
from functools import reduce
from typing import Dict, List, Union
from ..Utilities.FileHandling.io import check_load_df, save_data
from .icu_duration_v2 import prepare_stations_for_icu_outcomes
from .condition_outcome_generation import condition_outcome_generation
from .mortality_v2 import generate_mortality_outcomes_v2
from .mech_vent_v2 import prepare_resp_for_mv_outcomes
from .aki_outcome_v2 import aki_outcome
from .cam_icu_v2 import cam_icu_outcome
from .duration_outcome_generation import generate_duration_outcomes
from ..Utilities.Logging.log_messages import log_print_email_message as logm
from ..Utilities.FileHandling.variable_specification_utilities import load_variables_from_var_spec
from ..Utilities.General.func_utils import debug_inputs


def generate_outcomes_v3(dir_dict: Dict[str, str],
                         var_file_linkage_fp: str,
                         source_dir_key: str,
                         aki_phenotype_dir_key: str,
                         dest_dir_key: str,
                         visit_detail_type: str,
                         batch_id: str = None,
                         eid: str = 'visit_occurrence_id',
                         pid: str = 'person_id',
                         aki_race_correction: bool = False,
                         aki_phenotyping_version: int = 2,
                         outcome_success_file_path: str = None,
                         regex=True,
                         time_intervals: Dict[str, Dict[str, Union[List[str], str]]] = {'death': {'visit_detail_end': ['1D', '3D', '7D']},
                                                                                        'mv': ['2D', '3D', '7D', '30D', 'disch'],
                                                                                        'icu': ['2D', '3D', '7D', '30D', 'disch'],
                                                                                        'aki': ['3D', '7D'],
                                                                                        'cam': ['1D', '3D', '7D', 'disch']},
                         condition_outcomes_dict: Dict[str, List[str]] = {'cv': ['cv_cardiac_arrest', 'cv_hypo_no_shock', 'cv_hf', 'cv_shock'],
                                                                          'delirium_icd': ['delirium_icd'],
                                                                          'mech_wound': ['mech_wound'],
                                                                          'neuro': ['neuro_other', 'neuro_plegia_paralytic', 'neuro_stroke'],
                                                                          'surg_infection': ['surg_infection'],
                                                                          'proc': ['proc_graft_implant_foreign_body', 'proc_hemorrhage_hematoma_seroma', 'proc_non_hemorrhagic_technical'],
                                                                          'sepsis': ['sepsis'],
                                                                          'vte': ['vte_pe', 'vte_deep_super_vein']},
                         **logging_kwargs):
    """
    Generate outcomes final.

    Actions:
        1. Generate base dataframe which has one row for each completed OR Case and at least one row per inpatient/observation hospital encounter
        2. generate mortality outcomes from hospital and social security death index files
        3. Generate ICU LOS outcomes from internal stations file
        4. Generate ICD outcomes from diagnoses file
        5. Gernate MV outcomes from respiratory file
        6. Generate AKI and CKD outcomes from final_aki, trajectory_aki, and ckd files
        7. Genrate delirum CAM outcomes from cam file
        8. Combine aforementioned outcomes into one dataframe
        9. Adjust/add additional outcomes which are the union of the aforementioned outcomes produced
        10. Save result to file

    Parameters
    ----------
    encounter_name : str
        DESCRIPTION.
    or_case_name : str
        DESCRIPTION.
    internal_stations_name : str
        DESCRIPTION.
    ssdi_name : str
        DESCRIPTION.
    diagnoses_name : str
        DESCRIPTION.
    outcomes_map_fp : str
        DESCRIPTION.
    resp_df_name : str
        DESCRIPTION.
    aki_final_fp : str
        DESCRIPTION.
    aki_trajectory_fp : str
        DESCRIPTION.
    ckd_summary_fp : str
        DESCRIPTION.
    cam_name : str
        DESCRIPTION.
    eid : str
        DESCRIPTION.
    pid : str
        DESCRIPTION.
    outcome_file_name : str, optional
        DESCRIPTION. The default is 'original_ids_outcome_final.csv'.
    postop_intervals : list, optional
        DESCRIPTION. The default is ['1D', '3D', '7D'].
    time_intervals : list, optional
        DESCRIPTION. The default is ['surg_disch', 'admit_disch'].

    Returns
    -------
    log: str
        Log of status messages to be saved for debugging purposes.

    """
    # debug_inputs(function=generate_outcomes_v3, kwargs=locals(), dump_fp=f'outcome_generation_{batch_id}.p')
    # raise Exception('stop here')

    outcome_file_name: str = f'{visit_detail_type}_omop_outcome_final{("_" + batch_id) if isinstance(batch_id, str) else ""}'

    if not isinstance(outcome_success_file_path, str):
        outcome_success_file_path: str = os.path.join(dir_dict.get('status_files'), f'{outcome_file_name}_success_')

    if os.path.exists(outcome_success_file_path):
        return f'{outcome_file_name} alredy completed'

    logm(message='Creating base dataframe', **logging_kwargs)

    patterns: List[str] = [r'_{}\.csv'.format(batch_id), r'_{}_[0-9]+\.csv'.format(batch_id), r'_[0-9]+_[0-9]+\.csv', r'_[0-9]+\.csv', r'\.csv']

    # load encounter df
    encounter_df: pd.DataFrame = load_variables_from_var_spec(variables_columns=['visit_start_datetime', 'visit_end_datetime', 'discharged_to_concept_id', 'visit_concept_id', 'provider_id'],
                                                              var_spec_key=var_file_linkage_fp,
                                                              dir_dict=dir_dict,
                                                              data_source_key=source_dir_key,
                                                              cdm_tables=['visit_occurrence'],
                                                              tag_source=True,
                                                              project='Outcome_Generation',
                                                              mute_duplicate_var_warnings=True,
                                                              desired_types={**{x: 'datetime' for x in ['visit_start_datetime', 'visit_end_datetime']},
                                                                             **{'discharged_to_concept_id': 'sparse_int'}},
                                                              patterns=patterns,
                                                              allow_empty_files=True, regex=True, dtype=None, ds_type='pandas', **logging_kwargs).drop_duplicates()\
        .rename(columns={'provider_id': 'visit_provider_id'}).query('~source_file.str.contains(r"_force_")', engine='python').drop(columns=['source_file'])

    if encounter_df.shape[0] == 0:
        out: pd.DataFrame = encounter_df

    else:
        # load visit detail_df
        visit_detail_df: pd.DataFrame = load_variables_from_var_spec(variables_columns=['ward', 'icu', 'surgery', 'operating_room', 'visit_detail_start_datetime', 'visit_detail_end_datetime', 'variable_name', 'provider_id', 'visit_detail_id'],
                                                                     dir_dict=dir_dict,
                                                                     var_spec_key=var_file_linkage_fp,
                                                                     data_source_key=source_dir_key,
                                                                     project='Outcome_Generation',
                                                                     filter_variables=False,
                                                                     cdm_tables=['visit_detail'],
                                                                     mute_duplicate_var_warnings=True,
                                                                     desired_types={x: 'datetime' for x in ['visit_detail_start_datetime', 'visit_detail_end_datetime']},
                                                                     patterns=patterns,
                                                                     allow_empty_files=True,
                                                                     regex=True, dtype=None,
                                                                     tag_source=True,
                                                                     ds_type='pandas',
                                                                     **logging_kwargs).drop_duplicates()\
            .dropna(subset=[pid, eid, 'visit_detail_start_datetime', 'visit_detail_end_datetime', 'visit_detail_id'], how='any')\
            .rename(columns={'provider_id': f'{visit_detail_type}_provider_id'})\
            .drop_duplicates(subset=['visit_detail_id'])\
            .sort_values([pid, 'visit_detail_end_datetime'], ascending=True)\
            .query('~source_file.str.contains(r"_force_")', engine='python').drop(columns=['source_file'])\
            .query("variable_name.isin(['ward', 'icu', 'operating_room', 'surgery'])", engine='python')

        if (str(visit_detail_type) == 'surgery'):
            if not visit_detail_df.variable_name.isin(['surgery']).any():
                visit_detail_df.variable_name = visit_detail_df.variable_name.replace({'operating_room': 'surgery'})
            else:
                visit_detail_df: pd.DataFrame = visit_detail_df[~visit_detail_df.variable_name.isin(['operating_room'])].copy(deep=True)

        if str(visit_detail_type) in ['surgery', 'icu']:
            vds: pd.DataFrame = visit_detail_df.copy(deep=True).query(f'variable_name == "{visit_detail_type}"')\
                .rename(columns={'visit_detail_start_datetime': f'{visit_detail_type}_start_datetime',
                                 'visit_detail_end_datetime': f'{visit_detail_type}_end_datetime'})
            # add vd order for each encounter
            vds[f'{visit_detail_type}_order'] = vds.groupby(eid, group_keys=False)['visit_detail_id'].apply(lambda key: (key != key.shift()).astype(int).cumsum())

        # create base df
        base_df = encounter_df[['subject_id', pid, eid, 'visit_concept_id', 'visit_provider_id', 'visit_start_datetime', 'visit_end_datetime', 'discharged_to_concept_id']]\
            .dropna(subset=[pid, eid, 'visit_start_datetime', 'visit_end_datetime'])\
            .drop_duplicates(subset=[eid])

        if str(visit_detail_type) in ['surgery', 'icu']:
            # append visit_detail info to base df
            base_df = base_df.merge(vds.drop(columns=[pid, eid]),
                                    on='subject_id',
                                    how='left')

        # cols to strip
        cols_to_strip: list = [x for x in base_df.columns if x not in ['visit_detail_id', eid, 'subject_id']] + ['clean_death_date']

        '''
        Calculate mortality outcomes
        '''

        logm(message='calculating Mortality outcomes', **logging_kwargs)

        death_outcomes = generate_mortality_outcomes_v2(source_df=base_df.copy(),
                                                        ssdi_df=load_variables_from_var_spec(variables_columns=['death_date', 'death_type_concept_id'],
                                                                                             dir_dict=dir_dict,
                                                                                             var_spec_key=var_file_linkage_fp,
                                                                                             data_source_key=source_dir_key,
                                                                                             project='Outcome_Generation',
                                                                                             mute_duplicate_var_warnings=True,
                                                                                             desired_types={x: 'datetime' for x in ['death_date']},
                                                                                             patterns=patterns,
                                                                                             allow_empty_files=True,
                                                                                             regex=True, dtype=None,
                                                                                             ds_type='pandas',
                                                                                             **logging_kwargs).drop_duplicates(),
                                                        hsp_df=encounter_df[encounter_df.columns.intersection([pid, 'death_date'])].copy(),
                                                        eid=eid,
                                                        pid=pid,
                                                        visit_occurrence_start='visit_start_datetime',
                                                        visit_occurrence_end='visit_end_datetime',
                                                        visit_detail_end=f'{visit_detail_type}_end_datetime',
                                                        visit_detail_start=f'{visit_detail_type}_start_datetime',
                                                        visit_detail_type=visit_detail_type,
                                                        time_intervals=time_intervals.get('death', {}),
                                                        **logging_kwargs)

        # integrate clean death date into base df
        base_df = check_load_df(base_df.merge(death_outcomes[['clean_death_date', eid, 'visit_detail_id']],
                                how='left',
                                on=[eid, 'visit_detail_id']),
                                desired_types={'clean_death_date': 'datetime'})

        '''
        calculate  ICU outomes
        '''

        logm(message='calculating ICU outcomes', **logging_kwargs)

        icu_outcomes = generate_duration_outcomes(source_df=base_df.copy(),
                                                  df=visit_detail_df.query('variable_name == "icu"'),
                                                  pid=pid,
                                                  eid=eid,
                                                  unique_index_col='visit_detail_id',
                                                  visit_start_col='visit_start_datetime',
                                                  visit_end_col='visit_end_datetime',
                                                  visit_detail_type=visit_detail_type,
                                                  visit_detail_start_col=f'{visit_detail_type}_start_datetime',
                                                  visit_detail_end_col=f'{visit_detail_type}_end_datetime',
                                                  prep_func=prepare_stations_for_icu_outcomes,
                                                  time_intervals=['2D', '3D', '7D', '30D', 'disch'],
                                                  label='icu',
                                                  mortality_inclusive_durations=[f'{visit_detail_type}_30d', 'adm_30d'],
                                                  **logging_kwargs)

        '''
        calculate Condition outcomes
        '''

        logm(message='calculating ICD outcomes', **logging_kwargs)

        condition_outcomes = condition_outcome_generation(base_df=base_df,
                                                          condition_df=load_variables_from_var_spec(variables_columns=['condition_start_date', 'poa', 'variable_name', 'condition_concept_id'],
                                                                                                    dir_dict=dir_dict,
                                                                                                    var_spec_key=var_file_linkage_fp,
                                                                                                    data_source_key=source_dir_key,
                                                                                                    project='Outcome_Generation',
                                                                                                    mute_duplicate_var_warnings=True,
                                                                                                    desired_types={'condition_start_date': 'datetime', 'poa': 'sparse_int'},
                                                                                                    patterns=patterns,
                                                                                                    allow_empty_files=True,
                                                                                                    regex=True, dtype=None,
                                                                                                    ds_type='pandas',
                                                                                                    **logging_kwargs).drop_duplicates().drop(columns=['visit_occurrence_id', 'visit_detail_id'], errors='ignore'),
                                                          outcomes=condition_outcomes_dict,
                                                          visit_detail_type=visit_detail_type)

        '''
        Calculate MV outcomes
        '''

        logm(message='calculating MV outcomes', **logging_kwargs)

        mv_outcomes = generate_duration_outcomes(source_df=base_df.copy(),
                                                 df=load_variables_from_var_spec(variables_columns=['mechanical_ventilation', 'variable_name', 'device_exposure_start_datetime', 'device_exposure_end_datetime'],
                                                                                 dir_dict=dir_dict,
                                                                                 var_spec_key=var_file_linkage_fp,
                                                                                 data_source_key=source_dir_key,
                                                                                 project='Outcome_Generation',
                                                                                 mute_duplicate_var_warnings=True,
                                                                                 desired_types={'device_exposure_start_datetime': 'datetime', 'device_exposure_end_datetime': 'datetime'},
                                                                                 patterns=patterns,
                                                                                 allow_empty_files=True,
                                                                                 regex=True, dtype=None,
                                                                                 ds_type='pandas',
                                                                                 **logging_kwargs)
                                                 .query('variable_name == "mechanical_ventilation"')
                                                 .drop(columns=['visit_detail_id'], errors='ignore'),
                                                 pid=pid,
                                                 eid=eid,
                                                 unique_index_col='visit_detail_id',
                                                 visit_start_col='visit_start_datetime',
                                                 visit_end_col='visit_end_datetime',
                                                 visit_detail_type=visit_detail_type,
                                                 visit_detail_start_col=f'{visit_detail_type}_start_datetime',
                                                 visit_detail_end_col=f'{visit_detail_type}_end_datetime',
                                                 prep_func=prepare_resp_for_mv_outcomes,
                                                 time_intervals=time_intervals.get('mv', []),
                                                 label='mv',
                                                 **logging_kwargs)

        '''
        Calcuate AKI CKD outcomes
        '''

        logm(message='calculating AKI/CKD outcomes', **logging_kwargs)
        if isinstance(aki_race_correction, bool):
            aki_outcomes = aki_outcome(source_df=base_df.copy(),
                                       pid=pid,
                                       eid=eid,
                                       batch_id=batch_id,
                                       dir_dict=dir_dict,
                                       project_name='Outcome_Generation',
                                       directory=dir_dict.get(aki_phenotype_dir_key, aki_phenotype_dir_key),
                                       patterns=patterns,
                                       aki_final_fp=f'visit_occurrence_id_with{"" if aki_race_correction else "out"}_race_correction_v{aki_phenotyping_version}_final_aki',
                                       aki_trajectory_fp=f'visit_occurrence_id_with{"" if aki_race_correction else "out"}_race_correction_v{aki_phenotyping_version}_aki_trajectory',
                                       aki_summary_fp=f'visit_occurrence_id_with{"" if aki_race_correction else "out"}_race_correction_v{aki_phenotyping_version}_aki_summary',
                                       visit_start_col='visit_start_datetime',
                                       visit_detail_type=visit_detail_type,
                                       visit_detail_end_col=f'{visit_detail_type}_end_datetime',
                                       time_intervals=time_intervals.get('aki', []),
                                       **logging_kwargs)
        elif isinstance(aki_race_correction, list):
            aki_outcomes = reduce(lambda i, j: pd.merge(i[[x for x in i.columns if x not in cols_to_strip]],
                                                        j[[x for x in j.columns if x not in cols_to_strip]],
                                                        on=[eid, 'visit_detail_id'],
                                                        how='left'),
                                  [aki_outcome(source_df=base_df.copy(),
                                               eid=eid,
                                               pid=pid,
                                               batch_id=batch_id,
                                               dir_dict=dir_dict,
                                               project_name='Outcome_Generation',
                                               directory=dir_dict.get(aki_phenotype_dir_key, aki_phenotype_dir_key),
                                               aki_final_fp=f'visit_occurrence_id_with{"" if rc else "out"}_race_correction_v{aki_phenotyping_version}_final_aki',
                                               aki_trajectory_fp=f'visit_occurrence_id_with{"" if rc else "out"}_race_correction_v{aki_phenotyping_version}_aki_trajectory',
                                               aki_summary_fp=f'visit_occurrence_id_with{"" if rc else "out"}_race_correction_v{aki_phenotyping_version}_aki_summary',
                                               visit_start_col='visit_start_datetime',
                                               visit_detail_type=visit_detail_type,
                                               visit_detail_end_col=f'{visit_detail_type}_end_datetime',
                                               time_intervals=time_intervals.get('aki', []),
                                               **logging_kwargs)
                                   for rc in aki_race_correction])

        if isinstance(aki_race_correction, bool):
            ckd_summary = check_load_df(input_v=f'visit_occurrence_id_with{"" if aki_race_correction else "out"}_race_correction_v{aki_phenotyping_version}_ckd_summary',
                                        patterns=patterns,
                                        directory=dir_dict.get(aki_phenotype_dir_key, aki_phenotype_dir_key),
                                        allow_empty_files=True,
                                        usecols=[eid, 'final_class'],
                                        ds_type='pandas', **logging_kwargs)
        elif isinstance(aki_race_correction, list):
            ckd_summary = reduce(lambda x, y: pd.merge(x, y, how='outer', on=eid),
                                 [check_load_df(input_v=f'visit_occurrence_id_with{"" if rc else "out"}_race_correction_v{aki_phenotyping_version}_ckd_summary',
                                                allow_empty_files=True,
                                                usecols=[eid, 'final_class'], ds_type='pandas', **logging_kwargs)
                                  .rename(columns={'final_class': f'final_class_{"with_race_correction" if rc else "without_race_correction"}'})
                                  .set_index(eid) for rc in aki_race_correction])
        else:
            logm(message='Unrecognized ckd summary file path format', error=True, raise_exception=True, **logging_kwargs)

        ckd_outcomes = check_load_df(base_df, desired_types={'visit_occurrence_id': 'sparse_int'}).copy().merge(ckd_summary, on=eid, how='left')

        del ckd_summary

        '''
        Calculate Delirium CAM outcomes
        '''

        logm(message='calculating CAM outcomes', **logging_kwargs)

        cam_icu_outcomes = cam_icu_outcome(cam_df=load_variables_from_var_spec(variables_columns=['cam', 'variable_name', 'observation_datetime', 'measurement_datetime', 'value_as_concept_id'],
                                                                               dir_dict=dir_dict,
                                                                               var_spec_key=var_file_linkage_fp,
                                                                               data_source_key=source_dir_key,
                                                                               project='Outcome_Generation',
                                                                               mute_duplicate_var_warnings=True,
                                                                               desired_types={'observation_datetime': 'datetime', 'measurement_datetime': 'datetime'},
                                                                               patterns=patterns,
                                                                               allow_empty_files=True,
                                                                               regex=True, dtype=None,
                                                                               coalesce_fields={'measurement_datetime': ['observation_datetime', 'measurement_datetime']},
                                                                               ds_type='pandas',
                                                                               **logging_kwargs)
                                           .query('variable_name == "cam"'),
                                           base_df=base_df.copy(),
                                           unique_index_col='visit_detail_id',
                                           visit_start_col='visit_start_datetime',
                                           visit_end_col='visit_end_datetime',
                                           visit_detail_type=visit_detail_type,
                                           visit_detail_end_col=f'{visit_detail_type}_end_datetime',
                                           eid=eid,
                                           time_intervals=time_intervals.get('cam'),
                                           **logging_kwargs)

        orig_shape: int = base_df.shape[0]

        out = check_load_df(base_df, desired_types={x: 'sparse_int' for x in ['subject_id', 'visit_occurrence_id', 'visit_detail_id']})\
            .merge(check_load_df(icu_outcomes[[x for x in icu_outcomes.columns if x not in cols_to_strip]], desired_types={x: 'sparse_int' for x in ['subject_id', 'visit_occurrence_id', 'visit_detail_id']}), how='left', on=[eid, 'visit_detail_id', 'subject_id'])\
            .merge(check_load_df(condition_outcomes[[x for x in condition_outcomes.columns if x not in cols_to_strip]], desired_types={x: 'sparse_int' for x in ['subject_id', 'visit_occurrence_id', 'visit_detail_id']}), how='left', on=[eid, 'visit_detail_id', 'subject_id'])\
            .merge(check_load_df(mv_outcomes[[x for x in mv_outcomes.columns if x not in cols_to_strip]], desired_types={x: 'sparse_int' for x in ['subject_id', 'visit_occurrence_id', 'visit_detail_id']}), how='left', on=[eid, 'visit_detail_id', 'subject_id'])\
            .merge(check_load_df(aki_outcomes[[x for x in aki_outcomes.columns if x not in cols_to_strip]], desired_types={x: 'sparse_int' for x in ['subject_id', 'visit_occurrence_id', 'visit_detail_id']}), how='left', on=[eid, 'visit_detail_id', 'subject_id'])\
            .merge(check_load_df(ckd_outcomes[[x for x in ckd_outcomes.columns if x not in cols_to_strip]], desired_types={x: 'sparse_int' for x in ['subject_id', 'visit_occurrence_id', 'visit_detail_id']}), how='left', on=[eid, 'visit_detail_id', 'subject_id'])\
            .merge(check_load_df(death_outcomes[[x for x in death_outcomes.columns if x not in cols_to_strip]], desired_types={x: 'sparse_int' for x in ['subject_id', 'visit_occurrence_id', 'visit_detail_id']}), how='left', on=[eid, 'visit_detail_id', 'subject_id'])\
            .merge(check_load_df(cam_icu_outcomes[[x for x in cam_icu_outcomes.columns if x not in cols_to_strip]], desired_types={x: 'sparse_int' for x in ['subject_id', 'visit_occurrence_id', 'visit_detail_id']}), how='left', on=[eid, 'visit_detail_id', 'subject_id'])

        assert out.shape[0] == orig_shape, f'There was a merging error resulting in the unecessary duplications of {out.shape[0] - orig_shape} rows'

        # correct for ESRD in AKI final class is ESRD
        if 'final_class_with_race_correction' in out.columns:
            esrd_mask_1 = out.final_class_with_race_correction.astype(str).str.contains('ESRD', regex=False, case=False, na=False)

            if any(esrd_mask_1):
                for col in [x for x in out.columns if ('aki_' in x) and ('with_' in x)]:
                    out.loc[esrd_mask_1, col] = 'ESRD'
        else:
            out.rename(columns={'final_class': 'final_class_without_race_correction'}, inplace=True)

        if 'final_class_without_race_correction' in out.columns:
            esrd_mask_2 = out.final_class_without_race_correction.astype(str).str.contains('ESRD', regex=False, case=False, na=False)

            if any(esrd_mask_2):
                for col in [x for x in out.columns if ('aki_' in x) and ('without_' in x)]:
                    out.loc[esrd_mask_2, col] = 'ESRD'

        if pd.Series(condition_outcomes_dict.keys()).isin(['delirium_icd', 'mech_wound', 'neuro', 'proc']).sum() == 4:

            # create union outcomes
            for col in [x for x in out.columns if (('del' in x) and ('overall' in x))]:

                out.loc[:, col.replace('delirium_icd', 'delirium_icd_cam_comb')] = out.loc[:, [col, f'delirium_cam_{visit_detail_type}_disch' if visit_detail_type in col else 'delirium_cam_adm_disch']]\
                    .apply(lambda row: 1 if row.dropna().astype(bool).any() else None if row.isnull().all() else 0, axis=1)

                out.loc[:, col.replace('delirium_icd', 'neuro_delirium_cam_icd_comb')] = out.loc[:, [col, f'delirium_cam_{visit_detail_type}_disch' if visit_detail_type in col else 'delirium_cam_adm_disch',
                                                                                                     col.replace('delirium_icd', 'neuro')]]\
                    .apply(lambda row: 1 if row.dropna().astype(bool).any() else None if row.isnull().all() else 0, axis=1)

                out.loc[:, col.replace('delirium_icd', 'neuro_delirium_cam_comb')] = out.loc[:, [f'delirium_cam_{visit_detail_type}_disch' if visit_detail_type in col else 'delirium_cam_adm_disch',
                                                                                                 col.replace('delirium_icd', 'neuro')]]\
                    .apply(lambda row: 1 if row.dropna().astype(bool).any() else None if row.isnull().all() else 0, axis=1)

                if visit_detail_type in col:

                    out.loc[:, col.replace('delirium_icd', 'procedural_comp_infection_mech_wound_comb')] = out.loc[:, [col.replace('delirium_icd', 'mech_wound'), col.replace('delirium_icd', 'surg_infection'),
                                                                                                                       col.replace('delirium_icd', 'proc')]]\
                        .apply(lambda row: 1 if row.dropna().astype(bool).any() else None if row.isnull().all() else 0, axis=1)

                    out.loc[:, col.replace('delirium_icd', 'infection_mech_wound_comb')] = out.loc[:, [col.replace('delirium_icd', 'mech_wound'), col.replace('delirium_icd', 'surg_infection')]].apply(
                        lambda row: 1 if row.dropna().astype(bool).any() else None if row.isnull().all() else 0, axis=1)

    # save to file
    save_data(df=out,
              out_path=os.path.join(dir_dict.get(dest_dir_key, dest_dir_key), f'{outcome_file_name}.csv'),
              index=False,
              **logging_kwargs)

    # create status file
    open(outcome_success_file_path, 'a').close()


# if __name__ == "__main__":
#     import pandas as pd
#     import os
#     from functools import reduce
#     from typing import Dict, List, Union
#     import sys
#     sys.path.append(r'P:')
#     from GitHub.Utilities.FileHandling.io import check_load_df, save_data
#     from GitHub.Outcome_Generation.Python.outcome_generation.icu_duration_v2 import prepare_stations_for_icu_outcomes
#     from GitHub.Outcome_Generation.Python.outcome_generation.condition_outcome_generation import condition_outcome_generation
#     from GitHub.Outcome_Generation.Python.outcome_generation.mortality_v2 import generate_mortality_outcomes_v2
#     from GitHub.Outcome_Generation.Python.outcome_generation.mech_vent_v2 import prepare_resp_for_mv_outcomes
#     from GitHub.Outcome_Generation.Python.outcome_generation.aki_outcome_v2 import aki_outcome
#     from GitHub.Outcome_Generation.Python.outcome_generation.cam_icu_v2 import cam_icu_outcome
#     from GitHub.Outcome_Generation.Python.outcome_generation.duration_outcome_generation import generate_duration_outcomes
#     from GitHub.Utilities.Logging.log_messages import log_print_email_message as logm
#     from GitHub.Utilities.FileHandling.variable_specification_utilities import load_variables_from_var_spec
#     from GitHub.Utilities.PreProcessing.data_format_and_manipulation import ensure_columns
#     import pickle
    # locals().update(pickle.load(open(r"Z:\GitHub\APARI_Federated_Learning\outcome_generation_47_chunk_0.p", 'rb')))