/******
condition_occurrence_from_visit_start_datetime_to_visit_end_datetime_INNER_lookup_join Query prepared at 2024-03-13 13:32:28.726094 by query_builder.py

retrieves variables from the following row_ids: 114||115||116||117||118||119||120||121||122||123||124||125||126||127||128||129||135
******/
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = 'parent_visit_occurrence_id') 
SELECT 
	C.subject_id,
	co.person_id,
	co.visit_occurrence_id,
	co.visit_detail_id,
	variable_name,
	CASE WHEN co.condition_status_concept_id IN (32890, 32901, 32907) THEN 1 ELSE NULL END [poa],
	co.condition_start_date [condition_start_date],
	co.condition_end_date [condition_end_date],
	co.condition_concept_id [condition_concept_id]
 FROM 
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vop on vop.visit_occurrence_id = po.visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vo on vo.visit_occurrence_id = vop.parent_visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.condition_occurrence co on (co.person_id = po.person_id)
	INNER JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table L on (L.concept_id = co.condition_concept_id AND L.variable_name IN ('cv_cardiac_arrest', 'cv_hypo_no_shock', 'cv_hf', 'cv_shock', 'delirium_icd', 'mech_wound', 'neuro_other', 'neuro_plegia_paralytic', 'neuro_stroke', 'surg_infection', 'proc_graft_implant_foreign_body', 'proc_hemorrhage_hematoma_seroma', 'proc_non_hemorrhagic_technical', 'sepsis', 'vte_pe', 'vte_deep_super_vein'))
 WHERE 
	co.condition_start_date BETWEEN CONVERT(DATE, vo.parent_visit_start_datetime) AND CONVERT(DATE, vo.parent_visit_end_datetime)
	AND
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY;
ELSE
SELECT 
	C.subject_id,
	co.person_id,
	co.visit_occurrence_id,
	co.visit_detail_id,
	variable_name,
	CASE WHEN co.condition_status_concept_id IN (32890, 32901, 32907) THEN 1 ELSE NULL END [poa],
	co.condition_start_date [condition_start_date],
	co.condition_end_date [condition_end_date],
	co.condition_concept_id [condition_concept_id]
 FROM 
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vo on vo.visit_occurrence_id = po.visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.condition_occurrence co on (co.person_id = po.person_id)
	INNER JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table L on (L.concept_id = co.condition_concept_id AND L.variable_name IN ('cv_cardiac_arrest', 'cv_hypo_no_shock', 'cv_hf', 'cv_shock', 'delirium_icd', 'mech_wound', 'neuro_other', 'neuro_plegia_paralytic', 'neuro_stroke', 'surg_infection', 'proc_graft_implant_foreign_body', 'proc_hemorrhage_hematoma_seroma', 'proc_non_hemorrhagic_technical', 'sepsis', 'vte_pe', 'vte_deep_super_vein'))
 WHERE 
	co.condition_start_date BETWEEN CONVERT(DATE, visit_start_datetime) AND CONVERT(DATE, visit_end_datetime)
	AND
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY;