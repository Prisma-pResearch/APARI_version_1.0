/******
condition_occurrence_from_before_visit_occurrence_to_visit_end_datetime_INNER_lookup_join Query prepared at 2024-03-13 13:32:28.723066 by query_builder.py

retrieves variables from the following row_ids: 86||87||88||89||155
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
	INNER JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table L on (L.concept_id = co.condition_concept_id AND L.variable_name IN ('ckd', 'esrd', 'renal_transplant', 'aki'))
 WHERE 
	co.condition_start_date <= CONVERT(DATE, vo.parent_visit_end_datetime)
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
	INNER JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table L on (L.concept_id = co.condition_concept_id AND L.variable_name IN ('ckd', 'esrd', 'renal_transplant', 'aki'))
 WHERE 
	co.condition_start_date <= CONVERT(DATE, visit_end_datetime)
	AND
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY;