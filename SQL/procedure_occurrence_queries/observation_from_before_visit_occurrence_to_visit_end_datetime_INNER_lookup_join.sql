/******
observation_from_before_visit_occurrence_to_visit_end_datetime_INNER_lookup_join Query prepared at 2024-03-13 13:32:28.808564 by query_builder.py

retrieves variables from the following row_ids: 93
******/
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = 'parent_visit_occurrence_id') 
SELECT 
	C.subject_id,
	o.person_id,
	o.visit_occurrence_id,
	o.visit_detail_id,
	variable_name,
	o.observation_datetime [observation_datetime],
	o.observation_concept_id [observation_concept_id]
 FROM 
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vop on vop.visit_occurrence_id = po.visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vo on vo.visit_occurrence_id = vop.parent_visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.observation o on (o.person_id = po.person_id)
	INNER JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table L on (L.concept_id = o.observation_concept_id AND L.variable_name IN ('dialysis'))
 WHERE 
	o.observation_datetime <= vo.parent_visit_end_datetime
	AND
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY;
ELSE
SELECT 
	C.subject_id,
	o.person_id,
	o.visit_occurrence_id,
	o.visit_detail_id,
	variable_name,
	o.observation_datetime [observation_datetime],
	o.observation_concept_id [observation_concept_id]
 FROM 
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vo on vo.visit_occurrence_id = po.visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.observation o on (o.person_id = po.person_id)
	INNER JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table L on (L.concept_id = o.observation_concept_id AND L.variable_name IN ('dialysis'))
 WHERE 
	o.observation_datetime <= visit_end_datetime
	AND
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY;