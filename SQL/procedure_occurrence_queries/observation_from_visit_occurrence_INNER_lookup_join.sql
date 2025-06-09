/******
observation_from_visit_occurrence_INNER_lookup_join Query prepared at 2024-03-13 13:32:28.810564 by query_builder.py

retrieves variables from the following row_ids: 109
******/
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = 'parent_visit_occurrence_id') 
SELECT 
	C.subject_id,
	o.person_id,
	o.visit_occurrence_id,
	o.visit_detail_id,
	variable_name,
	o.observation_datetime [observation_datetime],
	o.value_as_concept_id [value_as_concept_id]
 FROM 
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vop on vop.visit_occurrence_id = po.visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vo on vo.visit_occurrence_id = vop.parent_visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.observation o on (o.person_id = po.person_id)
	INNER JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table L on (L.concept_id = o.observation_concept_id AND L.variable_name IN ('cam'))
 WHERE 
	(
	o.value_as_concept_id <> 0
	AND
	o.observation_datetime BETWEEN vo.parent_visit_start_datetime AND vo.parent_visit_end_datetime
	)
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
	o.value_as_concept_id [value_as_concept_id]
 FROM 
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vo on vo.visit_occurrence_id = po.visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.observation o on (o.person_id = po.person_id)
	INNER JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table L on (L.concept_id = o.observation_concept_id AND L.variable_name IN ('cam'))
 WHERE 
	(
	o.value_as_concept_id <> 0
	AND
	o.observation_datetime BETWEEN visit_start_datetime AND visit_end_datetime
	)
	AND
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY;