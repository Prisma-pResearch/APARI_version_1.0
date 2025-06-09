/******
AVG_observation_from_procedure_end_datetime_to_4_hours_after_procedure_end_datetime_INNER_lookup_join Query prepared at 2024-03-13 13:32:28.812560 by query_builder.py

retrieves variables from the following row_ids: 82
******/
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = 'parent_visit_occurrence_id')
SELECT 
	C.subject_id,
	o.person_id,
	unit_concept_id,
	variable_name,
	AVG(o.value_as_number) [value_as_number],
	COUNT(o.value_as_number) [value_as_number_count]
 FROM 
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vop on vop.visit_occurrence_id = po.visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vo on vo.visit_occurrence_id = vop.parent_visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.observation o on (o.person_id = po.person_id)
	INNER JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table L on (L.concept_id = o.observation_concept_id AND L.variable_name IN ('gcs_eye_score'))
 WHERE 
	o.observation_datetime BETWEEN procedure_end_datetime AND DATEADD(HOUR, 4, procedure_end_datetime)
	AND
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY
GROUP BY
	C.subject_id,
	o.person_id,
	unit_concept_id,
	variable_name
ELSE
SELECT 
	C.subject_id,
	o.person_id,
	unit_concept_id,
	variable_name,
	AVG(o.value_as_number) [value_as_number],
	COUNT(o.value_as_number) [value_as_number_count]
 FROM 
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.observation o on (o.person_id = po.person_id)
	INNER JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table L on (L.concept_id = o.observation_concept_id AND L.variable_name IN ('gcs_eye_score'))
 WHERE 
	o.observation_datetime BETWEEN procedure_end_datetime AND DATEADD(HOUR, 4, procedure_end_datetime)
	AND
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY
GROUP BY
	C.subject_id,
	o.person_id,
	unit_concept_id,
	variable_name