/******
observation_from_procedure_occurrence_INNER_lookup_join Query prepared at 2024-03-13 13:32:28.809564 by query_builder.py

retrieves variables from the following row_ids: 14||24||25||26||27||28
******/
SELECT 
	C.subject_id,
	o.person_id,
	o.visit_occurrence_id,
	o.visit_detail_id,
	unit_concept_id,
	variable_name,
	o.observation_datetime [observation_datetime],
	o.value_as_number [value_as_number],
	o.value_as_concept_id [value_as_concept_id],
	o.value_as_string [value_as_string]
 FROM 
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.observation o on (o.person_id = po.person_id AND o.observation_date = po.procedure_date)
	INNER JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table L on (L.concept_id = o.observation_concept_id AND L.variable_name IN ('peep', 'gcs_eye_score', 'procedure_urgency', 'room_start_datetime', 'room_end_datetime', 'sched_post_op_location'))
 WHERE
	(
		(variable_name IN ('peep', 'gcs_eye_score') AND o.observation_datetime BETWEEN po.procedure_datetime AND po.procedure_end_datetime)
		OR
		variable_name IN ('procedure_urgency', 'room_start_datetime', 'room_end_datetime', 'sched_post_op_location')
	)
	AND
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY;
