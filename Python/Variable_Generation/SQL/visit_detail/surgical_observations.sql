SELECT
	o.visit_detail_id,
	COALESCE (CONVERT(VARCHAR(50), o.value_as_concept_id), o.value_as_string) [value],
	l.variable_name
FROM
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.OBSERVATION o on o.visit_detail_id = c.subject_id
	INNER JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table l on (o.observation_concept_id = l.concept_id
										  AND
										  l.variable_name IN ('scheduled_surgical_service', 'scheduled_anesthesia_type', 'sched_post_op_location', 'case_status',
															 'nora_yn', 'sched_site', 'sched_location', 'sched_surgeon_deiden_id', 'sched_room',
															 'sched_trauma_room_y_n', 'sched_start_datetime', 'sched_primary_procedure'))
WHERE
	c.cohort_definition_id = XXXXXX
	AND
	c.subset_id = YYYY
