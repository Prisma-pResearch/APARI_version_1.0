SELECT
	o.visit_occurrence_id,
	o.observation_datetime,
	o.value_as_concept_id,
	CONCAT(l.variable_name, '_concept_id') [variable_name]
FROM
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.VISIT_DETAIL vd on vd.visit_detail_id = c.subject_id
	INNER JOIN DaTa_ScHeMa.OBSERVATION o on o.visit_occurrence_id = vd.visit_occurrence_id
	INNER JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table l on (o.observation_concept_id = l.concept_id
										  AND
										  l.variable_name IN ('admit_priority', 'admitting_service', 'discharge_service'))
WHERE
	c.cohort_definition_id = XXXXXX
	AND
	c.subset_id = YYYY
	AND
	o.value_as_concept_id <> 0