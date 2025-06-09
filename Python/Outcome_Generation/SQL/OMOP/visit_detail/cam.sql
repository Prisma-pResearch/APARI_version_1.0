SELECT DISTINCT
	m.person_id,
    m.visit_occurrence_id,
	l.variable_name,
    m.measurement_datetime,
	m.value_as_concept_id
FROM
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.VISIT_DETAIL vd on vd.visit_detail_id = c.subject_id
	INNER JOIN DaTa_ScHeMa.MEASUREMENT m on m.visit_occurrence_id = vd.visit_occurrence_id
	INNER JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table l on (m.measurement_concept_id = l.concept_id
										  AND
										  l.variable_name IN ('cam'))
WHERE
    c.cohort_definition_id = XXXXXX
	AND
	c.subset_id = YYYY
UNION
SELECT DISTINCT
	o.person_id,
    o.visit_occurrence_id,
	l.variable_name,
    o.observation_datetime,
	o.value_as_concept_id
FROM
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.VISIT_DETAIL vd on vd.visit_detail_id = c.subject_id
	INNER JOIN DaTa_ScHeMa.OBSERVATION o on o.visit_occurrence_id = vd.visit_occurrence_id
	INNER JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table l on (o.observation_concept_id = l.concept_id
										  AND
										  l.variable_name IN ('cam'))
WHERE
    c.cohort_definition_id = XXXXXX
	AND
	c.subset_id = YYYY