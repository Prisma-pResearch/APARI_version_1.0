SELECT DISTINCT
	m.person_id,
	l.variable_name,
	m.measurement_datetime,
	m.value_as_number,
	m.value_source_value,
	m.value_as_concept_id,
	m.operator_concept_id,
	m.unit_concept_id,
	m.unit_source_concept_id,
	m.unit_source_value
FROM
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.VISIT_DETAIL vd on vd.visit_detail_id = c.subject_id
	INNER JOIN DaTa_ScHeMa.MEASUREMENT m on m.person_id = vd.person_id
	INNER JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table l on (m.measurement_concept_id = l.concept_id
															 AND
															 l.domain = 'labs')
WHERE
	c.cohort_definition_id = XXXXXX
	AND
	c.subset_id = YYYY
	AND
	m.measurement_date BETWEEN DATEADD(YEAR, -1, c.cohort_start_date) AND c.cohort_start_date