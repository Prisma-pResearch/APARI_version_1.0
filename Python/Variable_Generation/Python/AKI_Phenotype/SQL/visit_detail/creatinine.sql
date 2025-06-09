SELECT
	m.person_id,
	m.visit_occurrence_id,
	'creatinine' variable_name,
	m.measurement_datetime,
	m.value_as_number,
	m.value_source_value,
	m.value_as_concept_id,
	m.operator_concept_id,
	m.unit_concept_id,
	m.unit_source_concept_id,
	m.unit_source_value
FROM
	DaTa_ScHeMa.MEASUREMENT m
WHERE
	m.measurement_concept_id IN (SELECT l.concept_id FROM LoOkUp_ScHeMa.IC3_Variable_Lookup_Table l where l.variable_name = 'creatinine')
	AND
	m.person_id IN (SELECT DISTINCT
						vd.person_id
					FROM
						ReSuLtS_ScHeMa.COHORT c
						INNER JOIN DaTa_ScHeMa.VISIT_DETAIL vd on vd.visit_detail_id = c.subject_id
					WHERE
						c.cohort_definition_id = XXXXXX
						AND
						c.subset_id = YYYY)