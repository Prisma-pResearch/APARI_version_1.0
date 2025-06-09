SELECT DISTINCT
	co.person_id,
	co.condition_start_date [start_date],
	co.condition_end_date [end_date],
	co.condition_concept_id,
	co.condition_status_concept_id,
	CASE WHEN co.condition_status_concept_id IN (32890, 32901, 32907) THEN 1 ELSE NULL END [poa]
FROM
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.VISIT_DETAIL vd on vd.visit_detail_id = c.subject_id
	INNER JOIN DaTa_ScHeMa.CONDITION_OCCURRENCE co on co.person_id = vd.person_id
	INNER JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table l on (co.condition_concept_id = l.concept_id
										  AND
										  l.domain = 'Condition')
WHERE
	c.cohort_definition_id = XXXXXX
	AND
	c.subset_id = YYYY
	AND
	co.condition_start_date <= DATEADD(MONTH, 2, c.cohort_start_date)