SELECT DISTINCT
	co.person_id,
	co.condition_start_date [start_date],
	c2.concept_code [diag_code],
	CASE WHEN c2.vocabulary_id LIKE '%9%' THEN 'ICD9' ELSE 'ICD10' END  [diag_type],
	CASE WHEN co.condition_status_concept_id IN (32890, 32901, 32907) THEN 1 ELSE NULL END [poa]
FROM
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.VISIT_DETAIL vd on vd.visit_detail_id = c.subject_id
	INNER JOIN DaTa_ScHeMa.CONDITION_OCCURRENCE co on co.person_id = vd.person_id
	INNER JOIN VoCaB_ScHeMa.CONCEPT c2 on (c2.concept_id = co.condition_source_concept_id AND c2.vocabulary_id LIKE 'ICD%')
WHERE
	c.cohort_definition_id = XXXXXX
	AND
	c.subset_id = YYYY
	AND
	co.condition_start_date <= DATEADD(MONTH, 2, c.cohort_end_date)