SELECT DISTINCT
	po.person_id,
	po.procedure_date [start_date],
	c2.concept_code [proc_code],
	CASE WHEN c2.vocabulary_id LIKE '%9%' THEN 'ICD9'
		WHEN c2.vocabulary_id LIKE 'CPT%' THEN 'CPT' ELSE 'ICD10' END  [proc_type]
FROM
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.VISIT_OCCURRENCE vo on vo.visit_occurrence_id = c.subject_id
	INNER JOIN DaTa_ScHeMa.PROCEDURE_OCCURRENCE po on po.person_id = vo.person_id
	INNER JOIN VoCaB_ScHeMa.CONCEPT c2 on (c2.concept_id = po.procedure_source_concept_id AND (c2.vocabulary_id LIKE 'ICD%' OR c2.vocabulary_id LIKE 'CPT%'))
WHERE
	c.cohort_definition_id = XXXXXX
	AND
	c.subset_id = YYYY
	AND
	po.procedure_date <= DATEADD(MONTH, 2, vo.visit_end_date)