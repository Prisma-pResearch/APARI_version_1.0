SELECT
	vd.visit_detail_id,
	a.aki_datetime,
	a.final_class,
	a.creatinine,
	a.reference_creatinine
FROM
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.VISIT_DETAIL vd on vd.visit_detail_id = c.subject_id
	INNER JOIN DaTa_ScHeMa.FINAL_AKI a on vd.person_id = a.person_id
WHERE
	c.cohort_definition_id = XXXXXX
	AND
	c.subset_id = YYYY
	AND
	CONVERT(DATE, a.aki_datetime) BETWEEN DATEADD(YEAR, -1, c.cohort_start_date) AND c.cohort_start_date