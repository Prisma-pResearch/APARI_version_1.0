/****** Script for SelectTopNRows command from SSMS  ******/
SELECT DISTINCT
	d.[person_id],
    d.[death_date],
    d.[death_type_concept_id]
FROM
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.VISIT_DETAIL vd on vd.visit_detail_id = c.subject_id
	INNER JOIN DaTa_ScHeMa.[DEATH] d on d.person_id = vd.person_id
WHERE
	c.cohort_definition_id = XXXXXX
	AND
	c.subset_id = YYYY