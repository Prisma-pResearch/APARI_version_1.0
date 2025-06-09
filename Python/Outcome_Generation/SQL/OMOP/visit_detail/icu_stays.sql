SELECT
	vd1.[visit_detail_id]
	,vd1.person_id
	,vd1.[visit_occurrence_id]
    ,vd2.[visit_detail_start_datetime]
    ,vd2.[visit_detail_end_datetime]
    ,variable_name
FROM
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.VISIT_DETAIL vd1 on vd1.visit_detail_id = c.subject_id
	INNER JOIN DaTa_ScHeMa.VISIT_DETAIL vd2 on vd1.visit_occurrence_id = vd2.visit_occurrence_id
	INNER JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table l on (vd2.visit_detail_concept_id = l.concept_id
										  AND
										  l.variable_name IN ('icu'))
WHERE
	c.cohort_definition_id = XXXXXX
	AND
	c.subset_id = YYYY