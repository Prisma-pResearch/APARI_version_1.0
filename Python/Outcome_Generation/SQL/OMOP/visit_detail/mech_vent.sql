SELECT
	d.person_id,
    d.visit_occurrence_id,
	l.variable_name,
    device_exposure_start_datetime,
    device_exposure_end_datetime,
    CASE WHEN d.device_exposure_start_datetime between dateadd(hour, 1, vd.visit_detail_end_datetime) and dateadd(hour, 2, vd.visit_detail_end_datetime) THEN 1
    ELSE 0 END as postop_mv_flag
FROM
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.VISIT_DETAIL vd on vd.visit_detail_id = c.subject_id
	INNER JOIN DaTa_ScHeMa.DEVICE_EXPOSURE d on d.visit_occurrence_id = vd.visit_occurrence_id
	INNER JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table l on (d.device_concept_id = l.concept_id
										  AND
										  l.variable_name IN ('mechanical_ventilation'))
WHERE
    c.cohort_definition_id = XXXXXX
	AND
	c.subset_id = YYYY