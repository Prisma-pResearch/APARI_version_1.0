SELECT DISTINCT
	d.person_id,
	d.device_exposure_start_datetime [start_date],
	1 [dialysis]
FROM
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.VISIT_DETAIL vd on vd.visit_detail_id = c.subject_id
	INNER JOIN DaTa_ScHeMa.DEVICE_EXPOSURE d on d.person_id = vd.person_id
	INNER JOIN DaTa_ScHeMa.VISIT_OCCURRENCE vo on vo.visit_occurrence_id = vd.visit_occurrence_id
	INNER JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table l on (d.device_concept_id = l.concept_id
										  AND
										  l.variable_name IN ('dialysis')
										  AND
										  l.domain = 'DEVICE')
WHERE
	c.cohort_definition_id = XXXXXX
	AND
	c.subset_id = YYYY
	AND
	d.device_exposure_start_date <= vo.visit_end_date  -- USED for AKI Phenotyping both Pre-OP and Postop
UNION
SELECT DISTINCT
	o.person_id,
	o.observation_datetime [start_date],
	1 [dialysis]
FROM
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.VISIT_DETAIL vd on vd.visit_detail_id = c.subject_id
	INNER JOIN DaTa_ScHeMa.OBSERVATION o on o.person_id = vd.person_id
	INNER JOIN DaTa_ScHeMa.VISIT_OCCURRENCE vo on vo.visit_occurrence_id = vd.visit_occurrence_id
	INNER JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table l on (o.observation_concept_id = l.concept_id
										  AND
										  l.variable_name = 'dialysis')
WHERE
	c.cohort_definition_id = XXXXXX
	AND
	c.subset_id = YYYY
	AND
	o.observation_type_concept_id <> 32838
	AND
	o.observation_date <= vo.visit_end_date  -- USED for AKI Phenotyping both Pre-OP and Postop