SELECT DISTINCT
	vo.person_id,
	vo.visit_occurrence_id,
	vo.visit_start_date [encounter_effective_date],
	vo.discharged_to_concept_id,
	CONVERT(DATE, p.birth_datetime) [birth_date],
	p.gender_concept_id,
	p.race_concept_id,
	p.ethnicity_concept_id,
	vo.visit_start_datetime,
	vo.visit_end_datetime,
	NULL [patient_type],
	NULL [height_cm],
	NULL [weight_kgs]
FROM
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.PROCEDURE_OCCURRENCE po on po.procedure_occurrence_id = c.subject_id
	INNER JOIN DaTa_ScHeMa.PERSON p on p.person_id = po.person_id
	INNER JOIN DaTa_ScHeMa.VISIT_OCCURRENCE vo on vo.visit_occurrence_id = po.visit_occurrence_id
WHERE
	c.cohort_definition_id = XXXXXX
	AND
	c.subset_id = YYYY