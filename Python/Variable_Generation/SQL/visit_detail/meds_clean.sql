SELECT DISTINCT
	vd.person_id,
	CASE WHEN ds.ingredient_concept_id IS NOT NULL THEN ds.ingredient_concept_id
		WHEN con.concept_class_id = 'Ingredient' THEN d.drug_concept_id
		ELSE NULL END [ingredient_concept_id],
	d.drug_exposure_start_datetime,
	d.drug_exposure_end_datetime,
	l.variable_name
FROM
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.VISIT_DETAIL vd on vd.visit_detail_id = c.subject_id
	INNER JOIN DaTa_ScHeMa.DRUG_EXPOSURE d on vd.person_id = d.person_id
	LEFT JOIN VoCaB_ScHeMa.DRUG_STRENGTH ds on ds.drug_concept_id = d.drug_concept_id
	LEFT JOIN VoCaB_ScHeMa.CONCEPT con on con.concept_id = d.drug_concept_id
	INNER JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table l on (l.concept_id = COALESCE(ds.ingredient_concept_id, d.drug_concept_id)
															 AND
															 l.domain = 'Drug')
WHERE
	CASE WHEN ds.ingredient_concept_id IS NOT NULL THEN ds.ingredient_concept_id
		WHEN con.concept_class_id = 'Ingredient' THEN d.drug_concept_id
		ELSE NULL END IS NOT NULL
	AND
	c.cohort_definition_id = XXXXXX
	AND
	c.subset_id = YYYY
	AND
	(d.drug_exposure_start_datetime <= c.cohort_end_date
	 OR
	 d.drug_exposure_end_datetime >= DATEADD(YEAR, -1, c.cohort_start_date))
	 AND
	 variable_name IS NOT NULL

