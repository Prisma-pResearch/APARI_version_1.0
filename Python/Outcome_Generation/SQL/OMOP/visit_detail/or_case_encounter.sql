SELECT DISTINCT
	vd.person_id,
	vd.visit_occurrence_id,
	vd.visit_detail_id,
	p.gender_concept_id,
	l.county,
	p.ethnicity_concept_id,
	p.race_concept_id,
	CASE WHEN vo.admitted_from_concept_id <> 0 THEN vo.admitted_from_concept_id ELSE NULL END [admitted_from_concept_id],
	vo.visit_start_datetime,
	vo.visit_end_datetime,
	vd.visit_detail_start_datetime,
	vd.care_site_id,
	vd.provider_id,
	prov.specialty_concept_id,
	po.procedure_source_concept_id,
	po2.procedure_concept_id [anesthesia_type],
	l.zip [person_zip],
	vo.visit_start_date [encounter_effective_date],
	vd.visit_detail_end_datetime,
	CONVERT(DATE, p.birth_datetime) [birth_date],
	DATEDIFF(YEAR, p.birth_datetime, vo.visit_start_datetime) [age],
	ppp.payer_concept_id,
	l2.zip [facility_zip],
	CASE WHEN vo.visit_concept_id = 262 THEN 1 ELSE 0 END seen_in_ed_y_n
FROM
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.VISIT_DETAIL vd on vd.visit_detail_id = c.subject_id
	INNER JOIN DaTa_ScHeMa.VISIT_OCCURRENCE vo on vo.visit_occurrence_id = vd.visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.PERSON p on p.person_id = vd.person_id
	LEFT JOIN DaTa_ScHeMa.PAYER_PLAN_PERIOD ppp on (ppp.person_id = vd.person_id AND c.cohort_start_date BETWEEN ppp.payer_plan_period_start_date AND ppp.payer_plan_period_end_date)
	LEFT JOIN DaTa_ScHeMa.[LOCATION] l on p.location_id = l.location_id -- NOTE We need to talk to other sites to see how they record past adresses the location history table is custom to our site and modeled after PCORNet.
	LEFT JOIN DaTa_ScHeMa.PROCEDURE_OCCURRENCE po on (po.visit_detail_id = vd.visit_detail_id AND po.modifier_concept_id IN (SELECT concept_id FROM LoOkUp_ScHeMa.IC3_Variable_Lookup_Table WHERE variable_name = 'primary_procedure')) -- Primary Procedure
	LEFT JOIN DaTa_ScHeMa.PROCEDURE_OCCURRENCE po2 on (po2.visit_detail_id = vd.visit_detail_id AND po2.procedure_concept_id IN (SELECT concept_id FROM LoOkUp_ScHeMa.IC3_Variable_Lookup_Table WHERE variable_name = 'anesthesia_type')) -- Anes Type
	LEFT JOIN DaTa_ScHeMa.[PROVIDER] prov on prov.provider_id = vd.provider_id
	LEFT JOIN DaTa_ScHeMa.CARE_SITE cs on cs.care_site_id = vd.care_site_id
	LEFT JOIN DaTa_ScHeMa.[LOCATION] l2 on l2.location_id = cs.location_id
WHERE
	c.cohort_definition_id = XXXXXX
	AND
	c.subset_id = YYYY
