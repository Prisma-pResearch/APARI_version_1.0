/******
visit_detail_from_visit_occurrence_INNER_lookup_join Query prepared at 2024-03-13 13:32:28.824564 by query_builder.py

retrieves variables from the following row_ids: 1||2||3||4||5||51||52||53||106||107||113||130||131||132||161||162||163||166||167||227||245||246||247||248
******/
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = 'parent_visit_occurrence_id') 
SELECT 
	C.subject_id,
	vd.person_id,
	vd.visit_occurrence_id,
	vd.visit_detail_id,
	variable_name,
	vd.visit_detail_start_datetime [visit_detail_start_datetime],
	vd.visit_detail_end_datetime [visit_detail_end_datetime],
	CASE WHEN vd.visit_detail_start_datetime < po.procedure_datetime THEN 1 ELSE 0 END [station_prior_to_or],
	CASE WHEN vd.visit_detail_start_datetime >= po.procedure_end_datetime THEN 1 ELSE 0 END [station_after_or],
	vd.visit_detail_concept_id [visit_detail_concept_id],
	vd.visit_detail_id [visit_detail_id],
	vd.provider_id [provider_id]
 FROM 
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vop on vop.visit_occurrence_id = po.visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vo on vo.visit_occurrence_id = vop.parent_visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.visit_detail2 vd on (vd.person_id = po.person_id
																	AND
													vd.visit_detail_start_datetime BETWEEN vo.parent_visit_start_datetime AND vo.parent_visit_end_datetime)
												
	LEFT JOIN  DaTa_ScHeMa.CARE_SITE2 care on care.care_site_id=vd.care_site_id
	INNER JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table L on (L.concept_id = care.place_of_service_concept_id AND L.variable_name IN ('emergency_department', 'intermediate_care/stepdown_unit', 'operating_room', 'or_holding_unit', 'post_anesthesia_care_unit', 'procedure_suite', 'ward', 'icu'))
 WHERE 

	variable_name IN ('emergency_department', 'intermediate_care/stepdown_unit', 'operating_room', 'or_holding_unit', 'post_anesthesia_care_unit', 'procedure_suite', 'ward', 'icu')

	AND
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY;
ELSE
SELECT 
	C.subject_id,
	vd.person_id,
	vd.visit_occurrence_id,
	vd.visit_detail_id,
	variable_name,
	vd.visit_detail_start_datetime [visit_detail_start_datetime],
	vd.visit_detail_end_datetime [visit_detail_end_datetime],
	CASE WHEN vd.visit_detail_start_datetime < po.procedure_datetime THEN 1 ELSE 0 END [station_prior_to_or],
	CASE WHEN vd.visit_detail_start_datetime >= po.procedure_end_datetime THEN 1 ELSE 0 END [station_after_or],
	vd.visit_detail_concept_id [visit_detail_concept_id],
	vd.visit_detail_id [visit_detail_id],
	vd.provider_id [provider_id]
 FROM 
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vo on vo.visit_occurrence_id = po.visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.visit_detail vd on (vd.person_id = po.person_id
																	AND
																	vd.visit_detail_start_datetime BETWEEN vo.visit_start_datetime AND vo.visit_end_datetime)
	INNER JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table L on (L.concept_id = vd.visit_detail_concept_id AND L.variable_name IN ('emergency_department', 'intermediate_care/stepdown_unit', 'operating_room', 'or_holding_unit', 'post_anesthesia_care_unit', 'procedure_suite', 'ward', 'icu'))
 WHERE 

	variable_name IN ('emergency_department', 'intermediate_care/stepdown_unit', 'operating_room', 'or_holding_unit', 'post_anesthesia_care_unit', 'procedure_suite', 'ward', 'icu')

	AND
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY;