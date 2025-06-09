/******
visit_occurrence_from_visit_occurrence_LEFT_lookup_join Query prepared at 2024-03-13 13:32:28.827564 by query_builder.py

retrieves variables from the following row_ids: 0||256
******/
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = 'parent_visit_occurrence_id') 
SELECT 
	C.subject_id,
	vo.person_id,
	vo.visit_occurrence_id,
	variable_name,
	vo.visit_start_datetime [visit_start_datetime],
	vo.visit_end_datetime [visit_end_datetime],
	CASE WHEN variable_name = 'seen_in_ed_yn' THEN 1 ELSE 0 END [seen_in_ed_yn]
 FROM 
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vop on vop.visit_occurrence_id = po.visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vo on vo.visit_occurrence_id = vop.parent_visit_occurrence_id
	LEFT JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table L on (L.concept_id = vo.visit_concept_id AND L.variable_name IN ('seen_in_ed_yn'))
 WHERE 
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY;
ELSE
SELECT 
	C.subject_id,
	vo.person_id,
	vo.visit_occurrence_id,
	variable_name,
	vo.visit_start_datetime [visit_start_datetime],
	vo.visit_end_datetime [visit_end_datetime],
	CASE WHEN variable_name = 'seen_in_ed_yn' THEN 1 ELSE 0 END [seen_in_ed_yn]
 FROM 
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vo on vo.visit_occurrence_id = po.visit_occurrence_id
	LEFT JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table L on (L.concept_id = vo.visit_concept_id AND L.variable_name IN ('seen_in_ed_yn'))
 WHERE
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY;