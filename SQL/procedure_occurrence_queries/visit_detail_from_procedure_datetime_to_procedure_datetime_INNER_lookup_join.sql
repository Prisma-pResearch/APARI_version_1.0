/******
visit_detail_from_procedure_datetime_to_procedure_datetime_INNER_lookup_join Query prepared at 2024-03-13 13:32:28.823564 by query_builder.py

retrieves variables from the following row_ids: 170
******/
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = 'parent_visit_occurrence_id') 
SELECT 
	C.subject_id,
	vd.person_id,
	vd.visit_occurrence_id,
	vd.visit_detail_id,
	vd.visit_detail_start_datetime [visit_detail_start_datetime],
	vd.visit_detail_end_datetime [visit_detail_end_datetime],
	vd.care_site_id [care_site_id]
 FROM 
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vop on vop.visit_occurrence_id = po.visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vo on vo.visit_occurrence_id = vop.parent_visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.visit_detail2 vd on (vd.person_id = po.person_id)
 WHERE 
	vd.visit_detail_start_datetime BETWEEN procedure_datetime AND procedure_datetime
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
	vd.visit_detail_start_datetime [visit_detail_start_datetime],
	vd.visit_detail_end_datetime [visit_detail_end_datetime],
	vd.care_site_id [care_site_id]
 FROM 
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.visit_detail vd on (vd.person_id = po.person_id)
 WHERE 
	vd.visit_detail_start_datetime BETWEEN procedure_datetime AND procedure_datetime
	AND
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY;