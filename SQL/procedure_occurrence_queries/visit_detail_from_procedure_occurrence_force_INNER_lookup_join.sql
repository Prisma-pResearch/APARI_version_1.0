/******
visit_detail_from_procedure_occurrence_INNER_lookup_join Query prepared at 2024-03-13 13:32:28.824564 by query_builder.py

retrieves variables from the following row_ids: 134
******/
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = 'parent_visit_occurrence_id') 
SELECT 
	C.subject_id,
	po.person_id,
	po.visit_occurrence_id,
	po.visit_detail_id,
	COALESCE(po.procedure_datetime, vd.visit_detail_start_datetime) [visit_detail_start_datetime],
	COALESCE(po.procedure_end_datetime, vd.visit_detail_end_datetime) [visit_detail_end_datetime],
	COALESCE(po.provider_id, vd.provider_id) [provider_id]
 FROM 
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vop on vop.visit_occurrence_id = po.visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vo on vo.visit_occurrence_id = vop.parent_visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.visit_detail vd on (vd.visit_detail_id = po.visit_detail_id)
 WHERE 
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY;
ELSE
SELECT 
	C.subject_id,
	po.person_id,
	po.visit_occurrence_id,
	po.visit_detail_id,
	COALESCE(po.procedure_datetime, vd.visit_detail_start_datetime) [visit_detail_start_datetime],
	COALESCE(po.procedure_end_datetime, vd.visit_detail_end_datetime) [visit_detail_end_datetime],
	COALESCE(po.provider_id, vd.provider_id) [provider_id]
 FROM 
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.visit_detail vd on (vd.visit_detail_id = po.visit_detail_id)
 WHERE 
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY;