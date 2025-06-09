/******
visit_detail_from_procedure_date_to_procedure_date_INNER_lookup_join Query prepared at 2024-03-13 13:32:28.822564 by query_builder.py

retrieves variables from the following row_ids: 150||151
******/

SELECT 
	C.subject_id,
	vd.person_id,
	vd.visit_occurrence_id,
	vd.visit_detail_id,
	vd.visit_detail_start_datetime [visit_detail_start_datetime],
	vd.visit_detail_end_datetime [visit_detail_end_datetime]
 FROM 
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.visit_detail2 vd on (vd.person_id = po.person_id
																	AND
																	vd.visit_detail_start_datetime BETWEEN procedure_datetime AND procedure_end_datetime)
 WHERE 
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY;
