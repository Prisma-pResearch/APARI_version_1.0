/******
location_from_visit_start_datetime_to_visit_end_datetime_INNER_lookup_join Query prepared at 2024-03-13 13:32:28.776560 by query_builder.py

retrieves variables from the following row_ids: 253
******/
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = 'parent_visit_occurrence_id') 
SELECT 
	C.subject_id,
	po.visit_occurrence_id,
	loc.zip [facility_zip]
 FROM 
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vop on vop.visit_occurrence_id = po.visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vo on vo.visit_occurrence_id = vop.parent_visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.CARE_SITE cs on cs.care_site_id = vo.care_site_id
	INNER JOIN DaTa_ScHeMa.LOCATION loc on loc.location_id = cs.location_id
 WHERE 
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY;
ELSE
SELECT 
	C.subject_id,
	po.visit_occurrence_id,
	loc.zip [facility_zip]
 FROM 
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vo on vo.visit_occurrence_id = po.visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.CARE_SITE cs on cs.care_site_id = vo.care_site_id
	INNER JOIN DaTa_ScHeMa.LOCATION loc on loc.location_id = cs.location_id
 WHERE 
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY;