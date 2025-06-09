/******
location_INNER_lookup_join Query prepared at 2024-03-13 13:32:28.747783 by query_builder.py

retrieves variables from the following row_ids: 207||252

The patients Home Location from Location History at procedure_datetime was used if available, otherwise the patients location from the person table was used
******/
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = 'parent_visit_occurrence_id') 
SELECT 
	C.subject_id,
	po.person_id,
	COALESCE(l2.county, loc.county) [county],
	COALESCE(l2.zip, loc.zip) [patient_zip]
 FROM 
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vop on vop.visit_occurrence_id = po.visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vo on vo.visit_occurrence_id = vop.parent_visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.person p on p.person_id = po.person_id
	LEFT JOIN DaTa_ScHeMa.LOCATION_HISTORY lh on (lh.entity_id = po.person_id AND lh.entity_domain = 'person' AND CONVERT(DATE, procedure_datetime) BETWEEN  lh.start_date AND lh.end_date)
	LEFT JOIN DaTa_ScHeMa.LOCATION l2 on l2.location_id = lh.location_id
	LEFT JOIN DaTa_ScHeMa.LOCATION loc on loc.location_id = p.location_id
 WHERE 
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY;
ELSE
SELECT 
	C.subject_id,
	po.person_id,
	loc.county [county], -- COALESCE(l2.county, loc.county) [county],
	loc.zip [patient_zip]  --COALESCE(l2.zip, loc.zip) [patient_zip]
 FROM 
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.person p on p.person_id = po.person_id
	-- LEFT JOIN DaTa_ScHeMa.LOCATION_HISTORY lh on (lh.entity_id = po.person_id AND lh.entity_domain = 'person' AND CONVERT(DATE, procedure_datetime) BETWEEN  lh.start_date AND lh.end_date)
	-- LEFT JOIN DaTa_ScHeMa.LOCATION l2 on l2.location_id = lh.location_id
	LEFT JOIN DaTa_ScHeMa.LOCATION loc on loc.location_id = p.location_id
 WHERE 
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY;