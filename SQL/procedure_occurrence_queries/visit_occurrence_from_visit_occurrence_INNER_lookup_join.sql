/******
visit_occurrence_from_visit_occurrence_INNER_lookup_join Query prepared at 2024-03-13 13:32:28.826564 by query_builder.py

retrieves variables from the following row_ids: 84||94||99||100||103||104||105||148||149||168||228||244||249||250||251||254
******/
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = 'parent_visit_occurrence_id') 
SELECT 
	C.subject_id,
	vo.person_id,
	vo.visit_occurrence_id,
	vo.visit_start_datetime [visit_start_datetime],
	vo.visit_end_datetime [visit_end_datetime],
	vo.discharged_to_concept_id [discharged_to_concept_id],
	vo.visit_end_date [visit_end_date],
	vo.visit_start_date [visit_start_date],
	vo.visit_concept_id [visit_concept_id],
	vo.admitted_from_concept_id [admitted_from_concept_id],
	vo.provider_id [provider_id],
	vo.care_site_id [care_site_id]
 FROM 
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vop on vop.visit_occurrence_id = po.visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vo on vo.visit_occurrence_id = vop.parent_visit_occurrence_id
 WHERE
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY;
ELSE
SELECT 
	C.subject_id,
	vo.person_id,
	vo.visit_occurrence_id,
	vo.visit_start_datetime [visit_start_datetime],
	vo.visit_end_datetime [visit_end_datetime],
	vo.discharged_to_concept_id [discharged_to_concept_id],
	vo.visit_end_date [visit_end_date],
	vo.visit_start_date [visit_start_date],
	vo.visit_concept_id [visit_concept_id],
	vo.admitted_from_concept_id [admitted_from_concept_id],
	vo.provider_id [provider_id],
	vo.care_site_id [care_site_id]
 FROM 
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vo on vo.visit_occurrence_id = po.visit_occurrence_id
 WHERE
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY;