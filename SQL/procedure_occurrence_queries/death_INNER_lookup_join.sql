/******
death_INNER_lookup_join Query prepared at 2024-03-13 13:32:28.739197 by query_builder.py

retrieves variables from the following row_ids: 101||102||110||111
******/
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = 'parent_visit_occurrence_id') 
SELECT 
	C.subject_id,
	death.person_id,
	death.death_date [death_date],
	death.death_type_concept_id [death_type_concept_id]
 FROM 
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vop on vop.visit_occurrence_id = po.visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vo on vo.visit_occurrence_id = vop.parent_visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.death death on (death.person_id = po.person_id)
 WHERE 
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY;
ELSE
SELECT 
	C.subject_id,
	death.person_id,
	death.death_date [death_date],
	death.death_type_concept_id [death_type_concept_id]
 FROM 
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.death death on (death.person_id = po.person_id)
 WHERE 
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY;