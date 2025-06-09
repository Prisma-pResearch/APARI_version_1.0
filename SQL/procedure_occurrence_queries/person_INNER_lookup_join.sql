/******
person_INNER_lookup_join Query prepared at 2024-03-13 13:32:28.814564 by query_builder.py

retrieves variables from the following row_ids: 95||96||97||98||152||153||154||169||222||223||229
******/
SELECT 
	C.subject_id,
	p.person_id,
	CONVERT(DATE, p.birth_datetime) [birth_date],
	p.gender_concept_id [gender_concept_id],
	p.race_concept_id [race_concept_id],
	p.ethnicity_concept_id [ethnicity_concept_id],
	p.birth_datetime [birth_date]
 FROM 
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.person p on p.person_id = po.person_id
 WHERE 
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY;