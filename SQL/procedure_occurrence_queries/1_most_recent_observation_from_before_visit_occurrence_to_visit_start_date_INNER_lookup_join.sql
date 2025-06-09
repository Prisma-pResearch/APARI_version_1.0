/******
1_most_recent_observation_from_before_visit_occurrence_to_visit_start_date_INNER_lookup_join Query prepared at 2024-03-13 13:32:28.795563 by query_builder.py

retrieves variables from the following row_ids: 231||232||233
******/
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = 'parent_visit_occurrence_id')
with partitioned as (
SELECT
	C.subject_id,
	o.person_id,
	o.visit_occurrence_id,
	o.visit_detail_id,
	variable_name,
	o.observation_datetime [observation_datetime],
	o.value_as_concept_id [value_as_concept_id],
	ROW_NUMBER() OVER(PARTITION BY po.visit_occurrence_id , variable_name ORDER BY o.observation_datetime DESC) AS seq
FROM
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vop on vop.visit_occurrence_id = po.visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vo on vo.visit_occurrence_id = vop.parent_visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.observation o on (o.person_id = po.person_id)
	INNER JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table L on (L.concept_id = o.observation_concept_id AND L.variable_name IN ('marital_status', 'language', 'smoking_status'))

WHERE
	(
	o.value_as_concept_id <> 0
	AND
	o.observation_date <= CONVERT(DATE, vo.parent_visit_start_datetime)
	)
	AND
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY
)
SELECT 
	[observation_datetime],
	person_id,
	subject_id,
	[value_as_concept_id],
	variable_name,
	visit_detail_id,
	visit_occurrence_id
 FROM partitioned WHERE seq = 1;
ELSE
with partitioned as (
SELECT
	C.subject_id,
	o.person_id,
	o.visit_occurrence_id,
	o.visit_detail_id,
	variable_name,
	o.observation_datetime [observation_datetime],
	o.value_as_concept_id [value_as_concept_id],
	ROW_NUMBER() OVER(PARTITION BY po.visit_occurrence_id , variable_name ORDER BY o.observation_datetime DESC) AS seq
FROM
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vo on vo.visit_occurrence_id = po.visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.observation o on (o.person_id = po.person_id)
	INNER JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table L on (L.concept_id = o.observation_concept_id AND L.variable_name IN ('marital_status', 'language', 'smoking_status'))

WHERE
	(
	o.value_as_concept_id <> 0
	AND
	o.observation_date <= visit_start_date
	)
	AND
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY
)
SELECT 
	[observation_datetime],
	person_id,
	subject_id,
	[value_as_concept_id],
	variable_name,
	visit_detail_id,
	visit_occurrence_id
 FROM partitioned WHERE seq = 1;