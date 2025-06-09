/******
1_most_recent_measurement_from_-365_days_before_visit_start_datetime_to_procedure_datetime_INNER_lookup_join Query prepared at 2024-03-13 13:32:28.778560 by query_builder.py

retrieves variables from the following row_ids: 306||372
******/
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = 'parent_visit_occurrence_id') 
with partitioned as (
SELECT
	C.subject_id,
	m.person_id,
	m.visit_occurrence_id,
	m.visit_detail_id,
	unit_concept_id,
	variable_name,
	m.measurement_datetime [measurement_datetime],
	m.value_as_number [value_as_number],
	ROW_NUMBER() OVER(PARTITION BY po.procedure_occurrence_id , variable_name ORDER BY m.measurement_datetime DESC) AS seq
FROM
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vop on vop.visit_occurrence_id = po.visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vo on vo.visit_occurrence_id = vop.parent_visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.measurement m on (m.person_id = po.person_id)
	INNER JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table L on (L.concept_id = m.measurement_concept_id AND L.variable_name IN ('height', 'weight'))

WHERE
	m.measurement_datetime BETWEEN DATEADD(DAY, -365, vo.parent_visit_start_datetime) AND procedure_datetime
	AND
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY
)
SELECT 
	[measurement_datetime],
	person_id,
	subject_id,
	unit_concept_id,
	[value_as_number],
	variable_name,
	visit_detail_id,
	visit_occurrence_id
 FROM partitioned WHERE seq = 1;
 ELSE
	with partitioned as (
SELECT
	C.subject_id,
	m.person_id,
	m.visit_occurrence_id,
	m.visit_detail_id,
	unit_concept_id,
	variable_name,
	m.measurement_datetime [measurement_datetime],
	m.value_as_number [value_as_number],
	ROW_NUMBER() OVER(PARTITION BY po.procedure_occurrence_id , variable_name ORDER BY m.measurement_datetime DESC) AS seq
FROM
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vo on vo.visit_occurrence_id = po.visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.measurement m on (m.person_id = po.person_id)
	INNER JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table L on (L.concept_id = m.measurement_concept_id AND L.variable_name IN ('height', 'weight'))

WHERE
	m.measurement_datetime BETWEEN DATEADD(DAY, -365, visit_start_datetime) AND procedure_datetime
	AND
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY
)
SELECT 
	[measurement_datetime],
	person_id,
	subject_id,
	unit_concept_id,
	[value_as_number],
	variable_name,
	visit_detail_id,
	visit_occurrence_id
 FROM partitioned WHERE seq = 1;