/******
AVG_measurement_from_procedure_end_datetime_to_4_hours_after_procedure_end_datetime_INNER_lookup_join Query prepared at 2024-03-13 13:32:28.789563 by query_builder.py

retrieves variables from the following row_ids: 29||30||31||32||33||34||35
******/
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = 'parent_visit_occurrence_id') 
SELECT 
	C.subject_id,
	m.person_id,
	unit_concept_id,
	variable_name,
	AVG(m.value_as_number) [value_as_number],
	COUNT(m.value_as_number) [value_as_number_count]
 FROM 
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.measurement m on (m.person_id = po.person_id)
	INNER JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table L on (L.concept_id = m.measurement_concept_id AND L.variable_name IN ('diastolic_blood_pressure', 'heart_rate', 'systolic_blood_pressure', 'spo2', 'respiratory_rate', 'body_temperature', 'gcs_eye_score'))
 WHERE 
	m.measurement_datetime BETWEEN procedure_end_datetime AND DATEADD(HOUR, 4, procedure_end_datetime)
	AND
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY
GROUP BY
	C.subject_id,
	m.person_id,
	unit_concept_id,
	variable_name
ELSE
SELECT 
	C.subject_id,
	m.person_id,
	unit_concept_id,
	variable_name,
	AVG(m.value_as_number) [value_as_number],
	COUNT(m.value_as_number) [value_as_number_count]
 FROM 
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.measurement m on (m.person_id = po.person_id)
	INNER JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table L on (L.concept_id = m.measurement_concept_id AND L.variable_name IN ('diastolic_blood_pressure', 'heart_rate', 'systolic_blood_pressure', 'spo2', 'respiratory_rate', 'body_temperature', 'gcs_eye_score'))
 WHERE 
	m.measurement_datetime BETWEEN procedure_end_datetime AND DATEADD(HOUR, 4, procedure_end_datetime)
	AND
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY
GROUP BY
	C.subject_id,
	m.person_id,
	unit_concept_id,
	variable_name