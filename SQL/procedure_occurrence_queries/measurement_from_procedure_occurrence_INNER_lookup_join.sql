/******
measurement_from_procedure_occurrence_INNER_lookup_join Query prepared at 2024-03-13 13:32:28.782560 by query_builder.py

retrieves variables from the following row_ids: 10||11||12||13||15||16||17||18||19||20||21||22||23
******/
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = 'parent_visit_occurrence_id') 
SELECT 
	C.subject_id,
	m.person_id,
	m.visit_occurrence_id,
	m.visit_detail_id,
	unit_concept_id,
	variable_name,
	m.measurement_datetime [measurement_datetime],
	m.value_as_number [value_as_number]
 FROM 
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vop on vop.visit_occurrence_id = po.visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vo on vo.visit_occurrence_id = vop.parent_visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.measurement m on (m.person_id = po.person_id)
	INNER JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table L on (L.concept_id = m.measurement_concept_id AND L.variable_name IN ('diastolic_blood_pressure', 'etco2', 'pip', 'peep', 'tidal_volume', 'tidal_volume_exhaled', 'tidal_volume_inspired', 'heart_rate', 'systolic_blood_pressure', 'spo2', 'respiratory_rate', 'body_temperature', 'gcs_eye_score'))
 WHERE 
	m.measurement_datetime BETWEEN procedure_datetime AND procedure_end_datetime
	AND
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY;
ELSE
SELECT 
	C.subject_id,
	m.person_id,
	m.visit_occurrence_id,
	m.visit_detail_id,
	unit_concept_id,
	variable_name,
	m.measurement_datetime [measurement_datetime],
	m.value_as_number [value_as_number]
 FROM 
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.measurement m on (m.person_id = po.person_id)
	INNER JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table L on (L.concept_id = m.measurement_concept_id AND L.variable_name IN ('diastolic_blood_pressure', 'etco2', 'pip', 'peep', 'tidal_volume', 'tidal_volume_exhaled', 'tidal_volume_inspired', 'heart_rate', 'systolic_blood_pressure', 'spo2', 'respiratory_rate', 'body_temperature', 'gcs_eye_score'))
 WHERE 
	m.measurement_datetime BETWEEN procedure_datetime AND procedure_end_datetime
	AND
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY;