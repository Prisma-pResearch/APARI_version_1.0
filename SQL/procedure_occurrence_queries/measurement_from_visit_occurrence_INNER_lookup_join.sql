/******
measurement_from_visit_occurrence_INNER_lookup_join Query prepared at 2024-03-13 13:32:28.788563 by query_builder.py

retrieves variables from the following row_ids: 45||46||47||137||139||140||144||146||147||108
******/
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = 'parent_visit_occurrence_id') 
SELECT
	--variable_name,
	--COUNT(*)
	C.subject_id,
	m.person_id,
	m.visit_occurrence_id,
	m.visit_detail_id,
	variable_name,
	m.measurement_datetime [measurement_datetime],
	m.unit_concept_id [unit_concept_id],
	m.value_as_number [value_as_number],
	CASE WHEN (measurement_datetime < procedure_datetime) AND variable_name = 'rbc_transfusion' THEN 'preop_rbc_volume'
         WHEN measurement_datetime BETWEEN procedure_datetime AND procedure_end_datetime  AND variable_name = 'rbc_transfusion' THEN 'intraop_rbc_volume'
         WHEN measurement_datetime > procedure_end_datetime  AND variable_name = 'rbc_transfusion' THEN 'postop_rbc_volume' ELSE NULL END [transfusion],
	m.value_as_concept_id [value_as_concept_id]
 FROM 
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vop on vop.visit_occurrence_id = po.visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vo on vo.visit_occurrence_id = vop.parent_visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.measurement m on (m.person_id = po.person_id)
	INNER JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table L on (L.concept_id = m.measurement_concept_id AND L.variable_name IN ('rbc_transfusion', 'mean_arterial_pressure', 'respiratory_rate', 'systolic_blood_pressure', 'gcs_score', 'spo2', 'fio2', 'cam'))
 WHERE 
	(
		(
			m.measurement_datetime BETWEEN vo.parent_visit_start_datetime AND vo.parent_visit_end_datetime
			AND
			variable_name IN ('rbc_transfusion', 'mean_arterial_pressure', 'respiratory_rate', 'systolic_blood_pressure', 'gcs_score', 'spo2', 'fio2')
			) --XXX0XXX
		OR
		(
			m.value_as_concept_id <> 0
			AND
			m.measurement_datetime BETWEEN vo.visit_start_datetime AND vo.visit_end_datetime
			AND
			variable_name IN ('cam')
			) --XXX1XXX
	)
	AND
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY
--GROUP BY
--	variable_name
	;
ELSE
SELECT 
	C.subject_id,
	m.person_id,
	m.visit_occurrence_id,
	m.visit_detail_id,
	variable_name,
	m.measurement_datetime [measurement_datetime],
	m.unit_concept_id [unit_concept_id],
	m.value_as_number [value_as_number],
	CASE WHEN measurement_datetime < procedure_datetime  AND variable_name = 'rbc_transfusion' THEN 'preop_rbc_volume'
         WHEN measurement_datetime BETWEEN procedure_datetime AND procedure_end_datetime  AND variable_name = 'rbc_transfusion' THEN 'intraop_rbc_volume'
         WHEN measurement_datetime > procedure_end_datetime  AND variable_name = 'rbc_transfusion' THEN 'postop_rbc_volume' ELSE NULL END [transfusion],
	m.value_as_concept_id [value_as_concept_id]
 FROM 
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vo on vo.visit_occurrence_id = po.visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.measurement m on (m.person_id = po.person_id)
	INNER JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table L on (L.concept_id = m.measurement_concept_id AND L.variable_name IN ('rbc_transfusion', 'mean_arterial_pressure', 'respiratory_rate', 'systolic_blood_pressure', 'gcs_score', 'spo2', 'fio2', 'cam'))
 WHERE 
	(
		(
			m.measurement_datetime BETWEEN visit_start_datetime AND visit_end_datetime
			AND
			variable_name IN ('rbc_transfusion', 'mean_arterial_pressure', 'respiratory_rate', 'systolic_blood_pressure', 'gcs_score', 'spo2', 'fio2')
			) --XXX0XXX
		OR
		(
			m.value_as_concept_id <> 0
			AND
			m.measurement_datetime BETWEEN visit_start_datetime AND visit_end_datetime
			AND
			variable_name IN ('cam')
			) --XXX1XXX
	)
	AND
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY;