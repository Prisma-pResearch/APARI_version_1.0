/******
labs_from_visit_occurrence_INNER_lookup_join Query prepared at 2024-03-13 13:32:28.794560 by query_builder.py

retrieves variables from the following row_ids: 143||145
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
	m.value_as_concept_id [value_as_concept_id],
	m.value_as_number [value_as_number],
	m.value_source_value [value_source_value],
	m.operator_concept_id [operator_concept_id],
	m.unit_concept_id [unit_concept_id],
	m.unit_source_concept_id [unit_source_concept_id],
	m.unit_source_value [unit_source_value]
 FROM 
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vop on vop.visit_occurrence_id = po.visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vo on vo.visit_occurrence_id = vop.parent_visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.measurement m on (m.person_id = po.person_id)
	INNER JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table L on (L.concept_id = m.measurement_concept_id AND L.variable_name IN ('lactate', 'pao2'))
 WHERE 
	m.measurement_datetime BETWEEN vo.parent_visit_start_datetime AND vo.parent_visit_end_datetime
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
	m.value_as_concept_id [value_as_concept_id],
	m.value_as_number [value_as_number],
	m.value_source_value [value_source_value],
	m.operator_concept_id [operator_concept_id],
	m.unit_concept_id [unit_concept_id],
	m.unit_source_concept_id [unit_source_concept_id],
	m.unit_source_value [unit_source_value]
 FROM 
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vo on vo.visit_occurrence_id = po.visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.measurement m on (m.person_id = po.person_id)
	INNER JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table L on (L.concept_id = m.measurement_concept_id AND L.variable_name IN ('lactate', 'pao2'))
 WHERE 
	m.measurement_datetime BETWEEN visit_start_datetime AND visit_end_datetime
	AND
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY;