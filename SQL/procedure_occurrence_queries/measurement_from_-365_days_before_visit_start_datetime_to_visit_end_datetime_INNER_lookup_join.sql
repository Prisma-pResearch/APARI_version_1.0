/******
measurement_from_-365_days_before_visit_start_datetime_to_visit_end_datetime_INNER_lookup_join Query prepared at 2024-03-13 13:32:28.781563 by query_builder.py

retrieves variables from the following row_ids: 142
******/
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = 'parent_visit_occurrence_id') 
SELECT 
	C.subject_id,
	m.person_id,
	m.visit_occurrence_id,
	m.visit_detail_id,
	unit_concept_id,
	'platelets' [variable_name],
	m.measurement_datetime [measurement_datetime],
	m.value_as_number [value_as_number]
 FROM 
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vop on vop.visit_occurrence_id = po.visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vo on vo.visit_occurrence_id = vop.parent_visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.measurement m on (m.person_id = po.person_id
											 AND
											 m.measurement_datetime BETWEEN DATEADD(DAY, -365, vo.parent_visit_start_datetime) AND vo.parent_visit_end_datetime
											 AND
											 m.measurement_concept_id IN (SELECT
											                               concept_id
																		   FROM
																		    LoOkUp_ScHeMa.IC3_Variable_Lookup_Table
																		   WHERE
																		     variable_name IN ('platelets')))
 WHERE
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
	'platelets' [variable_name],
	m.measurement_datetime [measurement_datetime],
	m.value_as_number [value_as_number]
 FROM 
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vo on vo.visit_occurrence_id = po.visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.measurement m on (m.person_id = po.person_id
											 AND
											 m.measurement_datetime BETWEEN DATEADD(DAY, -365, vo.visit_start_datetime) AND vo.visit_end_datetime
											 AND
											 m.measurement_concept_id IN (SELECT
											                               concept_id
																		   FROM
																		    LoOkUp_ScHeMa.IC3_Variable_Lookup_Table
																		   WHERE
																		     variable_name IN ('platelets')))
 WHERE
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY;