/******
observation_from_procedure_date_to_procedure_date_INNER_lookup_join Query prepared at 2024-03-13 13:32:28.807564 by query_builder.py

retrieves variables from the following row_ids: 240||241||255||236||237||239
******/
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = 'parent_visit_occurrence_id') 
SELECT 
	C.subject_id,
	o.person_id,
	o.visit_occurrence_id,
	o.visit_detail_id,
	variable_name,
	o.observation_datetime [observation_datetime],
	o.value_as_concept_id [value_as_concept_id],
	o.value_as_string [value_as_string]
 FROM 
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vop on vop.visit_occurrence_id = po.visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vo on vo.visit_occurrence_id = vop.parent_visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.observation o on (o.person_id = po.person_id AND o.observation_date = po.procedure_date)
	INNER JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table L on (L.concept_id = o.observation_concept_id AND L.variable_name IN ('sched_post_op_location', 'procedure_urgency', 'sched_start_datetime', 'surgical_service', 'scheduled_surgical_service', 'scheduled_anesthesia_type'))
 WHERE 
	(
		(
			variable_name IN ('sched_post_op_location', 'procedure_urgency', 'sched_start_datetime')
			) --XXX0XXX
		OR
		(
			o.value_as_concept_id <> 0
			AND
			variable_name IN ('surgical_service', 'scheduled_surgical_service', 'scheduled_anesthesia_type')
			) --XXX1XXX
	)
	AND
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY;
ELSE
SELECT 
	C.subject_id,
	o.person_id,
	o.visit_occurrence_id,
	o.visit_detail_id,
	variable_name,
	o.observation_datetime [observation_datetime],
	o.value_as_concept_id [value_as_concept_id],
	o.value_as_string [value_as_string]
 FROM 
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.observation o on (o.person_id = po.person_id AND o.observation_date = po.procedure_date)
	INNER JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table L on (L.concept_id = o.observation_concept_id AND L.variable_name IN ('sched_post_op_location', 'procedure_urgency', 'sched_start_datetime', 'surgical_service', 'scheduled_surgical_service', 'scheduled_anesthesia_type'))
 WHERE 
	(
		(
			variable_name IN ('sched_post_op_location', 'procedure_urgency', 'sched_start_datetime')
			) --XXX0XXX
		OR
		(
			o.value_as_concept_id <> 0
			AND
			variable_name IN ('surgical_service', 'scheduled_surgical_service', 'scheduled_anesthesia_type')
			) --XXX1XXX
	)
	AND
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY;