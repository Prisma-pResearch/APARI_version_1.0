/******
cost_from_visit_start_datetime_to_visit_end_datetime_INNER_lookup_join Query prepared at 2024-03-13 13:32:28.738197 by query_builder.py

retrieves variables from the following row_ids: 6||7||8||9
******/
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = 'parent_visit_occurrence_id') 
SELECT 
	C.subject_id,
	po.visit_occurrence_id,
	cost.total_charge [total_charge],
	cost.total_cost [total_cost],
	total_charge * 0.36 [inferred_total_cost],
	cost.total_charge [professional_service_charge]
 FROM 
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vop on vop.visit_occurrence_id = po.visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vo on vo.visit_occurrence_id = vop.parent_visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.cost cost on (cost.cost_event_id = po.visit_occurrence_id AND cost_domain_id = 'visit occurrence' )
 WHERE 
	(
		(cost_type_concept_id IN (32814, 32852, 32853, 32854, 32844, 32845, 32821, 32810)) --XXX0XXX
		OR
		(cost_type_concept_id IN (32871, 32872)) --XXX1XXX
	)
	AND
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY;
ELSE
SELECT 
	C.subject_id,
	po.visit_occurrence_id,
	cost.total_charge [total_charge],
	cost.total_cost [total_cost],
	total_charge * 0.36 [inferred_total_cost],
	cost.total_charge [professional_service_charge]
 FROM 
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.cost cost on (cost.cost_event_id = po.visit_occurrence_id AND cost_domain_id = 'visit occurrence' )
 WHERE 
	(
		(cost_type_concept_id IN (32814, 32852, 32853, 32854, 32844, 32845, 32821, 32810)) --XXX0XXX
		OR
		(cost_type_concept_id IN (32871, 32872)) --XXX1XXX
	)
	AND
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY;