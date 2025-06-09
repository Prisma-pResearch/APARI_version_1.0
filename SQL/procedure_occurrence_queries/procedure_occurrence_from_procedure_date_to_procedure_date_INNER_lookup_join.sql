/******
procedure_occurrence_from_procedure_date_to_procedure_date_INNER_lookup_join Query prepared at 2024-03-13 13:32:28.816564 by query_builder.py

retrieves variables from the following row_ids: 225||226
******/
with partitioned as (
SELECT 
	C.subject_id,
	po.person_id,
	po.visit_occurrence_id,
	po.visit_detail_id,
	pop.procedure_datetime [procedure_datetime],
	pop.procedure_end_datetime [procedure_end_datetime],
	po.procedure_source_concept_id [procedure_concept_id],
	po.procedure_source_concept_id [procedure_source_concept_id],
	po.procedure_source_value,
	po.provider_id,
	prov.specialty_concept_id [specialty_concept_id],
	l.work_rvu * l.intra_op [intraop_rvu],
	CASE WHEN l.intra_op = 0 OR l.intra_op IS NULL THEN 0.755 * l.work_rvu ELSE l.work_rvu * l.intra_op END [intraop_rvu_adjusted],
	--po.modifier_source_value,
	ROW_NUMBER() OVER(PARTITION BY C.subject_id ORDER BY CASE WHEN l.intra_op = 0 OR l.intra_op IS NULL THEN 0.755 * l.work_rvu ELSE l.work_rvu * l.intra_op END DESC) AS seq
 FROM 
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence pop on pop.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on (po.procedure_date = pop.procedure_date AND po.person_id = pop.person_id AND po.procedure_type_concept_id = 32817)
	LEFT JOIN LoOkUp_ScHeMa.PHYSICIAN_RVU_Lookup l on (l.omop_concept_id = po.procedure_source_concept_id
																			AND
																			po.procedure_date BETWEEN l.[start_date] AND l.[end_date])
	LEFT JOIN DaTa_ScHeMa.provider prov on (prov.provider_id = po.provider_id)
	

 WHERE
	po.procedure_type_concept_id = 32817
	AND
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY), partitioned2 as (
SELECT 
	C.subject_id,
	po.person_id,
	po.visit_occurrence_id,
	po.visit_detail_id,
	L.variable_name,
	po.procedure_datetime [procedure_datetime],
	po.procedure_end_datetime [procedure_end_datetime],
	po.procedure_concept_id [procedure_concept_id],
	ROW_NUMBER() OVER(PARTITION BY C.subject_id ORDER BY CASE
															WHEN L2.variable_name = 'general anesthesia' THEN 1
															WHEN L2.variable_name = 'spinal anesthesia' THEN 2
															WHEN L2.variable_name = 'epidural anesthesia' THEN 3
															WHEN L2.variable_name = 'regional anesthesia' THEN 4
															WHEN L2.variable_name = 'local anesthesia' THEN 5
															WHEN L2.variable_name = 'sedation anesthesia' THEN 6
															ELSE 9 END ASC) AS seq
	
 FROM 
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence pop on pop.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on (po.procedure_date = pop.procedure_date AND po.person_id = pop.person_id)
	INNER JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table L on (L.concept_id = po.procedure_concept_id AND L.variable_name IN ('anesthesia_type'))
	LEFT JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table L2 on (L2.concept_id = po.procedure_concept_id
																				AND
																				L2.variable_name IN ('epidural anesthesia',
																				'general anesthesia',
																				'local anesthesia',
																				'regional anesthesia',
																				'spinal anesthesia',
																				'sedation anesthesia'))
 WHERE 
	po.procedure_type_concept_id = 32817
	AND
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY)
SELECT
	subject_id,
	person_id,
	visit_occurrence_id,
	visit_detail_id,
	'primary_procedure' [variable_name],
	[procedure_datetime],
	[procedure_end_datetime],
	[procedure_concept_id],
	[intraop_rvu],
	[intraop_rvu_adjusted],
	provider_id,
	specialty_concept_id
	--, modifier_source_value
FROM
	partitioned
WHERE
	seq = 1
UNION
SELECT
	subject_id,
	person_id,
	visit_occurrence_id,
	visit_detail_id,
	[variable_name],
	[procedure_datetime],
	[procedure_end_datetime],
	[procedure_concept_id],
	NULL [intraop_rvu],
	NULL [intraop_rvu_adjusted],
	NULL provider_id,
	NULL specialty_concept_id
	--, modifier_source_value
FROM
	partitioned2
WHERE
	seq = 1