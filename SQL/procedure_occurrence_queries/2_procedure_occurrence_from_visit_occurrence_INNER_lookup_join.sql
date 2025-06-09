/******
2_procedure_occurrence_from_visit_occurrence_INNER_lookup_join Query prepared at 2024-03-13 13:32:28.815560 by query_builder.py

retrieves variables from the following row_ids: 78
******/
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = 'parent_visit_occurrence_id') 
with partitioned as (
SELECT
	subject_id,
	po2.person_id,
	po2.visit_occurrence_id,
	po2.visit_detail_id,
	'surgery' variable_name,
	po2.procedure_datetime [procedure_datetime],
	po2.procedure_end_datetime [procedure_end_datetime],
	po3.procedure_source_concept_id [second_or_procedure],
	l.work_rvu * l.intra_op [intraop_rvu],
	CASE WHEN l.intra_op = 0 OR l.intra_op IS NULL THEN 0.755 * l.work_rvu ELSE l.work_rvu * l.intra_op END [intraop_rvu_adjusted],
	ROW_NUMBER() OVER(PARTITION BY vo.visit_occurrence_id ORDER BY po2.procedure_datetime ASC, CASE WHEN l.intra_op = 0 OR l.intra_op IS NULL THEN 0.755 * l.work_rvu ELSE l.work_rvu * l.intra_op END DESC) AS seq
FROM
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vop on vop.visit_occurrence_id = po.visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vo on vo.visit_occurrence_id = vop.parent_visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po2 on (po.person_id = po2.person_id
																		    AND
																			po2.procedure_date BETWEEN DATEADD(DAY, 1, po.procedure_date ) AND CONVERT(DATE, vo.parent_visit_end_datetime)
																			AND
																			po2.procedure_concept_id IN (2000000027)) -- custom operation
	LEFT JOIN DaTa_ScHeMa.procedure_occurrence po3 on (po2.person_id = po3.person_id AND po3.procedure_date = po2.procedure_date AND po3.procedure_type_concept_id = 32817)
	LEFT JOIN LoOkUp_ScHeMa.PHYSICIAN_RVU_Lookup l on (l.omop_concept_id = po3.procedure_source_concept_id
														AND
														po3.procedure_date BETWEEN l.[start_date] AND l.[end_date])

WHERE
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY)
SELECT
	*
FROM
	partitioned
WHERE
	seq = 1;
ELSE
with partitioned as (
SELECT
	subject_id,
	po2.person_id,
	po2.visit_occurrence_id,
	po2.visit_detail_id,
	'surgery' variable_name,
	po2.procedure_datetime [procedure_datetime],
	po2.procedure_end_datetime [procedure_end_datetime],
	po3.procedure_source_concept_id [second_or_procedure],
	l.work_rvu * l.intra_op [intraop_rvu],
	CASE WHEN l.intra_op = 0 OR l.intra_op IS NULL THEN 0.755 * l.work_rvu ELSE l.work_rvu * l.intra_op END [intraop_rvu_adjusted],
	ROW_NUMBER() OVER(PARTITION BY vo.visit_occurrence_id ORDER BY po.procedure_datetime ASC, CASE WHEN l.intra_op = 0 OR l.intra_op IS NULL THEN 0.755 * l.work_rvu ELSE l.work_rvu * l.intra_op END DESC) AS seq
FROM
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vo on vo.visit_occurrence_id = po.visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po2 on (po.person_id = po2.person_id
																		    AND
																			po2.procedure_date BETWEEN DATEADD(DAY, 1, po.procedure_date ) AND CONVERT(DATE, vo.visit_end_datetime)
																			AND
																			po2.procedure_concept_id IN (2000000027))
	LEFT JOIN DaTa_ScHeMa.procedure_occurrence po3 on (po.person_id = po3.person_id AND po3.procedure_date = po2.procedure_date)
	LEFT JOIN LoOkUp_ScHeMa.PHYSICIAN_RVU_Lookup l on (l.omop_concept_id = po3.procedure_source_concept_id
																			AND
																			po3.procedure_date BETWEEN l.[start_date] AND l.[end_date])

WHERE
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY)
SELECT
	*
FROM
	partitioned
WHERE
	seq = 1;