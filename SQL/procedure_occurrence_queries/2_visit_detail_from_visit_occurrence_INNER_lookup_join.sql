/******
2_visit_detail_from_visit_occurrence_INNER_lookup_join Query prepared at 2024-03-13 13:32:28.821564 by query_builder.py

retrieves variables from the following row_ids: 54||55
******/
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = 'parent_visit_occurrence_id') 
with partitioned as (
SELECT
	subject_id,
	po.person_id,
	po.visit_occurrence_id,
	po2.procedure_occurrence_id [visit_detail_id],
	po2.procedure_datetime [second_or_start_datetime],
	po2.procedure_end_datetime [second_or_end_datetime],
	ROW_NUMBER() OVER(PARTITION BY vo.visit_occurrence_id ORDER BY po2.procedure_datetime ASC) AS seq
FROM
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.VISIT_OCCURRENCE vop on po.visit_occurrence_id = vop.visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.VISIT_OCCURRENCE vo on vo.visit_occurrence_id = vop.parent_visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po2 on (po.person_id = po2.person_id
																		    AND
																			po2.procedure_date BETWEEN DATEADD(DAY, 1, po.procedure_date ) AND CONVERT(DATE, vo.parent_visit_end_datetime)
																			AND
																			po2.procedure_concept_id IN (2000000027)) -- custom operation
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
	po.person_id,
	po.visit_occurrence_id,
	po2.procedure_occurrence_id [visit_detail_id],
	po2.procedure_datetime [second_or_end_datetime],
	po2.procedure_end_datetime [second_or_start_datetime],
	ROW_NUMBER() OVER(PARTITION BY vo.visit_occurrence_id ORDER BY po2.procedure_datetime ASC) AS seq
FROM
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.VISIT_OCCURRENCE vo on vo.visit_occurrence_id = po.visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po2 on (po.person_id = po2.person_id
																		    AND
																			po2.procedure_date BETWEEN DATEADD(DAY, 1, po.procedure_date ) AND CONVERT(DATE, vo.visit_end_datetime)
																			AND
																			po2.procedure_concept_id IN (2000000027))--, -- custom operation
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