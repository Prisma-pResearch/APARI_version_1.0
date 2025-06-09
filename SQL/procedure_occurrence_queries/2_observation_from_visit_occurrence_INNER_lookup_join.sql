/******
2_observation_from_visit_occurrence_INNER_lookup_join Query prepared at 2024-03-13 13:32:28.802563 by query_builder.py

retrieves variables from the following row_ids: 79
******/


IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = 'parent_visit_occurrence_id') 
with partitioned as (
SELECT
		subject_id,
	o.person_id,
	o.visit_occurrence_id,
	o.visit_detail_id,
	'procedure_urgency' variable_name,
	o.observation_datetime [observation_datetime],
	o.value_as_concept_id [second_or_surgery_type],
	ROW_NUMBER() OVER(PARTITION BY vo.visit_occurrence_id ORDER BY po.procedure_datetime ASC) AS seq
FROM
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vop on vop.visit_occurrence_id = po.visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vo on vo.visit_occurrence_id = vop.parent_visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po2 on (po.person_id = po2.person_id
																		    AND
																			po2.procedure_date BETWEEN DATEADD(DAY, 1, po.procedure_date ) AND CONVERT(DATE, vo.parent_visit_end_datetime)
																			AND

																			po2.procedure_concept_id IN (2000000027)) -- Custom Surgery Code
	LEFT JOIN DaTa_ScHeMa.observation o on (o.person_id=po2.person_id


																 AND
																 po2.procedure_date = o.observation_date
																 AND
																 o.observation_concept_id IN (SELECT
																								concept_id
																							  FROM
																								LoOkUp_ScHeMa.IC3_Variable_Lookup_Table
																							  WHERE
																								variable_name IN ('procedure_urgency')))

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
	o.person_id,
	o.visit_occurrence_id,
	o.visit_detail_id,
	'procedure_urgency' variable_name,
	o.observation_datetime [observation_datetime],
	o.value_as_concept_id [second_or_surgery_type],
	ROW_NUMBER() OVER(PARTITION BY vo.visit_occurrence_id ORDER BY po.procedure_datetime ASC) AS seq
FROM
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vo on vo.visit_occurrence_id = po.visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po2 on (po.person_id = po2.person_id
																		    AND
																			po2.procedure_date BETWEEN DATEADD(DAY, 1, po.procedure_date ) AND CONVERT(DATE, vo.visit_end_datetime)
																			AND
																			po2.procedure_concept_id IN (2000000027))

	INNER JOIN DaTa_ScHeMa.observation o on (o.person_id=po2.person_id

																 AND
																 po2.procedure_date = o.observation_date
																 AND
																 o.observation_concept_id IN (SELECT
																								concept_id
																							  FROM
																								LoOkUp_ScHeMa.IC3_Variable_Lookup_Table
																							  WHERE
																								variable_name IN ('procedure_urgency')))

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

