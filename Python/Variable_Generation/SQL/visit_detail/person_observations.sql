with partitioned as(
SELECT
	vd.visit_detail_id,
	o.observation_datetime,
	[variable_name],
	o.value_as_concept_id,
	ROW_NUMBER() OVER(PARTITION BY o.visit_occurrence_id, l.variable_name ORDER BY  o.observation_datetime DESC) AS seq
FROM
	DaTa_ScHeMa.OBSERVATION o
	INNER JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table l on (o.observation_concept_id = l.concept_id
															  AND
															  l.variable_name IN ('language', 'marital_status', 'smoking_status'))
	INNER JOIN DaTa_ScHeMa.VISIT_DETAIL vd on (vd.person_id = o.person_id
											   AND
											   vd.visit_detail_id IN (SELECT
																			subject_id
																	  FROM
																		ReSuLtS_ScHeMa.COHORT
																	  WHERE
																		cohort_definition_id = XXXXXX
																		AND
																		subset_id = YYYY)
												)
WHERE
	o.person_id IN (SELECT DISTINCT
						vd.person_id
					FROM
						ReSuLtS_ScHeMa.COHORT c
						INNER JOIN DaTa_ScHeMa.VISIT_DETAIL vd on vd.visit_detail_id = c.subject_id
					WHERE
						c.cohort_definition_id = XXXXXX
						AND
						c.subset_id = YYYY)
	AND
	o.observation_date BETWEEN DATEADD(YEAR, -1, vd.visit_detail_start_datetime) AND vd.visit_detail_start_datetime
	AND
	o.value_as_concept_id <> 0

	)
SELECT 
*
FROM
	partitioned 
WHERE
	seq = 1 -- Ensure only the most recent observation for each type
