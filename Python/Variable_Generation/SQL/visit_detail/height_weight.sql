with partitioned as(
SELECT
	m.person_id,
	CASE WHEN l.variable_name = 'height' THEN 'height_cm'
	ELSE 'weight_kgs' END [variable_name],
	m.measurement_datetime,
	CASE WHEN m.unit_concept_id = 8739 THEN m.value_as_number / 2.205 -- convert pounds to kg
		WHEN  m.unit_concept_id = 9330 THEN m.value_as_number / 2.54 -- convert inches to cm
		ELSE m.value_as_number END [value_as_number],
	ROW_NUMBER() OVER(PARTITION BY m.person_id, l.variable_name ORDER BY  m.measurement_datetime DESC) AS seq
FROM
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.VISIT_DETAIL vd on vd.visit_detail_id = c.subject_id
	INNER JOIN DaTa_ScHeMa.MEASUREMENT m on m.person_id = vd.person_id
	INNER JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table l on (m.measurement_concept_id = l.concept_id
										  AND
										  l.variable_name IN ('height', 'weight'))
WHERE
	c.cohort_definition_id = XXXXXX
	AND
	c.subset_id = YYYY
	AND
	m.measurement_date BETWEEN DATEADD(YEAR, -1, c.cohort_start_date) AND c.cohort_start_date
	AND
	m.unit_concept_id IN (9529, 8582, 8739, 9330)
)
SELECT 
person_id,
variable_name,
measurement_datetime,
value_as_number
FROM
	partitioned 
WHERE
	seq = 1 -- Ensure only the last observation of each type