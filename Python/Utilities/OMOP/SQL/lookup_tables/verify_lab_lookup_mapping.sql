SELECT
	A.*,
	c.concept_name,
	c.standard_concept,
	c2.concept_name,
	c2.standard_concept,
	l.variable_name,
	l2.variable_name
FROM
	(SELECT DISTINCT
		measurement_concept_id,
		measurement_source_concept_id
	FROM
		CDM.MEASUREMENT m
		INNER JOIN IC3_Internal.DATABASE_UPDATES du on (du.[file_name] = 'labs' AND m.measurement_id BETWEEN du.min_id AND du.max_id)
	) A
	LEFT JOIN VOCAB.CONCEPT c on A.measurement_concept_id = c.concept_id
	LEFT JOIN VOCAB.CONCEPT c2 on A.measurement_source_concept_id = c2.concept_id
	LEFT JOIN IC3.IC3_Variable_Lookup_Table l on l.concept_id = A.measurement_concept_id
	LEFT JOIN IC3.IC3_Variable_Lookup_Table l2 on l2.concept_id = A.measurement_source_concept_id
ORDER BY
	A.measurement_concept_id ASC
