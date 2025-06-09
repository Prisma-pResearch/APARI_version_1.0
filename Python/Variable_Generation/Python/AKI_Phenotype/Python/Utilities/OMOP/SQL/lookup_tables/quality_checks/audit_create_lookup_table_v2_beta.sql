

with partitioned as (
SELECT
	l.*,
	ROW_NUMBER() OVER(PARTITION BY concept_id ORDER BY  domain_id ASC) AS seq
FROM
	IC3_Variable_Lookup_Table_v2_beta l
WHERE
	variable_name <> 'inpatient_hospital_encounter'
)
SELECT
	l.*,
	C.concept_name
FROM
	IC3_Variable_Lookup_Table_v2_beta l
	LEFT JOIN VOCAB.CONCEPT C on C.concept_id = ancestor_concept_id
WHERE
	l.concept_id IN (
SELECT
	concept_id
FROM
	partitioned
WHERE
	seq = 2)
ORDER BY
	l.concept_id ASC