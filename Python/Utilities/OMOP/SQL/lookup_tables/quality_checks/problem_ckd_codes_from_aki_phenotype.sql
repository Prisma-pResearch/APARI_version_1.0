with partitioned as (
SELECT
	C.concept_code,
	C.concept_name [source_concept_name],
	Cr.relationship_id,
	C2.concept_name [standard_concept_name],
	C2.vocabulary_id,
	variable_name,
	ROW_NUMBER() OVER(PARTITION BY C.concept_id ORDER BY  variable_name DESC) AS seq
FROM
	VOCAB.CONCEPT C
	LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP cr on (C.concept_id = CR.concept_id_1 AND CR.relationship_id IN ('Maps to', 'Is a'))
	LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR2 on (CR2.concept_id_1 = CR.concept_id_2 AND CR.relationship_id = 'IS a' AND CR2.relationship_id = 'Maps To')
	LEFT JOIN VOCAB.CONCEPT C2 on C2.concept_id = CASE WHEN CR.relationship_id = 'Maps to' THEN CR.concept_id_2 ELSE CR2.concept_id_2 END
	LEFT JOIN IC3_Variable_Lookup_Table_v2_beta L on L.concept_id = C2.concept_id
WHERE
	C.vocabulary_id IN ('ICD10', 'ICD10CM', 'ICD9', 'ICD9CM')
	AND
	C.concept_code IN ('N01.8', 'N01.9', 'N01.4', 'N01.5', 'N01.6', 'N01.7', 'N01.0', 'N01.1', 'N01.2',
           'N01.3', 'E08.2', '582.89', 'E13.2', 'Q61.9', '582.81', 'N07.8', 'N07.9', 'N07.2',
           'N07.3', 'N07.0', 'N07.1', 'N07.6', 'N07.7', 'N07.4', 'N07.5', 'E08.21', 'E08.22', 'Q61.8', 'E08.29',
           'N05.8', 'N05.9', 'N05.0', 'N05.1', 'N05.2', 'N05.3', 'N05.4', 'N05.5', 'N05.6', 'N05.7', 'N03.8', 'N03.9',
           'N03.6', 'N03.7', 'N03.4', 'N03.5', 'N03.2', 'N03.3', 'N03.0', 'N03.1', '586', '403.10', '403.11', 'I13.10',
           'I13.11', '581.3', '581.2', '581.1', '581.0', '403.01', '581.9', '581.8', '403.00', 'E11.29', 'N25.89',
           'E11.22', 'E11.21', '583.6', 'N25.81', 'N05', 'N04', 'N07', 'N06', 'N01', 'N03', '583.1', '583.0', '585',
           '582','583.4', '583.7', '581', '583.9', '583.8', 'Q61.3', 'Q61.2', 'Q61.5', 'Q61.4', 'I12.9', '588.8',
           '588.9', 'I12.0', 'N28.0', '585.9', '585.4', '585.3', '585.2', '585.1', '403.91', 'N18.9', '250.43',
           '250.42', '250.41', '250.40', '404.00', '404.01', '404.02', '404.03', 'N18.2', 'N28', '404.11', '582.4',
           '582.5','N18.1', '582.0', '582.1', '582.2', '582.8', '582.9', 'E10.21', 'E10.22', '583.81', '404.13',
           '404.12', '588.89', '404.10', 'E10.29', '583.89', '588.81', '581.89', 'I13.2', '581.81', 'I13.0', 'I13.1',
           'N25', '404.93', '404.92', '404.91', '404.90', 'E09.29', 'E09.22', 'N18.3', 'E09.21', 'N18.4', 'N18.5',
           'E09.2', 'E11.2', 'N06.7', 'N06.6', 'N06.5', 'N06.4', 'N06.3', 'N06.2', 'N06.1', 'N06.0', 'N02.9', 'N06.9',
           'N06.8', 'N04.5', 'N04.4', 'N04.7', 'N04.6', 'N04.1', 'N04.0', 'N04.3', 'N04.2', '753.13', 'E13.22',
           'N04.9', 'N04.8', 'E13.29', '583.2', 'E10.2', 'Q61', 'E13.21', '583', 'N02.8', '403.90',
           'N25.8', 'N25.9', 'N02.3', 'N02.2', 'N02.1', 'N02.0', 'N02.7', 'N02.6', 'N02.5', 'N02.4')
	AND
	 variable_name IS NULL
	)
	SELECT * FROM partitioned WHERE seq = 1 ORDER BY concept_code ASC;