/****** Script for SelectTopNRows command from SSMS  ******/
SELECT DISTINCT
	med_display_name,
	rxnorm_concat,
	med_dose_unit_desc,
	ds.ingredient_concept_id,
	variable_name
FROM
	[IDEALIST].[PRISMAP].[unique_meds_linked_by_rxnorms] u
	LEFT JOIN IDEALIST.VOCAB.DRUG_STRENGTH ds on ds.drug_concept_id = u.standard_drug_id
	LEFT JOIN IDEALIST.VOCAB.CONCEPT con on con.concept_id = u.standard_concept
	INNER JOIN dbo.IC3_Variable_Lookup_Table l on (l.concept_id = COALESCE(ds.ingredient_concept_id, u.standard_drug_id)
												   AND
												   l.domain = 'Drug')
ORDER BY
	variable_name