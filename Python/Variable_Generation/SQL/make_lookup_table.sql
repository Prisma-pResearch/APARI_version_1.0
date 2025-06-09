SELECT
*
FROM
(
SELECT
	concept_id,
	concept_code,
	concept_name,
	vocabulary_id,
	concept_class_id,
	domain_id,
	l.ancestor_concept_id,
	CASE
		WHEN l.variable_name = 'healtchare_facility' THEN 'admitted_from_concept_id'
		WHEN l.variable_name IN ('regional_or_local_anesthesia', 'general_anesthesia') THEN 'anesthesia_type'
		WHEN l.variable_name IN ('charity', 'private') THEN 'payer_concept_id'
		WHEN l.variable_name IN ('current_smoker', 'former_smoker', 'never_smoker') THEN 'smoking_status'
		WHEN l.variable_name IN ('married_significant_partner', 'single', 'divorced_seperated_widowed') THEN 'marital_status'
		WHEN l.variable_name IN ('surgical_admission', 'medical_admission') THEN 'admitting_service'
		WHEN l.variable_name LIKE 'surgery_type_%' THEN 'surgical_service'
		WHEN l.variable_name = 'icu' THEN 'sched_post_op_location'
		WHEN l.variable_name IN ('emergency', 'non-emergency') THEN 'admit_priority'
		ELSE l.variable_name
		END [variable_name],
	CASE
		WHEN l.variable_name ='general_anesthesia' THEN 'GENERAL'
		WHEN l.variable_name = 'regional_or_local_anesthesia' THEN 'LOCAL/REGIONAL'
		WHEN l.variable_name = 'current_smoker' THEN 'CURRENT'
		WHEN l.variable_name = 'former_smoker' THEN 'FORMER'
		WHEN l.variable_name = 'never_smoker' THEN 'NEVER'
		WHEN l.variable_name = 'healtchare_facility' THEN 'TRANSFER'
		WHEN l.variable_name = 'married_significant_partner' THEN 'MARRIED'
		WHEN l.variable_name = 'divorced_seperated_widowed' THEN 'DIVORCED'
		WHEN l.variable_name = 'surgical_admission' THEN 'SURGERY'
		WHEN l.variable_name = 'medical_admission' THEN 'MEDICINE'
		WHEN l.variable_name = 'surgery_type_cardiothoracic' THEN 'CT_SURGERY'
		WHEN l.variable_name = 'surgery_type_gi' THEN 'GASTROINTENSTINAL_SURGERY'
		WHEN l.variable_name = 'icu' THEN 'ICU'
		WHEN l.variable_name LIKE 'surgery_type_%' THEN UPPER(REPLACE(l.variable_name, 'surgery_type_', ''))
		ELSE UPPER(l.variable_name)
		
		END [var_gen_value],
	CASE
		WHEN l.variable_name IN ('regional_or_local_anesthesia', 'general_anesthesia') THEN 'anesthesia_type'
		WHEN l.variable_name IN ('charity', 'private') THEN 'payer'
		WHEN l.variable_name IN ('current_smoker', 'former_smoker', 'never_smoker') THEN 'smoking_status'
		WHEN l.variable_name = 'healtchare_facility' THEN 'admit_source'
		WHEN l.variable_name IN ('married_significant_partner', 'single', 'divorced_seperated_widowed') THEN 'marital_status'
		WHEN l.variable_name IN ('emergency', 'non-emergency') THEN 'emergent'
		WHEN l.variable_name IN ('surgical_admission', 'medical_admission') THEN 'admit_type'
		WHEN l.variable_name LIKE 'surgery_type_%' THEN 'surgery_type'
		WHEN l.variable_name = 'icu' THEN 'postop_loc'
		END [var_gen_name]
FROM
	IC3_Variable_Lookup_Table_v2_beta l
	
WHERE
	l.variable_name IN ( 'regional_or_local_anesthesia', 'general_anesthesia', --anesthesia type
						'charity', 'private', -- payer
						'current_smoker', 'former_smoker', 'never_smoker', -- smoking status
						'healtchare_facility', -- admit source
						'married_significant_partner', 'single', 'divorced_seperated_widowed', -- marital status
						'emergency', 'non-emergency', -- emergent
						'surgical_admission', 'medical_admission', -- admit_type
						'icu' -- postop location
						)
	OR
	l.variable_name LIKE 'surgery_type_%'
UNION
SELECT
	concept_id,
	concept_code,
	concept_name,
	vocabulary_id,
	concept_class_id,
	domain_id,
	NULL ancestor_concept_id,
	CASE
		WHEN concept_id IN (8532, 8507) THEN 'gender_concept_id'
		WHEN concept_id IN (38003563, 38003564) THEN 'ethnicity_concept_id'
		WHEN concept_id IN (38003598, 8516, 8527) THEN 'race_concept_id'
		WHEN concept_id = 4180186 THEN 'language'
		WHEN concept_id IN (280, 289, 436) THEN 'payer_concept_id'
	
	END variable_name,
	CASE
		WHEN concept_id = 8532 THEN 'FEMALE'
		WHEN concept_id = 8507 THEN 'MALE'
		WHEN concept_id = 38003563 THEN 'HISPANIC'
		WHEN concept_id = 38003564 THEN 'NON-HISPANIC'
		WHEN concept_id IN (38003598, 8516, 38003599, 38003600) THEN 'AA'
		WHEN concept_id = 8527 THEN 'WHITE'
		WHEN concept_id = 4180186 THEN 'ENGLISH'
		WHEN concept_id = 280 THEN 'MEDICARE'
		WHEN concept_id = 289 THEN 'MEDICAID'
		WHEN concept_id = 436 THEN 'MEDICAID'
	END [var_gen_value],
	CASE
		WHEN concept_id IN (8532, 8507) THEN 'sex'
		WHEN concept_id IN (38003563, 38003564) THEN 'ethnicity'
		WHEN concept_id IN (38003598, 8516, 8527) THEN 'race'
		WHEN concept_id = 4180186 THEN 'language'
		WHEN concept_id IN (280, 289, 436) THEN 'payer'
		END [var_gen_name]
FROM
	VOCAB.CONCEPT C
WHERE
	concept_id IN (8532, 8507, -- sex
					38003563, 38003564, -- ethnicity
					38003598, 8516, 8527, 38003599, 38003600, -- race
					4180186, -- english language
					280, 289, 436 -- Payer

					)
					) F
ORDER BY var_gen_name, var_gen_value

