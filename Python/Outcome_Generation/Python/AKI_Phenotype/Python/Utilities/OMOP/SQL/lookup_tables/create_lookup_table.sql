DROP TABLE IF EXISTS dbo.IC3_Variable_Lookup_Table_v3_beta;
GO
CREATE TABLE dbo.IC3_Variable_Lookup_Table_v3_beta(
	[concept_id] [int] NOT NULL,
	[concept_code] [varchar](50) NOT NULL,
	[concept_name] [varchar](255) NOT NULL,
	[vocabulary_id] [varchar](20) NOT NULL,
	[concept_class_id] [varchar](20) NOT NULL,
	[domain_id] [varchar](20) NOT NULL,
	[ancestor_concept_id] [int] NULL,
	[variable_name] [varchar](100) NOT NULL,
	[variable_desc] [varchar](255) NULL,
 CONSTRAINT [id_var_IC3_Variable_Lookup_Table_v3_beta] UNIQUE NONCLUSTERED 
(
	[concept_id] ASC,
	[variable_name] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	NULL [ancestor_concept_id],
	CASE 
		WHEN (concept_name LIKE '%implant%' OR concept_name LIKE '%insert%' OR concept_name IN ('Drug Delivery Device')) THEN 'implant'
		WHEN (concept_name LIKE '%topical%' OR  concept_name IN ('Cement', 'Cream', 'Dermal Spray', 'Shampoo', 'Shower Powder', 'Dressing Gauze',
		'Medicated Bar Soap', 'Medicated Cottonball', 'Medicated Gauzeball', 'Medicated Guaze', 'Medicated Liquid Soap', 'Medicated nail lacquer',
		'Medicated Nail Polish', 'Medicated Pad', 'Medicated Shampoo', 'Medicated Tape')) THEN 'topical'
		WHEN (concept_name IN ('Rectal Cream', 'Rectal Creame', 'Rectal Foam', 'Rectal Gel', 'Rectal Ointment', 'Rectal Powder', 'Rectal Solution',
				'Rectal solution spray', 'Rectal Spray', 'Rectal Suppository', 'Rectal Suspension', 'Rectal suspension spray', 'Powder for rectal solution')) THEN 'rectal'
			WHEN (concept_name LIKE '%vagi%' OR concept_name IN ('Douche')) THEN 'vaginal'
			WHEN concept_name LIKE '%nasal%' THEN 'nasal'
			WHEN (concept_name LIKE '%oral%' OR
					concept_name IN ('Caplet', 'Extended Release Enteric Coated Capsule', 'Extended Release Enteric Coated Tablet', 'Pill', 'Pharmaceutical dose form',
								     '12 hour Extended Release Capsule', '12 hour Extended Release Tablet', '24 Hour Extended Release Capsule', 'Disintegrating Tablet',
								     '24 Hour Extended Release Tablet', 'Capsule', 'Chewable capsule', 'Chewable Tablet', 'Tablet', 'Syrup', 'Suppository', 'Rectal Suppository',
								    'Pudding', 'Enteral dose form', 'Enteric Coated Capsule', 'Enteric Coated Tablet', 'Extended Release Capsule', 'Extended Release Tablet',
								    'Chewable Bar', 'Effervescent granules', 'Effervescent powder', 'Effervescent tablet')) THEN 'enteric'
			WHEN (concept_name LIKE '%Sublingual%' OR  concept_name LIKE '%Oromucosal%' OR
			 concept_name IN ('Buccal Tablet', 'Buccal dose form', 'Buccal Film', 'Buccal lozenge', 'Chewing Gum', 'Conventional release buccal spray',
			 'Sustained Release Buccal Tablet', 'Lozenge', 'Gingival dose form', 'Gingival ointment', 'Mucosal Spray', 'Wafer')
			 OR concept_name LIKE '%dental%')THEN 'oral_mucosa'
			WHEN (concept_name LIKE '%otic%' OR concept_name LIKE '%Ophthalmic%' OR concept_name IN ('Eye drops emulsion', 'Eye emulsion',
			'Ocular solution for irrigation', 'Solution for intraocular injection', 'Tablet for Opthalmic Solution', 'Intraocular dose form')) THEN 'otic'
			WHEN (concept_name LIKE '%patch%' OR concept_name LIKE '%transdermal%') THEN 'transdermal'
			WHEN (concept_name LIKE '%aerosol%' OR concept_name LIKE '%inhaler%' OR concept_name LIKE '%Inhalation%' OR concept_name LIKE '%Inhalant%'
			OR concept_name IN ('Powder for suspension for inhalation')) THEN 'aerosol'
			WHEN (concept_name LIKE '%Cutaneous%' OR concept_name IN ('Soft Tissue Injection Suspension', 'Pen Injector')) THEN 'cutaneous'
			WHEN concept_name IN ('Suspension for infusion', 'Powder for suspension for infusion', 'Intravenous Solution', 'Emulsion for infusion',
								  'Intravenous Suspension', 'Intravesical dose form', 'Injectable Solution', 'Injectable Suspension', 'Injection', 'Powder for solution for infusion') THEN 'infusion'
			WHEN (concept_name LIKE '%intramuscular%' OR concept_name IN ('Auto-Injector')) THEN 'intramuscular'
			WHEN concept_name LIKE '%Oropharyngeal%' THEN 'oropharyngeal'
			WHEN concept_name IN ('Endotracheopulmonary dose form') THEN 'endotracheopulmonary'
			WHEN concept_name IN ('Enema') THEN 'Enema'
			WHEN concept_name IN ('Prefilled Syringe') THEN 'plausable_infusion'
			WHEN concept_name IN ('Solution', 'Suspension', 'Spray') THEN 'ambigous'
			ELSE 'misc_dose_form' END [variable_name],
			'Dose Form' [variable_desc]
--INTO IC3_Variable_Lookup_Table_v3_beta
FROM VOCAB.CONCEPT where concept_class_id = 'DOSE FORM'

GO




INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	NULL [ancestor_concept_id],
CASE
	WHEN concept_name IN ('Percutaneous', 'Interstitial route', 'Percutaneous route', 'Subcutaneous route', 'Intradermal route', 'Subdermal') THEN 'subcutaneous'
	WHEN concept_name IN ('Subretinal route', 'Ophthalmic route', 'Intracorneal route', 'Otic route', 'Conjunctival route', 'Ocular route', 'Intraocular route', 'Subconjunctival route', 'Subretinal') THEN 'otic'
	WHEN concept_name IN ('PEG tube route', 'Gastroenteral route', 'Oral route', 'Nasogastric route', 'Jejunostomy route', 'Digestive tract route', 'Intestinal route', 'Intrajejunal route', 'Intraileal route', 'Enteral route') THEN 'enteric'
	WHEN concept_name IN ('Buccal route', 'Oromucosal route', 'Sublingual route', 'Subgingival route', 'Gingival route', 'Translingual') THEN 'oral_mucosa'
	WHEN concept_name IN ('Endotracheopulmonary route') THEN 'endotracheopulmonary'
	WHEN concept_name IN ('Rectal route') THEN 'rectal'
	WHEN concept_name IN ('Intramuscular route') THEN 'intramuscular'
	WHEN concept_name IN ('Vaginal route') THEN 'vaginal'
	WHEN concept_name IN ('Cutaneous route') THEN 'cutaneous'
	WHEN concept_name IN ('Intra-arterial route', 'Intramedullary route', 'Intravenous central route', 'Intravenous route', 'Intravenous peripheral route', 'Intraosseous route') THEN 'infusion'
	WHEN concept_name IN ('Transdermal route') THEN 'transdermal'
	WHEN concept_name IN ('Nasal route') THEN 'nasal'
	WHEN concept_name IN ('Topical route') THEN 'topical'
	WHEN concept_name IN ('Inhalation') THEN 'aerosol'
	ELSE concept_name END [variable_name],
	'Drug Route' [variable_desc]

FROM
	VOCAB.CONCEPT C
WHERE
	C.domain_id = 'ROUTE'
	AND
	standard_concept = 'S'

GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	NULL [ancestor_concept_id],
	concept_name [variable_name],
	'Drug Route' [variable_desc]
FROM
	VOCAB.CONCEPT C
WHERE
	C.domain_id = 'ROUTE'
	AND
	standard_concept = 'S'
	AND
	C.concept_name NOT IN (SELECT variable_name FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE domain_id = 'ROUTE')
GO


INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	NULL [ancestor_concept_id],
	'language' as variable_name,
	'Patient Language' [variable_desc]
FROM 
	VOCAB.CONCEPT c
WHERE
	c.concept_id IN (
		4267143, -- language
		4182347, -- world language
		4052785  -- Language Spoken
	)
GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    'visit_detail' [domain_id],
	[ancestor_concept_id],
	'icu' as variable_name,
	'Intensive Care Unit Location Type' [variable_desc]
FROM 
	VOCAB.CONCEPT c
	INNER JOIN VOCAB.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
WHERE
	standard_concept = 'S'
	AND
	ca.ancestor_concept_id IN (
		32037, -- Intensive Care
								-- 581379, -- Inpatient Critical Care Facility
								4148981  -- Intesive care unit
	)
	AND
	c.concept_name NOT LIKE '%30%'
	AND
	c.concept_name NOT LIKE '%Telemetry%'
GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	'inpatient_hospital_encounter' as variable_name,
	'Inpatient Hospital Encounter' [variable_desc]
FROM 
	VOCAB.CONCEPT c
	INNER JOIN VOCAB.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
WHERE
	standard_concept = 'S'
	AND
	ca.ancestor_concept_id IN (
		-- 38004515, -- Hospital
		8717  -- Inpatient Hospital
	)
	AND
	c.concept_name NOT LIKE '%nonmedical%'
	AND
	c.concept_name NOT LIKE '%long term%'
	AND
	c.concept_name NOT LIKE '%rehab%'
	AND
	c.concept_name NOT LIKE '%psych%'
	AND
	c.concept_name NOT LIKE '%swing%'
GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	'cv_cardiac_arrest' as variable_name,
	NULL [variable_desc]
FROM 
	VOCAB.CONCEPT c
	INNER JOIN VOCAB.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
WHERE
	standard_concept = 'S'
	AND
	ca.ancestor_concept_id IN (
		321042 -- SNOMED Cardiac Arrest
	)
GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	'cv_hypo_no_shock' as variable_name,
	NULL [variable_desc]
FROM 
	VOCAB.CONCEPT c
	INNER JOIN VOCAB.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
WHERE
	standard_concept = 'S'
	AND
	ca.ancestor_concept_id IN (
		317002 -- SNOMED Low Blood Pressure
	)
GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	'cv_shock' as variable_name,
	NULL [variable_desc]
FROM 
	VOCAB.CONCEPT c
	INNER JOIN VOCAB.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
WHERE
	standard_concept = 'S'
	AND
	ca.ancestor_concept_id IN (
		201965 -- SNOMED SHOCK
	)
GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	NULL [ancestor_concept_id],
	CASE WHEN concept_id IN (21498112, 21498855,21498947 ) THEN 'emergency' ELSE 'non-emergency' END variable_name,
	NULL [variable_desc]
FROM 
	VOCAB.CONCEPT c
WHERE
	concept_id IN (
					21498112, --	Urgent	LOINC
					21498469, --	Elective	LOINC
					21498607, --	Non-urgent Trauma	LOINC
					21498751, --	Information not available	LOINC
					21498855, --	Emergent	LOINC
					21498947, --	Emergent Trauma	LOINC
					45885220 --	Newborn	LOINC
					)
GO




INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	NULL [ancestor_concept_id],
	'procedure_urgency' as variable_name,
	NULL [variable_desc]
FROM 
	VOCAB.CONCEPT c
WHERE
	concept_id = 21491678 -- LOINC Procedure urgency
GO





INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	'cv_hf' as variable_name,
	NULL [variable_desc]
FROM 
	VOCAB.CONCEPT c
	INNER JOIN VOCAB.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
WHERE
	standard_concept = 'S'
	AND
	ca.ancestor_concept_id IN (
		316139 -- SNOMED Heart Failure
	)
GO

 
with partitioned as(
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	CASE WHEN c.concept_id IN (SELECT descendant_concept_id FROM VOCAB.CONCEPT_ANCESTOR WHERE ancestor_concept_id = 381316) THEN 'neuro_stroke'
	WHEN c.concept_id IN (SELECT DISTINCT descendant_concept_id FROM VOCAB.CONCEPT_ANCESTOR WHERE ancestor_concept_id IN (374022, 192606, 374377)) THEN 'neuro_plegia_paralytic'
	WHEN c.concept_id IN (SELECT descendant_concept_id FROM VOCAB.CONCEPT_ANCESTOR WHERE ancestor_concept_id = 373995) THEN 'delirium_icd'
	ELSE 'neuro_other' END [variable_name],
	NULL [variable_desc],
	ROW_NUMBER() OVER(PARTITION BY concept_id, CASE WHEN c.concept_id IN (SELECT descendant_concept_id FROM VOCAB.CONCEPT_ANCESTOR WHERE ancestor_concept_id = 381316) THEN 'neuro_stroke'
	WHEN c.concept_id IN (SELECT DISTINCT descendant_concept_id FROM VOCAB.CONCEPT_ANCESTOR WHERE ancestor_concept_id IN (374022, 192606, 374377)) THEN 'neuro_plegia_paralytic'
	WHEN c.concept_id IN (SELECT descendant_concept_id FROM VOCAB.CONCEPT_ANCESTOR WHERE ancestor_concept_id = 373995) THEN 'delirium_icd'
	ELSE 'neuro_other' END ORDER BY  domain_id ASC) AS seq
FROM 
	VOCAB.CONCEPT c
	INNER JOIN VOCAB.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
WHERE
	standard_concept = 'S'
	AND
	ca.ancestor_concept_id IN (37110545, -- DISORDER of NS following procedure
								374022, 192606, 374377, -- Plegias
								381316, -- Stroke
								4043738, -- Hydrocephalus
								440424, -- Aphasia
								441594, -- Dysphasia
								373995 -- Delirium
								)
	AND
	c.concept_name NOT LIKE '%vaccination%'
	AND
	c.concept_name NOT LIKE '%immunization%'
	AND
	c.concept_name NOT LIKE '% palsy%'
	AND
	c.concept_name NOT LIKE '%autosomal%'
	AND
	c.concept_name NOT LIKE '%x-linked%'
	AND
	c.concept_name NOT LIKE '%juvenile%'
)
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	variable_name,
	[variable_desc]
FROM
	partitioned 
WHERE
	seq = 1 -- remove duplicate key value pairs
GO

with partitioned as(
	SELECT 
		[concept_id],
		[concept_code],
		[concept_name],
		[vocabulary_id],
		concept_class_id,
		[domain_id],
		[ancestor_concept_id],
		CASE WHEN c.concept_id IN (SELECT descendant_concept_id FROM VOCAB.CONCEPT_ANCESTOR WHERE ancestor_concept_id = 437474) THEN 'surg_infection'
		WHEN c.concept_id IN (SELECT DISTINCT descendant_concept_id FROM VOCAB.CONCEPT_ANCESTOR WHERE ancestor_concept_id IN (442018)) THEN 'proc_graft_implant_foreign_body'
		WHEN c.concept_id IN (SELECT descendant_concept_id FROM VOCAB.CONCEPT_ANCESTOR WHERE ancestor_concept_id IN (4002836, 37116361)) THEN 'proc_hemorrhage_hematoma_seroma'
		WHEN c.concept_id IN (SELECT descendant_concept_id FROM VOCAB.CONCEPT_ANCESTOR WHERE ancestor_concept_id = 136580) THEN 'mech_wound'
		ELSE 'proc_non_hemorrhagic_technical' END [variable_name],
		ROW_NUMBER() OVER(PARTITION BY concept_id, CASE WHEN c.concept_id IN (SELECT descendant_concept_id FROM VOCAB.CONCEPT_ANCESTOR WHERE ancestor_concept_id = 437474) THEN 'surg_infection'
		WHEN c.concept_id IN (SELECT DISTINCT descendant_concept_id FROM VOCAB.CONCEPT_ANCESTOR WHERE ancestor_concept_id IN (442018)) THEN 'proc_graft_implant_foreign_body'
		WHEN c.concept_id IN (SELECT descendant_concept_id FROM VOCAB.CONCEPT_ANCESTOR WHERE ancestor_concept_id IN (4002836, 37116361)) THEN 'proc_hemorrhage_hematoma_seroma'
		WHEN c.concept_id IN (SELECT descendant_concept_id FROM VOCAB.CONCEPT_ANCESTOR WHERE ancestor_concept_id = 136580) THEN 'mech_wound'
		ELSE 'proc_non_hemorrhagic_technical' END ORDER BY  domain_id ASC) AS seq,
	NULL [variable_desc]
	FROM 
		VOCAB.CONCEPT c
		INNER JOIN VOCAB.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
	WHERE
		standard_concept = 'S'
		AND
		ca.ancestor_concept_id IN (37116361, --Accidental wound during procedure
									4300243, -- POSTOP COmplication
									36712819, -- Post Procedure abcess
									136580 -- SNOMED Dehiscence of surgical wound 
									)
)
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	variable_name,
	[variable_desc]
FROM
	partitioned 
WHERE
	seq = 1 -- remove duplicate key value pairs
GO



INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	'sepsis' as variable_name,
	NULL [variable_desc]
FROM 
	VOCAB.CONCEPT c
	INNER JOIN VOCAB.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
WHERE
	standard_concept = 'S'
	AND
	ca.ancestor_concept_id IN (
		132797 -- SEPSIS
	)
	AND
	c.concept_name NOT LIKE '%chronic%'

GO

with partitioned as(
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	CASE WHEN c.concept_id IN (SELECT descendant_concept_id FROM VOCAB.CONCEPT_ANCESTOR WHERE ancestor_concept_id = 440417) THEN 'vte_pe'
	ELSE 'vte_deep_super_vein' END [variable_name],
	ROW_NUMBER() OVER(PARTITION BY concept_id, CASE WHEN c.concept_id IN (SELECT descendant_concept_id FROM VOCAB.CONCEPT_ANCESTOR WHERE ancestor_concept_id = 440417) THEN 'vte_pe'
	ELSE 'vte_deep_super_vein' END ORDER BY  domain_id ASC) AS seq,
	NULL [variable_desc]
FROM 
	VOCAB.CONCEPT c
	INNER JOIN VOCAB.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
WHERE
	standard_concept = 'S'
	AND
	ca.ancestor_concept_id IN (440417, -- PE
								4327889, -- VTE
								4133004, -- DVT
								318775 -- Venous Embolism
								)
)
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	variable_name,
	[variable_desc]
FROM
	partitioned 
WHERE
	seq = 1 -- remove duplicate key value pairs
GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	'ckd' [variable_name],
	NULL [variable_desc]
FROM 
	VOCAB.CONCEPT c
	INNER JOIN VOCAB.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
WHERE
	standard_concept = 'S'
	AND
	ca.ancestor_concept_id IN (46271022) -- CKD
	
GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	'aki' [variable_name],
	NULL [variable_desc]
FROM 
	VOCAB.CONCEPT c
	INNER JOIN VOCAB.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
WHERE
	standard_concept = 'S'
	AND
	ca.ancestor_concept_id IN (197320, -- Acute renal failure syndrome SNOMED
								36716946)  -- Acute renal insufficiency SNOMED
								
GO


INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	'esrd' [variable_name],
	NULL [variable_desc]
FROM 
	VOCAB.CONCEPT c
	INNER JOIN VOCAB.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
WHERE
	standard_concept = 'S'
	AND
	ca.ancestor_concept_id IN (193782, -- End-stage renal disease SNOMED
							   443611) -- CKD Stage 5 == ESRD
GO


INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	'smoking_status' as variable_name,
	NULL [variable_desc]
FROM 
	VOCAB.CONCEPT c
	INNER JOIN VOCAB.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
WHERE
	standard_concept = 'S'
	AND
	ca.ancestor_concept_id IN (
		43054909, -- Tobacco Smoking status LOINC
		35811013, -- UK biobank smoking status (check that levels exist in lookup table)
		40766362, -- Tobacco smoking status LOINC findings (check that levels exist in lookup table)
		4275495  -- tobacco smoking behavior snomed (These decendents are the levels themselves not the question about smoking status, this will need to be mapped in the lookup table)
	)
UNION
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	c.concept_id [ancestor_concept_id],
	'smoking_status' as variable_name,
	NULL [variable_desc]
FROM 
	VOCAB.CONCEPT c
WHERE
	c.concept_id = 4144271 -- SNOMED Tobacco smoking consumption (used by UMinn)
GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	NULL [ancestor_concept_id],
	'physician_specialty' as variable_name,
	NULL [variable_desc]
FROM 
	VOCAB.CONCEPT c
WHERE
	concept_class_id = 'Physician Specialty' AND standard_concept = 'S'
GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	'healtchare_service' as variable_name,
	NULL [variable_desc]
FROM 
	VOCAB.CONCEPT c
	INNER JOIN VOCAB.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
WHERE
	standard_concept = 'S'
	AND
	ancestor_concept_id = 4074187

GO


INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	'mechanical_ventilation' as variable_name,
	NULL [variable_desc]
FROM 
	VOCAB.CONCEPT c
	INNER JOIN VOCAB.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
WHERE
	standard_concept = 'S'
	AND
	ca.ancestor_concept_id IN (
		40493026, -- SNOMED MECH VENT
		45768197 -- SNOMED Ventilator
	)

GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	NULL [ancestor_concept_id],
	'cam' as variable_name,
	NULL [variable_desc]
FROM 
	VOCAB.CONCEPT c
WHERE
	c.concept_id IN (
		42539072,
		44807161
	)
GO

with partitioned as (
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	'dialysis' as variable_name,
	ROW_NUMBER() OVER(PARTITION BY concept_id ORDER BY  domain_id ASC) AS seq,
	NULL [variable_desc]
FROM 
	VOCAB.CONCEPT c
	INNER JOIN VOCAB.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
WHERE
	standard_concept = 'S'
	AND
	(
		(ca.ancestor_concept_id IN (
			4032243,  -- dialysis procedure snomed
			4265605, -- diaysis catheter
			4021071) -- dialysis fluid
			
		AND NOT
		(c.concept_name LIKE '%Extracorporeal%'
		 AND
		 c.concept_name NOT LIKE '%dialysis%'
		 AND
		 c.concept_name NOT LIKE '%hemofiltration%')
		 )
		 OR
		 ca.ancestor_concept_id IN (4269440, -- Dialysis Observable
									4019967, -- Dependence on renal dialysis
									43021247, -- Complication associated with dialysis catheter
									46270032, -- Non-compliance with renal dialysis
									443611, -- CKD Stage V
									4090651) -- Dialysis finding
	 )
)
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	variable_name,
	[variable_desc]
FROM
	partitioned 
WHERE
	seq = 1 -- remove duplicate key value pairs
GO



with partitioned as (
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	'renal_transplant' as variable_name,
	ROW_NUMBER() OVER(PARTITION BY concept_id ORDER BY  domain_id ASC) AS seq,
	NULL [variable_desc]
FROM 
	VOCAB.CONCEPT c
	INNER JOIN VOCAB.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
WHERE
	standard_concept = 'S'
	AND
	ca.ancestor_concept_id IN (
		4163566, -- Renal replacement SNOMED
		45887600, -- Renal allotransplantation, implantation of graft CPT4
		42539502 -- Transplanted kidney present
	)
)
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	variable_name,
	[variable_desc]
FROM
	partitioned 
WHERE
	seq = 1 -- remove duplicate key value pairs
GO



INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	NULL [ancestor_concept_id],
	'marital_status' as variable_name,
	NULL [variable_desc]
FROM 
	VOCAB.CONCEPT c
WHERE
	c.concept_id IN (
		4053609, --	125680007	Marital status	SNOMED
		1585891, --	TheBasics_MaritalStatus	The Basics: Marital Status	PPI
		40766231, --	63503-7	Marital status [NHANES]	LOINC
		3046344, --	45404-1	Marital status	LOINC
		4083596, --	184116008	Patient marital status unknown	SNOMED
		3018063, --	11381-1	Marital status and living arrangements - Reported	LOINC
		1332833, --	basics_8	What is your current marital status?	PPI
		3012846, --	11380-3	Marital status and living arrangements Narrative - Reported	LOINC
		4076095, --	224083004	Marital or partnership status	SNOMED
		4267504 --	365581002	Finding of marital or partnership status	SNOMED

	)

GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	NULL [ancestor_concept_id],
	'married_significant_partner' as variable_name,
	NULL [variable_desc]
FROM 
	VOCAB.CONCEPT c
WHERE
	c.concept_id IN (
		45883710, --	Living with partner	LOINC
		45876756, --	Married	LOINC
		45876756, --	Married	LOINC
		21499180, --	Domestic partner	LOINC
		45876756, --	Married	LOINC
		1332916, --	Living with partner	PPI
		1332845, --	Married	PPI
		4030401, --	Common law partnership	SNOMED
		4325710, --	Domestic partnership	SNOMED
		4116182, --	Eloped	SNOMED
		4204399, --	Engaged to be married	SNOMED
		4019840, --	Homosexual marriage	SNOMED
		4019841, --	Homosexual marriage, female	SNOMED
		4022646, --	Homosexual marriage, male	SNOMED
		4278461, --	Legally married	SNOMED
		4338692, --	Married	SNOMED
		44791567, --	Married/civil partner	SNOMED
		4132774, --	Newly wed	SNOMED
		4150598, --	Remarried	SNOMED
		4030401, --	Common law partnership	SNOMED
		4325710, --	Domestic partnership	SNOMED
		4204399, --	Engaged to be married	SNOMED
		4338692, --	Married	SNOMED
		44791567 --	Married/civil partner	SNOMED

	)

GO


INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	NULL [ancestor_concept_id],
	'single' as variable_name,
	NULL [variable_desc]
FROM 
	VOCAB.CONCEPT c
WHERE
	c.concept_id IN (
		45881671, --	Never married	LOINC
		1620470, --	Cohabitating	LOINC
		45881671, --	Never married	LOINC
		45879879, --	Single	LOINC
		45881671, --	Never married	LOINC
		21499178, --	Unmarried	LOINC
		1332908, --	Never married	PPI
		4185851, --	Bachelor	SNOMED
		4022641, --	Broken engagement	SNOMED
		4145800, --	Broken with partner	SNOMED
		4172700, --	Cohabitee left home	SNOMED
		4242253, --	Cohabiting	SNOMED
		43021238, --	Purposely unmarried and sexually abstinent	SNOMED
		4172698, --	Separated from cohabitee	SNOMED
		4053842, --	Single person	SNOMED
		4053854, --	Single, never married	SNOMED
		4270893, --	Spinster	SNOMED
		4053842 --	Single person	SNOMED
	)

GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	NULL [ancestor_concept_id],
	'divorced_seperated_widowed' as variable_name,
	NULL [variable_desc]
FROM 
	VOCAB.CONCEPT c
WHERE
	c.concept_id IN (
		45883375, --	Divorced	LOINC
		45884459, --	Separated	LOINC
		45883711, --	Widowed	LOINC
		45883375, --	Divorced	LOINC
		45884459, --	Separated	LOINC
		1620675, --	Separated or divorced	LOINC
		45883711, --	Widowed	LOINC
		21499179, --	Annulled	LOINC
		45883375, --	Divorced	LOINC
		21498731, --	Legally separated	LOINC
		45883711, --	Widowed	LOINC
		1332924, --	Divorced	PPI
		1332942, --	Separated	PPI
		4069297, --	Divorced	SNOMED
		44791569, --	Divorced/person whose civil partnership has been dissolved	SNOMED
		4079702, --	Husband left home	SNOMED
		4327561, --	Legally separated with interlocutory decree	SNOMED
		4049093, --	Marriage annulment	SNOMED
		4027529, --	Separated	SNOMED
		4171752, --	Spouse left home	SNOMED
		4322182, --	Trial separation	SNOMED
		4149091, --	Widow	SNOMED
		4143188, --	Widowed	SNOMED
		44791570, --	Widowed/surviving civil partner	SNOMED
		4302155, --	Widower	SNOMED
		4172699, --	Wife left home	SNOMED
		4022641, --	Broken engagement	SNOMED
		4145800, --	Broken with partner	SNOMED
		44791569, --	Divorced/person whose civil partnership has been dissolved	SNOMED
		4049093, --	Marriage annulment	SNOMED
		4027529 --	Separated	SNOMED
	)

GO


INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	NULL [ancestor_concept_id],
	'admit_priority' as variable_name,
	NULL [variable_desc]
FROM 
	VOCAB.CONCEPT c
WHERE
	c.concept_id IN (	
		46235349,  -- Admission Priority
		46236615  --Admission priority
	)

GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	NULL [ancestor_concept_id],
	CASE WHEN c.concept_id = 2000000033	THEN 'admitting_service'
		WHEN c.concept_id = 2000000034 THEN	'discharge_service'
		WHEN c.concept_id = 2000000035	THEN 'scheduled_surgical_service'
		WHEN c.concept_id = 4149152 THEN 'surgical_service'
		WHEN c.concept_id = 2000000036	THEN 'scheduled_service'
		ELSE NULL END as variable_name,
	NULL [variable_desc]

FROM 
	VOCAB.CONCEPT c
WHERE
	c.concept_id IN (
		2000000033,	-- 'Admitting Service'
		2000000034, --	'Discharge Service'
		2000000035,	-- 'Scheduled Surgical Service'
		4149152, -- 'Surgical Service'
		2000000036  -- Scheduled Service
	)

GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	'weight' as variable_name,
	NULL [variable_desc]
FROM 
	VOCAB.CONCEPT c
	INNER JOIN VOCAB.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
WHERE
	standard_concept = 'S'
	AND
	ca.ancestor_concept_id IN (
		4099154,  -- Body Weight SNOMED
		37111521,  -- Weight
		3025315  -- Body Weight LOINC
	)
	AND 
	(
		c.concept_name NOT LIKE '%birth%'
		AND
		c.concept_name NOT LIKE '%baseline%'
		AND
		c.concept_name NOT LIKE '%previous%'
	)

GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	NULL [ancestor_concept_id],
	'height' as variable_name,
	NULL [variable_desc]
FROM 
	VOCAB.CONCEPT c
WHERE
	c.concept_id IN (	
		607590,  -- Body Height SNOMED  #TODO: Check why this concept id is missing from the concept table
		3036277 -- Body Height LOINC
	)

GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	NULL [ancestor_concept_id],
	'scheduled_anesthesia_type' as variable_name,
	NULL [variable_desc]
FROM 
	VOCAB.CONCEPT c
WHERE
	c.concept_id = 2000000028

GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	ancestor_concept_id [ancestor_concept_id],
	'general_anesthesia' as variable_name,
	NULL [variable_desc]
FROM 
	VOCAB.CONCEPT_ANCESTOR CA
	INNER JOIN VOCAB.CONCEPT C on C.concept_id = CA.descendant_concept_id
WHERE
	CA.ancestor_concept_id = 4174669

GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	ancestor_concept_id [ancestor_concept_id],
	'regional_or_local_anesthesia' as variable_name,
	NULL [variable_desc]
FROM 
	VOCAB.CONCEPT_ANCESTOR CA
	INNER JOIN VOCAB.CONCEPT C on C.concept_id = CA.descendant_concept_id
WHERE
	CA.ancestor_concept_id = 42538249 -- Injection of anesthetic agent SNOMED
	AND
	descendant_concept_id NOT IN (SELECT descendant_concept_id FROM VOCAB.CONCEPT_ANCESTOR WHERE ancestor_concept_id = 4253896) -- SNOMED  Administration of anesthesia for procedure
	AND
	descendant_concept_id NOT IN (SELECT descendant_concept_id FROM VOCAB.CONCEPT_ANCESTOR WHERE ancestor_concept_id = 45888085) -- CPT SURGERY
	AND
	descendant_concept_id NOT IN (SELECT descendant_concept_id FROM VOCAB.CONCEPT_ANCESTOR WHERE ancestor_concept_id = 45888423) -- CPT Anesthesia for Radiological Procedures
	AND
	descendant_concept_id NOT IN (SELECT descendant_concept_id FROM VOCAB.CONCEPT_ANCESTOR WHERE ancestor_concept_id = 4174669) -- SNOMED General Anesthesia

GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	ancestor_concept_id [ancestor_concept_id],
	'charity' as variable_name,
	NULL [variable_desc]
FROM 
	VOCAB.CONCEPT_ANCESTOR CA
	INNER JOIN VOCAB.CONCEPT C on C.concept_id = CA.descendant_concept_id
WHERE
	ancestor_concept_id = 437
	AND
	descendant_concept_id NOT IN (440) -- Clincial Trial/Research
GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	ancestor_concept_id [ancestor_concept_id],
	'private' as variable_name,
	NULL [variable_desc]
FROM 
	VOCAB.CONCEPT_ANCESTOR CA
	INNER JOIN VOCAB.CONCEPT C on C.concept_id = CA.descendant_concept_id
WHERE
	ancestor_concept_id = 327
GO

with partitioned as (
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	ancestor_concept_id [ancestor_concept_id],
	'current_smoker' as variable_name,
	ROW_NUMBER() OVER(PARTITION BY concept_id ORDER BY  domain_id ASC) AS seq,
	NULL [variable_desc]
FROM 
	VOCAB.CONCEPT_ANCESTOR CA
	INNER JOIN VOCAB.CONCEPT C on C.concept_id = CA.descendant_concept_id
WHERE
	ancestor_concept_id IN (40486518, --	Failed attempt to stop smoking	SNOMED
						    4052948, --	Keeps trying to stop smoking	SNOMED
							4216174, --	Not interested in stopping smoking	SNOMED
							421540, --9	Ready to stop smoking	SNOMED
							4298794, --	Smoker	SNOMED
							4046886, --	Smoking reduced	SNOMED
							4190573, --	Thinking about stopping smoking	SNOMED
							4269997, --	Tobacco smoking consumption - finding	SNOMED
							4058137, --	Trying to give up smoking	SNOMED
							44789712, --	Wants to stop smoking	SNOMED
							45883387, --	Every-day smoker	LOINC
							45881686, --	Some-day smoker	LOINC
							35817596, --	Current	UK Biobank
							45881517, --	Current every day smoker	LOINC
							4588403, --7	Current some day smoker	LOINC
							45884038, --	Heavy tobacco smoker	LOINC
							45878118, --	Light tobacco smoker	LOINC
							45881518, --	Smoker, current status unknown	LOINC
							903654) --	Tobacco or its derivatives user	OMOP Extension
)
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	variable_name,
	[variable_desc]
FROM
	partitioned 
WHERE
	seq = 1 -- remove duplicate key value pairs
GO

with partitioned as (
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	ancestor_concept_id [ancestor_concept_id],
	'former_smoker' as variable_name,
	ROW_NUMBER() OVER(PARTITION BY concept_id ORDER BY  domain_id ASC) AS seq,
	NULL [variable_desc]
FROM 
	VOCAB.CONCEPT_ANCESTOR CA
	INNER JOIN VOCAB.CONCEPT C on C.concept_id = CA.descendant_concept_id
WHERE
	ancestor_concept_id IN (40486696, --	Smoked before confirmation of pregnancy	SNOMED
							4052032, --	Stopped smoking	SNOMED
							45883458, --	Former smoker	LOINC
							35822845, --	Previous	UK Biobank
							45883458, --	Former smoker	LOINC
							4310250, --	Ex-smoker	SNOMED
							3197200 --	History of tobacco use	Nebraska Lexicon
							)
)
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	variable_name,
	[variable_desc]
FROM
	partitioned 
WHERE
	seq = 1 -- remove duplicate key value pairs
GO

with partitioned as (
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	ancestor_concept_id [ancestor_concept_id],
	'never_smoker' as variable_name,
	ROW_NUMBER() OVER(PARTITION BY concept_id ORDER BY  domain_id ASC) AS seq,
	NULL [variable_desc]
FROM 
	VOCAB.CONCEPT_ANCESTOR CA
	INNER JOIN VOCAB.CONCEPT C on C.concept_id = CA.descendant_concept_id
WHERE
	ancestor_concept_id IN (35821337, --	Never	UK Biobank
							45879404, --	Never smoker	LOINC
							903653, --	Never used tobacco or its derivatives	OMOP Extension
							4233854, --	Aggressive non-smoker	SNOMED
							4237392, --	Intolerant non-smoker	SNOMED
							4144272, --	Never smoked tobacco	SNOMED
							4019979, --	Non-smoker for medical reasons	SNOMED
							4022662, --	Non-smoker for personal reasons	SNOMED
							4022663, --	Non-smoker for religious reasons	SNOMED
							4227889 --	Tolerant non-smoker	SNOMED
							)
	AND
	concept_id NOT IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta where variable_name = 'former_smoker')
)
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	variable_name,
	[variable_desc]
FROM
	partitioned 
WHERE
	seq = 1 -- remove duplicate key value pairs
GO


with partitioned as (
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	ancestor_concept_id [ancestor_concept_id],
	'healtchare_facility' as variable_name,
	ROW_NUMBER() OVER(PARTITION BY concept_id ORDER BY  domain_id ASC) AS seq,
	NULL [variable_desc]
FROM 
	VOCAB.CONCEPT_ANCESTOR CA
	INNER JOIN VOCAB.CONCEPT C on C.concept_id = CA.descendant_concept_id
WHERE
	ancestor_concept_id IN (9201, --Inpatient Visit
							42898160 -- Non-hospital institution Visit
							)
)
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	variable_name,
	[variable_desc]
FROM
	partitioned 
WHERE
	seq = 1 -- remove duplicate key value pairs
GO





with partitioned as (
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	'anesthesia_type' as variable_name,
	ROW_NUMBER() OVER(PARTITION BY concept_id ORDER BY  domain_id ASC) AS seq,
	NULL [variable_desc]
FROM 
	VOCAB.CONCEPT c
	INNER JOIN VOCAB.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
WHERE
	ca.ancestor_concept_id IN (4160439, -- Adminstration of Anesthesia SNOMED
							   4219502, -- Sedation SNOMED
							   42538249) -- Injection of anesthetic agent SNOMED
	AND
	descendant_concept_id NOT IN (SELECT descendant_concept_id FROM VOCAB.CONCEPT_ANCESTOR WHERE ancestor_concept_id = 4253896) -- SNOMED  Administration of anesthesia for procedure
	AND
	descendant_concept_id NOT IN (SELECT descendant_concept_id FROM VOCAB.CONCEPT_ANCESTOR WHERE ancestor_concept_id = 45888085) -- CPT SURGERY
	AND
	descendant_concept_id NOT IN (SELECT descendant_concept_id FROM VOCAB.CONCEPT_ANCESTOR WHERE ancestor_concept_id = 45888423) -- CPT Anesthesia for Radiological Procedures
)
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	variable_name,
	[variable_desc]
FROM
	partitioned 
WHERE
	seq = 1 -- remove duplicate key value pairs
GO

with partitioned as (
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	'urine_output' as variable_name,
	ROW_NUMBER() OVER(PARTITION BY concept_id ORDER BY  domain_id ASC) AS seq,
	NULL [variable_desc]
FROM 
	VOCAB.CONCEPT c
	INNER JOIN VOCAB.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
WHERE
	ca.ancestor_concept_id IN (
		4265527,
		3014315,
		4264378,
		40481610
	)
	AND
	concept_name NOT LIKE '%hour%'
	AND
	concept_name NOT LIKE '%timed%'

)
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	variable_name,
	[variable_desc]
FROM
	partitioned 
WHERE
	seq = 1 -- remove duplicate key value pairs
GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	NULL [ancestor_concept_id],
	'blood_loss' as variable_name,
	NULL [variable_desc]
FROM 
	VOCAB.CONCEPT c
WHERE
	c.concept_id = 21493943

GO

with partitioned as (
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	'body_temperature' as variable_name,
	ROW_NUMBER() OVER(PARTITION BY concept_id ORDER BY  domain_id ASC) AS seq,
	NULL [variable_desc]
FROM 
	VOCAB.CONCEPT c
	INNER JOIN VOCAB.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
WHERE
	ca.ancestor_concept_id IN (
		4302666, -- SNOMED Body Temp
		4329518,  -- SNOMED Digital Temp
		1003960 -- LOINC Body temp
	)
	AND
	c.standard_concept = 'S'
	AND
	concept_name NOT LIKE '%hour%'
	AND
	concept_name NOT LIKE '%timed%'
	AND
	concept_name NOT LIKE '%first%'
	AND
	concept_name NOT LIKE '%site%'
	AND
	concept_name NOT LIKE '%maximum%'
	AND
	concept_name NOT LIKE '%model%'
	AND
	concept_name NOT LIKE '%special%'
	AND
	concept_name NOT LIKE '%after%'

)
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	variable_name,
	[variable_desc]
FROM
	partitioned 
WHERE
	seq = 1 -- remove duplicate key value pairs
GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	'spo2' as variable_name,
	NULL [variable_desc]
FROM 
	VOCAB.CONCEPT c
	INNER JOIN VOCAB.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
WHERE
	ca.ancestor_concept_id IN (
		4011919, -- Oxygen Saturation SNOMED
		4020553, -- Oxygen Saturation LOINC
		21492232  -- Plethysmogram Arterial blood Pulse oximetry
	)
	AND
	c.standard_concept = 'S'
	AND
	concept_name NOT LIKE '%minimum%'
	AND
	concept_name NOT LIKE '%target%'
	AND
	concept_name NOT LIKE '%documented%'
	AND
	c.domain_id = 'Measurement'
	AND
	concept_name NOT LIKE '%gases%'
	AND
	concept_name NOT LIKE '%hour%'

GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	'fio2' as variable_name,
	NULL [variable_desc]
FROM 
	VOCAB.CONCEPT c
	INNER JOIN VOCAB.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
WHERE
	ca.ancestor_concept_id IN (
		3026238,  --vent fio2
		36033026,  -- Loinc FiO2
		4353936  -- SNOMED FiO2
	)
	AND
	c.standard_concept = 'S'
	AND
	concept_name NOT LIKE '%hour%'

GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	'etco2' as variable_name,
	NULL [variable_desc]
FROM 
	VOCAB.CONCEPT c
	INNER JOIN VOCAB.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
WHERE
	ca.ancestor_concept_id IN (
		4353938,  -- SNOMED ETCO2
		3035357,  -- ETCO2 LOINC
		21490569, -- LOINC Airway Adapter
		3035115 -- LOINC Calculated
	)
	AND
	c.standard_concept = 'S'

GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	NULL [ancestor_concept_id],
	'mac_40' as variable_name,
	NULL [variable_desc]
FROM 
	VOCAB.CONCEPT c
WHERE
	c.concept_id = 3661605

GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	'peep' as variable_name,
	NULL [variable_desc]
FROM 
	VOCAB.CONCEPT c
	INNER JOIN VOCAB.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
WHERE
	ca.ancestor_concept_id IN (
		37075680,  -- LOINC PEEP
		3022875,  -- LOINC PEEP
		44782659,  -- SNOMED PEEP
		4353713 -- SNOMED Peep
	)
	AND
	c.standard_concept = 'S'

GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	'o2_flow_rate' as variable_name,
	NULL [variable_desc]
FROM 
	VOCAB.CONCEPT c
	INNER JOIN VOCAB.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
WHERE
	ca.ancestor_concept_id IN (
		4141684,  -- SNOMED O2 Flow
		3005629  -- LOINC O2 Flow
	)
	AND
	c.standard_concept = 'S'



GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	'tidal_volume' as variable_name,
	NULL [variable_desc]
FROM 
	VOCAB.CONCEPT c
	INNER JOIN VOCAB.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
WHERE
	ca.ancestor_concept_id IN (
		4029625,  -- SNOMED Tidal Volume
		3012410,  -- LOINC Tidal Vent Setting
		21490854  -- LOINC VENT TIDAL VOLUME
	)
	AND
	c.standard_concept = 'S'
	AND
	concept_name NOT LIKE '%inspiratory%'
	AND
	concept_name NOT LIKE '%expiratory%'

GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	'tidal_volume_inspired' as variable_name,
	NULL [variable_desc]
FROM 
	VOCAB.CONCEPT c
	INNER JOIN VOCAB.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
WHERE
	ca.ancestor_concept_id IN (
		44782826,  -- SNOMED Inspiratory Tidal Volume
		3033780,  -- LOINC Tidal Volume Inspired
		3016166  -- LOINC Tidal Volume Inspired spont + mech
	)
	AND
	c.standard_concept = 'S'

GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	'tidal_volume_exhaled' as variable_name,
	NULL [variable_desc]
FROM 
	VOCAB.CONCEPT c
	INNER JOIN VOCAB.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
WHERE
	ca.ancestor_concept_id IN (
		44782827, -- SNOMED Expiratory Tidal Volume
		21490789,  -- LOINC Expired Tidal Volume
		3015016  -- LOINC Tidal Volume Expired spont + mech
	)
	AND
	c.standard_concept = 'S'

GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	'respiratory_rate' as variable_name,
	NULL [variable_desc]
FROM 
	VOCAB.CONCEPT c
	INNER JOIN VOCAB.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
WHERE
	ca.ancestor_concept_id IN (
		4313591, -- Respiratory Rate SNOMED
		3024171  -- Respiratory Rate LOINC
	)
	AND
	c.standard_concept = 'S'
	AND
	c.concept_name NOT LIKE '%baseline%'

GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	NULL [ancestor_concept_id],
	'pip' as variable_name,
	NULL [variable_desc]
FROM 
	VOCAB.CONCEPT c
WHERE
	c.concept_id = 4101694

GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	'heart_rate' as variable_name,
	NULL [variable_desc]
FROM 
	VOCAB.CONCEPT c
	INNER JOIN VOCAB.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
WHERE
	ca.ancestor_concept_id IN (
		4239408, -- SNOMED Heart Rate
		3027018  -- LOINC Heart Rate
	)
	AND
	c.standard_concept = 'S'
	AND
	c.concept_name NOT LIKE '%admission%'
	AND
	c.concept_name NOT LIKE '%maximum%'
	AND
	c.concept_name NOT LIKE '%resting%'
	AND
	c.concept_name NOT LIKE '%baseline%'
	AND
	c.concept_name NOT LIKE '%minimum%'
	AND
	c.concept_name NOT LIKE '%target%'

GO

with partitioned as (
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	'diastolic_blood_pressure' as variable_name,
	ROW_NUMBER() OVER(PARTITION BY concept_id ORDER BY  domain_id ASC) AS seq,
	NULL [variable_desc]
FROM 
	VOCAB.CONCEPT c
	INNER JOIN VOCAB.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
WHERE
	ca.ancestor_concept_id IN (
		4154790, -- SNOMED DBP
		3012888,  -- LOINC DBP
		3034703,  -- LOINC DBP SITTING
		3013940, -- LOINC DBP Supine
		35610320,  -- SNOMED Diastolic Arterial Pressure
		4354253, -- SNOMED Invasive Diasolic
		4068414, -- SNOMED Non-Invasive Diastolic
		4236281, -- SNOMED Lying Diastolic
		4248524  -- SNOMED Sitting BP
	)
	AND
	c.standard_concept = 'S'
	AND
	c.concept_name NOT LIKE '%target%'
	AND
	c.concept_name NOT LIKE '%hour%'
	AND
	c.concept_name NOT LIKE '%admission%'
	AND
	c.concept_name NOT LIKE '%centile%'
	AND
	c.concept_name NOT LIKE '%birth%'
	AND
	c.concept_name NOT LIKE '%ambulatory%'
	AND
	c.concept_name NOT LIKE '%standing%'
)
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	variable_name,
	[variable_desc]
FROM
	partitioned 
WHERE
	seq = 1 -- remove duplicate key value pairs
GO

with partitioned as (
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	'systolic_blood_pressure' as variable_name,
	ROW_NUMBER() OVER(PARTITION BY concept_id ORDER BY  domain_id ASC) AS seq,
	NULL [variable_desc]
FROM 
	VOCAB.CONCEPT c
	INNER JOIN VOCAB.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
WHERE
	ca.ancestor_concept_id IN (
		4217013, -- SNOMED SBP
		4152194, -- SNOMED SBP
		3004249,  -- LOINC SBP
		3018586,  -- LOINC SBP SITTING
		3009395, -- LOINC SBP Supine
		21490853, -- SNOMED Invasive Systolic
		4353843, -- SNOMED Invasive Sys Art pressure
		4354252, -- SNOMED Non-Invasive Systolic
		4248525, -- SNOMED Lying SBP
		4232915  -- SNOMED Sitting SBP
	)
	AND
	c.standard_concept = 'S'
	AND
	c.concept_name NOT LIKE '%target%'
	AND
	c.concept_name NOT LIKE '%hour%'
	AND
	c.concept_name NOT LIKE '%admission%'
	AND
	c.concept_name NOT LIKE '%centile%'
	AND
	c.concept_name NOT LIKE '%birth%'
	AND
	c.concept_name NOT LIKE '%ambulatory%'
	AND
	c.concept_name NOT LIKE '%standing%'
)
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	variable_name,
	[variable_desc]
FROM
	partitioned 
WHERE
	seq = 1 -- remove duplicate key value pairs
GO

with partitioned as (
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	'mean_arterial_pressure' as variable_name,
	ROW_NUMBER() OVER(PARTITION BY concept_id ORDER BY  domain_id ASC) AS seq,
	NULL [variable_desc]
FROM 
	VOCAB.CONCEPT c
	INNER JOIN VOCAB.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
WHERE
	ca.ancestor_concept_id IN (
		4108289, -- SNOMED Non-Invasive MAP
		4239021, -- SNOMED Mean Blood Pressure
		4108290, -- SNOMED Invasive MAP
		4239021, -- SNOMED MAP
		3027598 -- LOINC MAP
	)
	AND
	c.standard_concept = 'S'
	AND
	c.concept_name NOT LIKE '%target%'
	AND
	c.concept_name NOT LIKE '%hour%'
	AND
	c.concept_name NOT LIKE '%admission%'
	AND
	c.concept_name NOT LIKE '%centile%'
	AND
	c.concept_name NOT LIKE '%birth%'
	AND
	c.concept_name NOT LIKE '%ambulatory%'
	AND
	c.concept_name NOT LIKE '%standing%'
	AND
	c.concept_name NOT LIKE '%venous%'
	AND
	c.concept_name NOT LIKE '%vein%'
)
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	variable_name,
	[variable_desc]
FROM
	partitioned 
WHERE
	seq = 1 -- remove duplicate key value pairs
GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	NULL [ancestor_concept_id],
	'urine_sample' as variable_name,
	NULL [variable_desc]
FROM 
	VOCAB.CONCEPT c
WHERE
	concept_id IN (40481783, -- 2 hour urine specimen
					4174445, -- 24 hour urine sample
					4122280, -- Mid-stream urine sample
					4257649, -- Timed urine specimen
					1033714, -- Urine
					4046280) -- Urine specimen

GO


INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	NULL [ancestor_concept_id],
	'blood_sample' [variable_name],
	NULL [variable_desc]
FROM 
	VOCAB.CONCEPT c
WHERE
	concept_id IN (4047496, -- Arterial blood specimen
						1029919, -- Blood
						1029928, -- Blood arterial
						1004423, -- Blood arterial + Blood venous
						1029338, -- Blood arterial and Blood mixed venous
						1029929, -- Blood capillary
						1620206, -- Blood central venous
						1029931, -- Blood mixed venous
						1029932, -- Blood peripheral
						4001225, -- Blood specimen
						1029933, -- Blood venous
						4046834, -- Capillary blood specimen
						4001226, -- Mixed venous blood specimen
						4047495, -- Peripheral blood specimen
						1032286, -- Plasma
						1019913, -- Plasma arterial
						1012720, -- Plasma or Blood
						1032314, -- Plasma or RBC
						4000626, -- Plasma specimen
						1019914, -- Plasma venous
						1032890, -- Red Blood Cells
						1033211, -- Serum
						1033219, -- Serum and Blood
						1012718, -- Serum and Plasma
						1033297, -- Serum or Blood
						1033306, -- Serum or Plasma
						1004284, -- Serum or Plasma + Blood venous
						4001181, -- Serum specimen
						1033343, -- Serum, Plasma or Blood
						4045667, -- Venous blood specimen
						45766302, -- Venous cord blood specimen
						4122283) -- Whole blood sample


GO


INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	'alp' [variable_name],
	NULL [variable_desc]
FROM
    VOCAB.CONCEPT_ANCESTOR CA
    INNER JOIN VOCAB.CONCEPT C on CA.descendant_concept_id = c.concept_id
	LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
	CA.ancestor_concept_id IN (4230636) --SNOMED Alkaline phosphatase measurement
	AND
	C.concept_name NOT LIKE '%/a%'
	AND
	C.concept_name NOT LIKE '%renal%'
	AND
	C.concept_name NOT LIKE '%bone%'
	AND
	C.concept_name NOT LIKE '%intestin%'
	AND
	C.concept_name NOT LIKE '%placenta%'
	AND
	C.concept_name NOT LIKE '%fetal%'
	AND
	C.concept_name LIKE '%activity%'
	AND
	CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'blood_sample')

GO


INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	'aptt' [variable_name],
	NULL [variable_desc]
FROM
    VOCAB.CONCEPT_ANCESTOR CA
    INNER JOIN VOCAB.CONCEPT C on CA.descendant_concept_id = c.concept_id
	LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
	CA.ancestor_concept_id IN (44809202, -- Activated partial thromboplastin time
							   37037324, -- aPTT | Blood | Coagulation
							   37075315, -- aPTT | Plasma | Coagulation
							   37041593) -- aPTT | Platelet poor plasma | Coagulation
	AND
	C.concept_class_id IN ('Lab Test', 'Observable Entity')
	AND
	C.concept_name NOT LIKE '%challenge%'
	AND
	C.concept_name NOT LIKE '%post %'
	AND
	C.concept_name NOT LIKE '% after %'
	AND
	C.concept_name NOT LIKE '%target%'
	AND
	C.concept_name NOT LIKE '%minimum%'
	AND
	C.concept_name NOT LIKE '%maximum%'
	AND
	C.concept_name NOT LIKE '%ratio%'

GO


INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	'bilirubin_total' [variable_name],
	NULL [variable_desc]
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    CA.ancestor_concept_id IN (4118986) -- Bilirubin measurement
	AND
	C.concept_name LIKE '%Total%'
	AND
	C.concept_name NOT LIKE '%presence%'
	AND
	C.concept_name NOT LIKE '%ratio%'
	AND
	C.concept_name NOT LIKE '%/b%'
	AND
	CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'blood_sample')

GO


INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	'bicarbonate' [variable_name],
	NULL [variable_desc]
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    CA.ancestor_concept_id IN (4227915) -- Bicarbonate measurement
	AND
	C.concept_name LIKE '%bicarbonate%'
	AND
	CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'blood_sample')

GO


INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	'calcium' [variable_name],
	NULL [variable_desc]
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    CA.ancestor_concept_id IN (4216722) --  Calcium Measurement
	AND
	C.concept_name NOT LIKE '%ionized%'
	AND
	CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'blood_sample')

                                                 
GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	'creatinine' [variable_name],
	NULL [variable_desc]
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    CA.ancestor_concept_id IN (4324383) -- Creatinine measurement
	AND
	C.concept_name NOT LIKE '%ratio%'
	AND
	C.concept_name NOT LIKE '%strip%'
	AND
	C.concept_name NOT LIKE '%dipstick%'
    AND
	CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'blood_sample')


GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	LOWER('C-reactive_protein') [variable_name],
	NULL [variable_desc]
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    CA.ancestor_concept_id IN (4208414) -- Creatinine measurement
	AND
	C.concept_name NOT LIKE '%ratio%'
	AND
	C.concept_name NOT LIKE '%strip%'
	AND
	C.concept_name NOT LIKE '%dipstick%'
    AND
	CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'blood_sample')

GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	LOWER('Eosinophils') [variable_name],
	NULL [variable_desc]
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    CA.ancestor_concept_id IN (37393857, -- SNOMED Eosinophil count
							   37060474) -- LOINC Eosinophils | Blood | Hematology and Cell counts
	AND
	C.concept_name NOT LIKE '%presence%'
	AND
	C.concept_name NOT LIKE '%percent%'
    AND
	(CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'blood_sample')
	OR
	CR.concept_id_2 IS NULL)
	AND
	C.standard_concept = 'S'

GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	LOWER('Glucose') [variable_name],
	NULL [variable_desc]
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    CA.ancestor_concept_id IN (4149519) -- SNOMED Glucose Measurment
	AND
	C.concept_name NOT LIKE '%challenge%'
	AND
	C.concept_name NOT LIKE '%percent%'
    AND
	CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'blood_sample')
	AND
	C.standard_concept = 'S'
	AND
	C.concept_name NOT LIKE '% PO%'

GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	LOWER('HBA1C') [variable_name],
	NULL [variable_desc]
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    CA.ancestor_concept_id IN (4184637) -- Hemoglobin A1C (HBA1C) measurement
	AND
	C.concept_name NOT LIKE 'most recent%'
	--AND
	--C.concept_name NOT LIKE '%percent%'
    AND
	(CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'blood_sample')
	OR CR.concept_id_2 IS NULL)
	AND
	C.standard_concept = 'S'

GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	LOWER('Hematocrit') [variable_name],
	NULL [variable_desc]
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    CA.ancestor_concept_id IN (4151358, -- Hematocrit measurement
							   37393840, -- Hematocrit
							   37209030, -- Hematocit volume fraction in blood
							   37070108) -- LOINC Hematocrit | Blood | Hematology and Cell counts
	--AND
	--C.concept_name NOT LIKE 'most recent%'
	--AND
	--C.concept_name NOT LIKE '%percent%'
    AND
	(CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'blood_sample')
	OR CR.concept_id_2 IS NULL)
	AND
	C.standard_concept = 'S'

GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	LOWER('IMMGranulocytes') [variable_name],
	NULL [variable_desc]
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    CA.ancestor_concept_id IN (37042766) --LOINC Immature granulocytes | Blood | Hematology and Cell counts
	AND
	C.concept_name NOT LIKE '%Presence%'
    AND
	(CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'blood_sample')
	OR CR.concept_id_2 IS NULL)
	AND
	C.standard_concept = 'S'

GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	LOWER('lactate') [variable_name],
	NULL [variable_desc]
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    CA.ancestor_concept_id IN (37394005) --SNOMED Lactate from fluid observable
	AND
	C.concept_name NOT LIKE '%Presence%'
	AND
	C.concept_name NOT LIKE '%CSF%'
    AND
	(CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'blood_sample')
	OR CR.concept_id_2 IS NULL)
	AND
	C.standard_concept = 'S'
UNION
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	cr1.concept_id_2 [ancestor_concept_id],
	LOWER('lactate') [variable_name],
	NULL [variable_desc]
FROM
        VOCAB.CONCEPT_RELATIONSHIP cr1
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = cr1.concept_id_1
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    cr1.concept_id_2 = 40785884 AND cr1.relationship_id = 'Has component'
	AND
	C.concept_name NOT LIKE '%Presence%'
	AND
	C.concept_name NOT LIKE '%CSF%'
    AND
	(CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'blood_sample')
	OR CR.concept_id_2 IS NULL)
	AND
	C.standard_concept = 'S'
GO


INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	cr1.concept_id_2 [ancestor_concept_id],
	LOWER('MDW') [variable_name],
	NULL [variable_desc]
FROM
        VOCAB.CONCEPT_RELATIONSHIP cr1
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = cr1.concept_id_1
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    cr1.concept_id_2 = 37041679 --Monocyte distribution width
	AND
	cr1.relationship_id = 'Has component'
    AND
	(CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'blood_sample')
	OR CR.concept_id_2 IS NULL)
	AND
	C.standard_concept = 'S'
GO





INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	LOWER('Monocytes') [variable_name],
	NULL [variable_desc]
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    CA.ancestor_concept_id IN (37393859) --SNOMED Monocyte count
	AND
	C.concept_name NOT LIKE '%Presence%'
	AND
	C.concept_name NOT LIKE '%percent%'
    AND
	(CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'blood_sample')
	OR CR.concept_id_2 IS NULL)
	AND
	C.standard_concept = 'S'
UNION
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	cr1.concept_id_2 [ancestor_concept_id],
	LOWER('Monocytes') [variable_name],
	NULL [variable_desc]
FROM
        VOCAB.CONCEPT_RELATIONSHIP cr1
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = cr1.concept_id_1
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    cr1.concept_id_2 = 40785793-- LOINC Monocytes
	AND
	cr1.relationship_id = 'Has component'
    AND
	(CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'blood_sample')
	OR CR.concept_id_2 IS NULL)
	AND
	C.standard_concept = 'S'
GO


with partitioned as (
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	LOWER('pH') [variable_name],
	ROW_NUMBER() OVER(PARTITION BY concept_id ORDER BY  domain_id ASC) AS seq,
	NULL [variable_desc]
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    CA.ancestor_concept_id IN (37393986,  --SNOMED Fluid sample pH
								40653530, -- LOINC  pH|Log Substance Concentration|Moment in time|Blood, Serum or Plasma
								4215028) -- SNOMED pH measurement
    AND
	(CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'blood_sample')
	OR (CR.concept_id_2 IS NULL AND (C.concept_name LIKE '%blood%' OR c.concept_name LIKE '%serum%' OR c.concept_name LIKE '%plasma%')))
	AND
	C.standard_concept = 'S'
	AND
	C.concept_id NOT IN (2212348, 2212349)
)
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	variable_name,
	[variable_desc]
FROM
	partitioned 
WHERE
	seq = 1 -- remove duplicate key value pairs
GO




with partitioned as (
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	LOWER('Potassium') [variable_name],
	ROW_NUMBER() OVER(PARTITION BY concept_id ORDER BY  domain_id ASC) AS seq,
	NULL [variable_desc]
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    CA.ancestor_concept_id IN (4245152) -- SNOMED potassium measurement
    AND
	(CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'blood_sample')
	OR (CR.concept_id_2 IS NULL AND (C.concept_name LIKE '%blood%' OR c.concept_name LIKE '%serum%' OR c.concept_name LIKE '%plasma%')))
	AND
	C.standard_concept = 'S'
)
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	variable_name,
	[variable_desc]
FROM
	partitioned 
WHERE
	seq = 1 -- remove duplicate key value pairs
GO



with partitioned as (
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	LOWER('Platelet') [variable_name],
	ROW_NUMBER() OVER(PARTITION BY concept_id ORDER BY  domain_id ASC) AS seq,
	NULL [variable_desc]
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    CA.ancestor_concept_id IN (37393863, -- SNOMED Platelet count
							   37037425, -- LOINC Platelets | Blood | Hematology and Cell counts
								37058547, --LOINC Platelets | Plasma | Hematology and Cell counts
								37071943, -- Platelets | Blood capillary | Hematology and Cell counts
								37034532) -- LOINC Platelets | Platelet rich plasma | Hematology and Cell counts
    AND
	(CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'blood_sample')
	OR (CR.concept_id_2 IS NULL AND (C.concept_name LIKE '%blood%' OR c.concept_name LIKE '%serum%' OR c.concept_name LIKE '%plasma%')))
	AND
	C.standard_concept = 'S'
)
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	variable_name,
	[variable_desc]
FROM
	partitioned 
WHERE
	seq = 1 -- remove duplicate key value pairs
GO



INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	LOWER('Bilirubin_Direct') [variable_name],
	NULL [variable_desc]
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    CA.ancestor_concept_id IN (4118986) -- Bilirubin measurement
	AND
	(CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'blood_sample')
	OR CR.concept_id_2 IS NULL)
	AND
	C.standard_concept = 'S'
	AND
	C.concept_name LIKE '%Direct%'
	AND
	C.concept_name NOT LIKE '%presence%'
	AND
	C.concept_name NOT LIKE '%ratio%'
	AND
	C.concept_name NOT LIKE '%/b%'
	AND
	C.concept_name NOT LIKE '%indirect%'

GO


INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	LOWER('Bilirubin_Indirect') [variable_name],
	NULL [variable_desc]
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    CA.ancestor_concept_id IN (4118986) -- Bilirubin measurement
	AND
	(CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'blood_sample')
	OR CR.concept_id_2 IS NULL)
	AND
	C.standard_concept = 'S'
	AND
	C.concept_name NOT LIKE '%presence%'
	AND
	C.concept_name NOT LIKE '%ratio%'
	AND
	C.concept_name NOT LIKE '%/b%'
	AND
	C.concept_name LIKE '%indirect%'
	AND
	C.concept_name NOT LIKE '%conjugated%'

GO

	
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	LOWER('ALBUMIN_UR') [variable_name],
	NULL [variable_desc]
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    CA.ancestor_concept_id IN (4152996 )-- SNOMED Urine albumin Measurement
	AND
	(CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'urine_sample')
	OR (CR.concept_id_2 IS NULL AND C.concept_name LIKE '%urine%'))
	AND
	C.standard_concept = 'S'
	AND
	C.concept_name NOT LIKE '%Microalbumin%'
	AND
	C.concept_name NOT LIKE '%24%'
	AND
	C.concept_name NOT LIKE '%presence%'
	AND
	C.concept_name NOT LIKE '%methemalbumin%'
	AND
	C.concept_name NOT LIKE '%Protein.total%'
	AND
	C.concept_name NOT LIKE '%/time%'

GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	LOWER('ALBUMIN') [variable_name],
	NULL [variable_desc]
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    CA.ancestor_concept_id IN (4097664) -- SNOMED Albumin Measurement
	AND
	(CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'blood_sample')
	OR (CR.concept_id_2 IS NULL AND (C.concept_name LIKE '%blood%' OR C.concept_name LIKE '%serum%' OR C.concept_name LIKE '%plasma%')))
	AND
	C.standard_concept = 'S'
	AND
	C.concept_name NOT LIKE '%Microalbumin%'
	AND
	C.concept_name NOT LIKE '%glycated%'
	AND
	C.concept_name NOT LIKE '%CSF%'
	AND
	C.concept_name NOT LIKE '%methemalbumin%'
	AND
	C.concept_name NOT LIKE '%Protein.total%'
	AND
	C.concept_name NOT LIKE '%presence%'
	AND
	C.concept_id NOT IN (40481778, 44817128)

GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	LOWER('ALBUMIN_UR_24H') [variable_name],
	NULL [variable_desc]
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    CA.ancestor_concept_id IN (4152996 )-- SNOMED Urine albumin Measurement
	AND
	(CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'urine_sample')
	OR (CR.concept_id_2 IS NULL AND C.concept_name LIKE '%urine%'))
	AND
	C.standard_concept = 'S'
	AND
	C.concept_name NOT LIKE '%Microalbumin%'
	AND
	C.concept_name LIKE '%24%'
	AND
	C.concept_name NOT LIKE '%presence%'
	AND
	C.concept_name NOT LIKE '%methemalbumin%'
	AND
	C.concept_name NOT LIKE '%Protein.total%'

GO


INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	LOWER('ALT') [variable_name],
	'Alanine aminotransferase (ALT) - blood measurement' [variable_desc]
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    CA.ancestor_concept_id IN (4095055 )-- SNOMED ALT - blood measurement
	AND
	(CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'blood_sample')
	OR (CR.concept_id_2 IS NULL AND (C.concept_name LIKE '%blood%' OR C.concept_name LIKE '%serum%' OR C.concept_name LIKE '%plasma%')))
	AND
	C.standard_concept = 'S'
	AND
	c.concept_name NOT LIKE '%Maximum%'
	AND
	--C.concept_name NOT LIKE '%Microalbumin%'
	--AND
	--C.concept_name LIKE '%24%'
	--AND
	C.concept_name NOT LIKE '%presence%'
	--AND
	--C.concept_name NOT LIKE '%methemalbumin%'
	--AND
	--C.concept_name NOT LIKE '%Protein.total%'

GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	LOWER('Anion_Gap') [variable_name],
	NULL [variable_desc]
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    CA.ancestor_concept_id IN (4103762, 37037994, 37075673, 37066307)
	--AND
	--(CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'blood_sample')
	--OR (CR.concept_id_2 IS NULL AND (C.concept_name LIKE '%blood%' OR C.concept_name LIKE '%serum%' OR C.concept_name LIKE '%plasma%')))
	AND
	C.standard_concept = 'S'
	--AND
	--c.concept_name NOT LIKE '%Maximum%'
	--AND
	----C.concept_name NOT LIKE '%Microalbumin%'
	----AND
	----C.concept_name LIKE '%24%'
	----AND
	--C.concept_name NOT LIKE '%presence%'
	--AND
	--C.concept_name NOT LIKE '%methemalbumin%'
	--AND
	--C.concept_name NOT LIKE '%Protein.total%'

GO


INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	LOWER('APR_UR') [variable_name],
	NULL [variable_desc]
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    CA.ancestor_concept_id IN (4152996 )-- SNOMED Urine albumin Measurement
	AND
	(CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'urine_sample')
	OR (CR.concept_id_2 IS NULL AND C.concept_name LIKE '%urine%'))
	AND
	C.standard_concept = 'S'
	AND
	C.concept_name NOT LIKE '%Microalbumin%'
	AND
	C.concept_name NOT LIKE '%24%'
	AND
	C.concept_name NOT LIKE '%presence%'
	AND
	C.concept_name NOT LIKE '%methemalbumin%'
	AND
	C.concept_name LIKE '%Protein.total%'

GO

with partitioned as (
SELECT
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	[variable_name],
	[variable_desc],
	ROW_NUMBER() OVER(PARTITION BY concept_id ORDER BY  domain_id ASC) AS seq
FROM
(SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	LOWER('APR_UR_24H') [variable_name],
	NULL [variable_desc]
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    CA.ancestor_concept_id IN (4152996 )-- SNOMED Urine albumin Measurement
	--AND
	--(CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'urine_sample')
	--OR (CR.concept_id_2 IS NULL AND C.concept_name LIKE '%urine%'))
	AND
	C.standard_concept = 'S'
	AND
	C.concept_name NOT LIKE '%Microalbumin%'
	AND
	C.concept_name LIKE '%24%'
	AND
	C.concept_name NOT LIKE '%presence%'
	AND
	C.concept_name NOT LIKE '%methemalbumin%'
	AND
	C.concept_name LIKE '%Protein.total%'
UNION
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	NULL [ancestor_concept_id],
	LOWER('APR_UR_24H') [variable_name],
	NULL [variable_desc]
FROM
        VOCAB.CONCEPT C
WHERE
	Concept_id = 43055430) F
)
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	[variable_name],
	[variable_desc]
FROM
	partitioned
WHERE
	seq = 1
GO




INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	LOWER('AST') [variable_name],
	'Aspartate aminotransferase (AST)' [variable_desc]
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    CA.ancestor_concept_id IN (4263457 )-- SNOMED Aspartate aminotransferase measurement
	AND
	(CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'blood_sample')
	OR (CR.concept_id_2 IS NULL AND (C.concept_name LIKE '%blood%' OR C.concept_name LIKE '%serum%' OR C.concept_name LIKE '%plasma%')))
	AND
	C.standard_concept = 'S'
	AND
	c.concept_name NOT LIKE '%Maximum%'
	AND
	--C.concept_name NOT LIKE '%Microalbumin%'
	--AND
	--C.concept_name LIKE '%24%'
	--AND
	C.concept_name NOT LIKE '%presence%'
	--AND
	--C.concept_name NOT LIKE '%methemalbumin%'
	--AND
	--C.concept_name NOT LIKE '%Protein.total%'

GO


INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	LOWER('BASE_DEFICIT') [variable_name],
	NULL [variable_desc]
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    CA.ancestor_concept_id IN (4194291, 4095105)
	AND
	(CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'blood_sample')
	OR (CR.concept_id_2 IS NULL AND (C.concept_name LIKE '%blood%' OR C.concept_name LIKE '%serum%' OR C.concept_name LIKE '%plasma%')))
	AND
	C.standard_concept = 'S'
	AND
	c.concept_name LIKE '%deficit%'
	--AND
	--C.concept_name NOT LIKE '%Microalbumin%'
	--AND
	--C.concept_name LIKE '%24%'
	--AND
	--C.concept_name NOT LIKE '%presence%'
	--AND
	--C.concept_name NOT LIKE '%methemalbumin%'
	--AND
	--C.concept_name NOT LIKE '%Protein.total%'

GO


INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	LOWER('BASE_EXCESS') [variable_name],
	NULL [variable_desc]
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    CA.ancestor_concept_id IN (4194291, 4095105)
	AND
	(CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'blood_sample')
	OR (CR.concept_id_2 IS NULL AND (C.concept_name LIKE '%blood%' OR C.concept_name LIKE '%serum%' OR C.concept_name LIKE '%plasma%')))
	AND
	C.standard_concept = 'S'
	AND
	c.concept_name LIKE '%excess%'
	--AND
	--C.concept_name NOT LIKE '%Microalbumin%'
	--AND
	--C.concept_name LIKE '%24%'
	--AND
	--C.concept_name NOT LIKE '%presence%'
	--AND
	--C.concept_name NOT LIKE '%methemalbumin%'
	--AND
	--C.concept_name NOT LIKE '%Protein.total%'

GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	LOWER('Basophils') [variable_name],
	NULL [variable_desc]
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    ((ancestor_concept_id = 45876009 AND C.concept_name LIKE '%basophils%' AND C.concept_name NOT LIKE '%leukocytes%' AND C.concept_name NOT LIKE '%eosinophils%')
		OR
		ancestor_concept_id = 4172647
	)
	AND
	(CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'blood_sample')
	OR (CR.concept_id_2 IS NULL AND (C.concept_name LIKE '%blood%' OR C.concept_name LIKE '%serum%' OR C.concept_name LIKE '%plasma%')))
	AND
	C.standard_concept = 'S'
	AND
	C.concept_name LIKE '%Immature%'
	--AND
	--C.concept_name NOT LIKE '%presence%'
	--AND
	--C.concept_name NOT LIKE '%methemalbumin%'
	--AND
	--C.concept_name NOT LIKE '%Protein.total%'

GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	LOWER('serum_asparate') [variable_name],
	NULL [variable_desc]
FROM
	IC3_Variable_Lookup_Table_v3_beta
WHERE
	variable_name = 'AST'
	AND
	concept_name LIKE '%serum%'

GO


INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	LOWER('BUN') [variable_name],
	NULL [variable_desc]
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    ancestor_concept_id IN (4074649) -- BUN
	AND
	(CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'blood_sample')
	OR (CR.concept_id_2 IS NULL AND (C.concept_name LIKE '%blood%' OR C.concept_name LIKE '%serum%' OR C.concept_name LIKE '%plasma%')))
	AND
	C.standard_concept = 'S'
	AND
	c.concept_name NOT LIKE '%Creatinine%'
	--AND
	--C.concept_name NOT LIKE '%presence%'
	--AND
	--C.concept_name NOT LIKE '%methemalbumin%'
	--AND
	--C.concept_name NOT LIKE '%Protein.total%'

GO


with partitioned as (
SELECT
	*,
	ROW_NUMBER() OVER(PARTITION BY concept_id ORDER BY  domain_id ASC) AS seq
FROM
(SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	LOWER('CARBOXYHEM') [variable_name],
	NULL [variable_desc]
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    ancestor_concept_id IN (4017625) -- CARBOXYHEM quantitative
	AND
	(CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'blood_sample')
	OR (CR.concept_id_2 IS NULL AND (C.concept_name LIKE '%blood%' OR C.concept_name LIKE '%serum%' OR C.concept_name LIKE '%plasma%')))
	AND
	C.standard_concept = 'S'
	--AND
	--C.concept_name NOT LIKE '%presence%'
	--AND
	--C.concept_name NOT LIKE '%methemalbumin%'
	--AND
	--C.concept_name NOT LIKE '%Protein.total%'
UNION
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	NULL [ancestor_concept_id],
	LOWER('CARBOXYHEM') [variable_name],
	NULL [variable_desc]
FROM
	VOCAB.CONCEPT C WHERE concept_code IN (
		'71884-1',
		'71885-8',
		'71888-2',
		'71889-0',
		'71890-8') AND vocabulary_id = 'LOINC') t
)
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	[variable_name],
	[variable_desc]
FROM
	partitioned
WHERE
	seq = 1
GO


INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	LOWER('serum_CO2') [variable_name],
	'Partial Pressure of Carbon Dioxide in Blood' [variable_desc]
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    ancestor_concept_id IN (4097882, -- Measurement of partial pressure of carbon dioxide in blood
							40652760) -- LOINC Carbon dioxide|Substance Concentration|Moment in time|Blood, Serum or Plasma
	AND
	(CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'blood_sample')
	OR (CR.concept_id_2 IS NULL AND (C.concept_name LIKE '%blood%' OR C.concept_name LIKE '%serum%' OR C.concept_name LIKE '%plasma%' OR C.concept_name LIKE '%capillary%' OR C.concept_name LIKE '%venous%')))
	AND
	C.standard_concept = 'S'
	--AND
	--C.concept_name NOT LIKE '%presence%'
	--AND
	--C.concept_name NOT LIKE '%methemalbumin%'
	--AND
	--C.concept_name NOT LIKE '%Protein.total%'

GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	LOWER('Glucose_UR') [variable_name],
	NULL [variable_desc]
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    CA.ancestor_concept_id IN (4149883 )-- SNOMED Urine Glucose Measurement
	AND
	(CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'urine_sample')
	OR (CR.concept_id_2 IS NULL AND C.concept_name LIKE '%urine%'))
	AND
	C.standard_concept = 'S'
	AND
	C.concept_name NOT LIKE '%presence%'
	AND
	C.concept_name NOT LIKE '%challenge%'
	AND
	C.concept_name NOT LIKE '%/time%'
	AND
	c.concept_name NOT LIKE '%hour%'
	AND
	c.concept_name NOT LIKE '%minute%'
	AND
	c.concept_name NOT LIKE '%self%'

GO




INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	LOWER('ESR') [variable_name],
	'Erythrocyte sedimentation rate measurement' [variable_desc]
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    ancestor_concept_id IN (4212065)
	AND
	(CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'blood_sample')
	OR (CR.concept_id_2 IS NULL)) --AND (C.concept_name LIKE '%blood%' OR C.concept_name LIKE '%serum%' OR C.concept_name LIKE '%plasma%' OR C.concept_name LIKE '%capillary%' OR C.concept_name LIKE '%venous%')))
	AND
	C.standard_concept = 'S'
	--AND
	--C.concept_name NOT LIKE '%presence%'
	--AND
	--C.concept_name NOT LIKE '%methemalbumin%'
	--AND
	--C.concept_name NOT LIKE '%Protein.total%'

GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	LOWER('SERUM_HCO3') [variable_name],
	NULL [variable_desc]
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    ancestor_concept_id IN (4227915) -- SNOMED Bicarbonate measurement
	AND
	(CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'blood_sample')
	OR (CR.concept_id_2 IS NULL)) --AND (C.concept_name LIKE '%blood%' OR C.concept_name LIKE '%serum%' OR C.concept_name LIKE '%plasma%' OR C.concept_name LIKE '%capillary%' OR C.concept_name LIKE '%venous%')))
	AND
	C.standard_concept = 'S'
	AND
	 c.concept_name NOT LIKE '%base%'
    AND 
    c.concept_name NOT LIKE '%vasopressin'
	AND
	c.concept_id NOT IN (4193415, -- TOTAL blood CO2
						 40485080, --Urea, electrolytes and creatinine measurement
						 40483205) -- Urea, electrolytes and glucose measurement
	--AND
	--C.concept_name NOT LIKE '%presence%'
	--AND
	--C.concept_name NOT LIKE '%methemalbumin%'
	--AND
	--C.concept_name NOT LIKE '%Protein.total%'

GO


INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	LOWER('hgb') [variable_name],
	NULL [variable_desc]
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    ancestor_concept_id IN (37029074, 37054839, 37072252) -- hgb
	AND
	(CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'blood_sample')
	OR (CR.concept_id_2 IS NULL)) --AND (C.concept_name LIKE '%blood%' OR C.concept_name LIKE '%serum%' OR C.concept_name LIKE '%plasma%' OR C.concept_name LIKE '%capillary%' OR C.concept_name LIKE '%venous%')))
	AND
	C.standard_concept = 'S'
    AND 
    C.concept_name NOT LIKE '%presence%'
	--AND
	--C.concept_name NOT LIKE '%presence%'
	--AND
	--C.concept_name NOT LIKE '%methemalbumin%'
	--AND
	--C.concept_name NOT LIKE '%Protein.total%'

GO

--- Labs from here down probably should be double checked for additional levels

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	LOWER('hgb_ur') [variable_name],
	NULL [variable_desc]
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    ancestor_concept_id IN (4016242, 37056155) -- hgb_ur
	AND
	(CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'urine_sample')
	OR (CR.concept_id_2 IS NULL)) --AND (C.concept_name LIKE '%blood%' OR C.concept_name LIKE '%serum%' OR C.concept_name LIKE '%plasma%' OR C.concept_name LIKE '%capillary%' OR C.concept_name LIKE '%venous%')))
	AND
	C.standard_concept = 'S'
    AND 
    C.concept_name NOT LIKE '%presence%'
	--AND
	--C.concept_name NOT LIKE '%presence%'
	--AND
	--C.concept_name NOT LIKE '%methemalbumin%'
	--AND
	--C.concept_name NOT LIKE '%Protein.total%'

GO

with partitioned as (
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	LOWER('serum_inr') [variable_name],
	NULL [variable_desc],
	ROW_NUMBER() OVER(PARTITION BY concept_id ORDER BY  domain_id ASC) AS seq
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    ancestor_concept_id IN (3032080, 4131379, 4261078, 37042344, 37074906, 37061141) -- serum_inr
	AND
	(CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'blood_sample')
	OR (CR.concept_id_2 IS NULL)) --AND (C.concept_name LIKE '%blood%' OR C.concept_name LIKE '%serum%' OR C.concept_name LIKE '%plasma%' OR C.concept_name LIKE '%capillary%' OR C.concept_name LIKE '%venous%')))
	AND
	C.standard_concept = 'S'

)
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	variable_name,
	[variable_desc]
FROM
	partitioned 
WHERE
	seq = 1 -- remove duplicate key value pairs
GO


with partitioned as (
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	LOWER('lymphocytes') [variable_name],
	NULL [variable_desc],
	ROW_NUMBER() OVER(PARTITION BY concept_id ORDER BY  domain_id ASC) AS seq
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    ca.ancestor_concept_id IN (3004327, 3003215, 40487382, 37208689, 4254663) -- lymphocytes
	AND
	(CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'blood_sample')
	OR (CR.concept_id_2 IS NULL)) --AND (C.concept_name LIKE '%blood%' OR C.concept_name LIKE '%serum%' OR C.concept_name LIKE '%plasma%' OR C.concept_name LIKE '%capillary%' OR C.concept_name LIKE '%venous%')))
	AND
	C.standard_concept = 'S'
	    
	AND
	c.standard_concept = 'S'
	AND
	c.concept_name NOT LIKE '%t cell%'
	AND
	c.concept_name NOT LIKE '%b cell%'
	AND
	c.concept_name NOT LIKE '%killer%'
	AND
	c.concept_name NOT LIKE '%B lymph%'
	AND
	c.concept_name NOT LIKE '%T lymph%'
	AND
	c.concept_name NOT LIKE '%antigen%'
	AND
	c.concept_name NOT LIKE '%reactive%'
	AND
	c.concept_name NOT LIKE '%percent%'


)
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	variable_name,
	[variable_desc]
FROM
	partitioned 
WHERE
	seq = 1 -- remove duplicate key value pairs
GO


with partitioned as (
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	LOWER('lymphocytes_per') [variable_name],
	NULL [variable_desc],
	ROW_NUMBER() OVER(PARTITION BY concept_id ORDER BY  domain_id ASC) AS seq
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    ca.ancestor_concept_id IN (3037511, 3038058, 37208690, 4156652) -- lymphocytes_per
	AND
	(CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'blood_sample')
	OR (CR.concept_id_2 IS NULL)) --AND (C.concept_name LIKE '%blood%' OR C.concept_name LIKE '%serum%' OR C.concept_name LIKE '%plasma%' OR C.concept_name LIKE '%capillary%' OR C.concept_name LIKE '%venous%')))
	AND
	C.standard_concept = 'S'
)
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	variable_name,
	[variable_desc]
FROM
	partitioned 
WHERE
	seq = 1 -- remove duplicate key value pairs
GO



with partitioned as (
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	LOWER('mch') [variable_name],
	NULL [variable_desc],
	ROW_NUMBER() OVER(PARTITION BY concept_id ORDER BY  domain_id ASC) AS seq
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    ca.ancestor_concept_id IN (3012030, 37398674, 37068065) -- mch
	AND
	(CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'blood_sample')
	OR (CR.concept_id_2 IS NULL)) --AND (C.concept_name LIKE '%blood%' OR C.concept_name LIKE '%serum%' OR C.concept_name LIKE '%plasma%' OR C.concept_name LIKE '%capillary%' OR C.concept_name LIKE '%venous%')))
	AND
	C.standard_concept = 'S'
)
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	variable_name,
	[variable_desc]
FROM
	partitioned 
WHERE
	seq = 1 -- remove duplicate key value pairs
GO

with partitioned as (
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	LOWER('mchc') [variable_name],
	NULL [variable_desc],
	ROW_NUMBER() OVER(PARTITION BY concept_id ORDER BY  domain_id ASC) AS seq
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    ca.ancestor_concept_id IN (3009744, 37045413, 40654759, 37393850) -- mchc
	AND
	(CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'blood_sample')
	OR (CR.concept_id_2 IS NULL)) --AND (C.concept_name LIKE '%blood%' OR C.concept_name LIKE '%serum%' OR C.concept_name LIKE '%plasma%' OR C.concept_name LIKE '%capillary%' OR C.concept_name LIKE '%venous%')))
	AND
	C.standard_concept = 'S'
)
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	variable_name,
	[variable_desc]
FROM
	partitioned 
WHERE
	seq = 1 -- remove duplicate key value pairs
GO


with partitioned as (
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	LOWER('mcv') [variable_name],
	NULL [variable_desc],
	ROW_NUMBER() OVER(PARTITION BY concept_id ORDER BY  domain_id ASC) AS seq
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    ca.ancestor_concept_id IN (3023599, 37065843, 37393851) -- mcv
	AND
	(CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'blood_sample')
	OR (CR.concept_id_2 IS NULL)) --AND (C.concept_name LIKE '%blood%' OR C.concept_name LIKE '%serum%' OR C.concept_name LIKE '%plasma%' OR C.concept_name LIKE '%capillary%' OR C.concept_name LIKE '%venous%')))
	AND
	C.standard_concept = 'S'
)
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	variable_name,
	[variable_desc]
FROM
	partitioned 
WHERE
	seq = 1 -- remove duplicate key value pairs
GO


with partitioned as (
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	LOWER('methhem') [variable_name],
	NULL [variable_desc],
	ROW_NUMBER() OVER(PARTITION BY concept_id ORDER BY  domain_id ASC) AS seq
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    ca.ancestor_concept_id IN (40481360, 37066808, 3025543, 3007930, 3006217, 3025543, 3024889, 3025412, 3031282, 42869634, 42869635, 42869637, 42869638, 43054993) -- methhem
	AND
	c.concept_name NOT LIKE '%urine%' 
	AND 
	c.concept_name NOT LIKE '%cord%'
	AND
	c.concept_name NOT LIKE '%cerebral%'
	AND
	c.concept_name NOT LIKE '%qualitative%'
	AND
	c.concept_name NOT LIKE '%per day%'
	AND
	c.standard_concept = 'S'
	AND
	c.concept_name NOT LIKE '%/hemoglobin%'
	AND
	c.concept_name NOT LIKE 'hemoglobin%'
	AND
	c.concept_name NOT LIKE '%presence%'
	AND
	(CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'blood_sample')
	OR (CR.concept_id_2 IS NULL)) --AND (C.concept_name LIKE '%blood%' OR C.concept_name LIKE '%serum%' OR C.concept_name LIKE '%plasma%' OR C.concept_name LIKE '%capillary%' OR C.concept_name LIKE '%venous%')))
	AND
	C.standard_concept = 'S'
	AND
	C.concept_id NOT IN (4019553) -- Cyanmethemoglobin measurement
)
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	variable_name,
	[variable_desc]
FROM
	partitioned 
WHERE
	seq = 1 -- remove duplicate key value pairs
GO




with partitioned as (
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	LOWER('microalbumin_ur') [variable_name],
	NULL [variable_desc],
	ROW_NUMBER() OVER(PARTITION BY concept_id ORDER BY  domain_id ASC) AS seq
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    ca.ancestor_concept_id IN (4263307, 3000034) -- microalbumin_ur
    AND 
    c.concept_name LIKE '%uri%'
    AND 
    c.concept_name NOT LIKE '%hour%'
	AND
	(CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'urine_sample')
	OR (CR.concept_id_2 IS NULL)) --AND (C.concept_name LIKE '%blood%' OR C.concept_name LIKE '%serum%' OR C.concept_name LIKE '%plasma%' OR C.concept_name LIKE '%capillary%' OR C.concept_name LIKE '%venous%')))
	AND
	C.standard_concept = 'S'
)
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	variable_name,
	[variable_desc]
FROM
	partitioned 
WHERE
	seq = 1 -- remove duplicate key value pairs
GO


with partitioned as (
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	LOWER('mpv') [variable_name],
	NULL [variable_desc],
	ROW_NUMBER() OVER(PARTITION BY concept_id ORDER BY  domain_id ASC) AS seq
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    ca.ancestor_concept_id IN (3043111, 4192368, 40302423, 40452035, 40267653, 4192368) -- mpv
	AND
	(CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'blood_sample')
	OR (CR.concept_id_2 IS NULL)) --AND (C.concept_name LIKE '%blood%' OR C.concept_name LIKE '%serum%' OR C.concept_name LIKE '%plasma%' OR C.concept_name LIKE '%capillary%' OR C.concept_name LIKE '%venous%')))
	AND
	C.standard_concept = 'S'
)
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	variable_name,
	[variable_desc]
FROM
	partitioned 
WHERE
	seq = 1 -- remove duplicate key value pairs
GO



with partitioned as (
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	LOWER('neutrophil') [variable_name],
	NULL [variable_desc],
	ROW_NUMBER() OVER(PARTITION BY concept_id ORDER BY  domain_id ASC) AS seq
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    ca.ancestor_concept_id IN (3013650, 3017501, 37208699) -- neutrophil
	AND
	(CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'blood_sample')
	OR (CR.concept_id_2 IS NULL)) --AND (C.concept_name LIKE '%blood%' OR C.concept_name LIKE '%serum%' OR C.concept_name LIKE '%plasma%' OR C.concept_name LIKE '%capillary%' OR C.concept_name LIKE '%venous%')))
	AND
	C.standard_concept = 'S'
)
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	variable_name,
	[variable_desc]
FROM
	partitioned 
WHERE
	seq = 1 -- remove duplicate key value pairs
GO



with partitioned as (
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	LOWER('neutrophil_per_band') [variable_name],
	NULL [variable_desc],
	ROW_NUMBER() OVER(PARTITION BY concept_id ORDER BY  domain_id ASC) AS seq
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    ca.ancestor_concept_id IN (3007591, 4100147, 3559251, 3559252) -- neutrophil_per_band
	AND
	(CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'blood_sample')
	OR (CR.concept_id_2 IS NULL)) --AND (C.concept_name LIKE '%blood%' OR C.concept_name LIKE '%serum%' OR C.concept_name LIKE '%plasma%' OR C.concept_name LIKE '%capillary%' OR C.concept_name LIKE '%venous%')))
	AND
	C.standard_concept = 'S'
)
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	variable_name,
	[variable_desc]
FROM
	partitioned 
WHERE
	seq = 1 -- remove duplicate key value pairs
GO



with partitioned as (
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	LOWER('O2SATA') [variable_name],
	NULL [variable_desc],
	ROW_NUMBER() OVER(PARTITION BY concept_id ORDER BY  domain_id ASC) AS seq
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    ca.ancestor_concept_id IN (3013502, 3016502, 3558251, 4013965) -- O2_saturation_arterial
	AND
	c.concept_name NOT LIKE '%oximetry%'
	AND
	(CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'blood_sample')
	OR (CR.concept_id_2 IS NULL)) --AND (C.concept_name LIKE '%blood%' OR C.concept_name LIKE '%serum%' OR C.concept_name LIKE '%plasma%' OR C.concept_name LIKE '%capillary%' OR C.concept_name LIKE '%venous%')))
	AND
	C.standard_concept = 'S'
)
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	variable_name,
	[variable_desc]
FROM
	partitioned 
WHERE
	seq = 1 -- remove duplicate key value pairs
GO



with partitioned as (
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	LOWER('PCO2A') [variable_name],
	NULL [variable_desc],
	ROW_NUMBER() OVER(PARTITION BY concept_id ORDER BY  domain_id ASC) AS seq
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
        ca.ancestor_concept_id IN (37037933, 3027946, 40308327, 4042749, 4042749) -- serum_co2_pp_arterial
	AND
	c.concept_name NOT LIKE '%moles%'
	AND
	(CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'blood_sample')
	OR (CR.concept_id_2 IS NULL)) --AND (C.concept_name LIKE '%blood%' OR C.concept_name LIKE '%serum%' OR C.concept_name LIKE '%plasma%' OR C.concept_name LIKE '%capillary%' OR C.concept_name LIKE '%venous%')))
	AND
	C.standard_concept = 'S'
)
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	variable_name,
	[variable_desc]
FROM
	partitioned 
WHERE
	seq = 1 -- remove duplicate key value pairs
GO



with partitioned as (
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	LOWER('PO2A') [variable_name],
	NULL [variable_desc],
	ROW_NUMBER() OVER(PARTITION BY concept_id ORDER BY  domain_id ASC) AS seq
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    ca.ancestor_concept_id IN (4094581, 3027801, 4103460, 40308324) -- serum_o2_pp_arterial
	AND
	c.vocabulary_id NOT LIKE '%CPT%'
	AND 
	c.concept_name NOT LIKE '%venous%'
	AND
	(CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'blood_sample')
	OR (CR.concept_id_2 IS NULL)) --AND (C.concept_name LIKE '%blood%' OR C.concept_name LIKE '%serum%' OR C.concept_name LIKE '%plasma%' OR C.concept_name LIKE '%capillary%' OR C.concept_name LIKE '%venous%')))
	AND
	C.standard_concept = 'S'
)
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	variable_name,
	[variable_desc]
FROM
	partitioned 
WHERE
	seq = 1 -- remove duplicate key value pairs
GO



with partitioned as (
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	LOWER('RBC') [variable_name],
	NULL [variable_desc],
	ROW_NUMBER() OVER(PARTITION BY concept_id ORDER BY  domain_id ASC) AS seq
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    ca.ancestor_concept_id IN (3026361, 37393849) -- RBC
	AND
	(c.concept_name LIKE '%red%' OR c.concept_name LIKE '%erythr%')
	AND 
	c.concept_name NOT LIKE '%hypochromic%'
	AND
	(CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'blood_sample')
	OR (CR.concept_id_2 IS NULL)) --AND (C.concept_name LIKE '%blood%' OR C.concept_name LIKE '%serum%' OR C.concept_name LIKE '%plasma%' OR C.concept_name LIKE '%capillary%' OR C.concept_name LIKE '%venous%')))
	AND
	C.standard_concept = 'S'
)
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	variable_name,
	[variable_desc]
FROM
	partitioned 
WHERE
	seq = 1 -- remove duplicate key value pairs
GO



with partitioned as (
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	LOWER('RBC_UR') [variable_name],
	NULL [variable_desc],
	ROW_NUMBER() OVER(PARTITION BY concept_id ORDER BY  domain_id ASC) AS seq
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    ca.ancestor_concept_id IN (4016235, 3009105) -- RBC_UR

	AND
	(CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'urine_sample')
	OR (CR.concept_id_2 IS NULL)) --AND (C.concept_name LIKE '%blood%' OR C.concept_name LIKE '%serum%' OR C.concept_name LIKE '%plasma%' OR C.concept_name LIKE '%capillary%' OR C.concept_name LIKE '%venous%')))
	AND
	C.standard_concept = 'S'
)
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	variable_name,
	[variable_desc]
FROM
	partitioned 
WHERE
	seq = 1 -- remove duplicate key value pairs
GO


with partitioned as (
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	LOWER('RDW') [variable_name],
	NULL [variable_desc],
	ROW_NUMBER() OVER(PARTITION BY concept_id ORDER BY  domain_id ASC) AS seq
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    ca.ancestor_concept_id IN (40451480, 4281085, 3019897, 37397924) -- RDW
	AND
	(CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'blood_sample')
	OR (CR.concept_id_2 IS NULL)) --AND (C.concept_name LIKE '%blood%' OR C.concept_name LIKE '%serum%' OR C.concept_name LIKE '%plasma%' OR C.concept_name LIKE '%capillary%' OR C.concept_name LIKE '%venous%')))
	AND
	C.standard_concept = 'S'
)
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	variable_name,
	[variable_desc]
FROM
	partitioned 
WHERE
	seq = 1 -- remove duplicate key value pairs
GO


with partitioned as (
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	LOWER('SODIUM') [variable_name],
	NULL [variable_desc],
	ROW_NUMBER() OVER(PARTITION BY concept_id ORDER BY  domain_id ASC) AS seq
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    ca.ancestor_concept_id IN (3000285, 3019550, 3043706, 37393103, 37392172) -- serum_na
	AND
	(CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'blood_sample')
	OR (CR.concept_id_2 IS NULL)) --AND (C.concept_name LIKE '%blood%' OR C.concept_name LIKE '%serum%' OR C.concept_name LIKE '%plasma%' OR C.concept_name LIKE '%capillary%' OR C.concept_name LIKE '%venous%')))
	AND
	C.standard_concept = 'S'
)
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	variable_name,
	[variable_desc]
FROM
	partitioned 
WHERE
	seq = 1 -- remove duplicate key value pairs
GO



with partitioned as (
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	LOWER('Troponin_I') [variable_name],
	NULL [variable_desc],
	ROW_NUMBER() OVER(PARTITION BY concept_id ORDER BY  domain_id ASC) AS seq
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    ca.ancestor_concept_id IN (3033745, 3032971, 4007805, 4010039) -- troponin_I
	AND
	(CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'blood_sample')
	OR (CR.concept_id_2 IS NULL)) --AND (C.concept_name LIKE '%blood%' OR C.concept_name LIKE '%serum%' OR C.concept_name LIKE '%plasma%' OR C.concept_name LIKE '%capillary%' OR C.concept_name LIKE '%venous%')))
	AND
	C.standard_concept = 'S'
)
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	variable_name,
	[variable_desc]
FROM
	partitioned 
WHERE
	seq = 1 -- remove duplicate key value pairs
GO



with partitioned as (
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	LOWER('Troponin_T') [variable_name],
	NULL [variable_desc],
	ROW_NUMBER() OVER(PARTITION BY concept_id ORDER BY  domain_id ASC) AS seq
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    ca.ancestor_concept_id IN (3019800, 40769783, 3048529, 3019572, 4005525, 4010038) -- troponin_T
	AND
	(CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'blood_sample')
	OR (CR.concept_id_2 IS NULL)) --AND (C.concept_name LIKE '%blood%' OR C.concept_name LIKE '%serum%' OR C.concept_name LIKE '%plasma%' OR C.concept_name LIKE '%capillary%' OR C.concept_name LIKE '%venous%')))
	AND
	C.standard_concept = 'S'
)
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	variable_name,
	[variable_desc]
FROM
	partitioned 
WHERE
	seq = 1 -- remove duplicate key value pairs
GO



with partitioned as (
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	LOWER('UACR') [variable_name],
	NULL [variable_desc],
	ROW_NUMBER() OVER(PARTITION BY concept_id ORDER BY  domain_id ASC) AS seq
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    ca.ancestor_concept_id IN (3034485, 37398781) -- urine albumin/Cr ratio
	AND
	(CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'urine_sample')
	OR (CR.concept_id_2 IS NULL)) --AND (C.concept_name LIKE '%blood%' OR C.concept_name LIKE '%serum%' OR C.concept_name LIKE '%plasma%' OR C.concept_name LIKE '%capillary%' OR C.concept_name LIKE '%venous%')))
	AND
	C.standard_concept = 'S'
	AND
	C.concept_name NOT LIKE '%micro%'
)
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	variable_name,
	[variable_desc]
FROM
	partitioned 
WHERE
	seq = 1 -- remove duplicate key value pairs
GO



with partitioned as (
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	LOWER('UAP_cat') [variable_name],
	NULL [variable_desc],
	ROW_NUMBER() OVER(PARTITION BY concept_id ORDER BY  domain_id ASC) AS seq
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    ca.ancestor_concept_id IN (3014051, 40760845) -- presence of protien, only LOINC value
	AND
	(CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'urine_sample')
	OR (CR.concept_id_2 IS NULL)) --AND (C.concept_name LIKE '%blood%' OR C.concept_name LIKE '%serum%' OR C.concept_name LIKE '%plasma%' OR C.concept_name LIKE '%capillary%' OR C.concept_name LIKE '%venous%')))
	AND
	C.standard_concept = 'S'
)
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	variable_name,
	[variable_desc]
FROM
	partitioned 
WHERE
	seq = 1 -- remove duplicate key value pairs
GO



with partitioned as (
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	LOWER('UMACR') [variable_name],
	NULL [variable_desc],
	ROW_NUMBER() OVER(PARTITION BY concept_id ORDER BY  domain_id ASC) AS seq
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    ca.ancestor_concept_id IN (37398777, 3001802) -- microalbumin/cr ratio
	AND
	(CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'urine_sample')
	OR (CR.concept_id_2 IS NULL)) --AND (C.concept_name LIKE '%blood%' OR C.concept_name LIKE '%serum%' OR C.concept_name LIKE '%plasma%' OR C.concept_name LIKE '%capillary%' OR C.concept_name LIKE '%venous%')))
	AND
	C.standard_concept = 'S'
)
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	variable_name,
	[variable_desc]
FROM
	partitioned 
WHERE
	seq = 1 -- remove duplicate key value pairs
GO



with partitioned as (
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	LOWER('UNCR') [variable_name],
	NULL [variable_desc],
	ROW_NUMBER() OVER(PARTITION BY concept_id ORDER BY  domain_id ASC) AS seq
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    ca.ancestor_concept_id IN (3018311, 4112223) -- urea nitrogen cr mass ration in serum
	AND
	(CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'blood_sample')
	OR (CR.concept_id_2 IS NULL)) --AND (C.concept_name LIKE '%blood%' OR C.concept_name LIKE '%serum%' OR C.concept_name LIKE '%plasma%' OR C.concept_name LIKE '%capillary%' OR C.concept_name LIKE '%venous%')))
	AND
	C.standard_concept = 'S'
)
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	variable_name,
	[variable_desc]
FROM
	partitioned 
WHERE
	seq = 1 -- remove duplicate key value pairs
GO



with partitioned as (
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	LOWER('WBC') [variable_name],
	NULL [variable_desc],
	ROW_NUMBER() OVER(PARTITION BY concept_id ORDER BY  domain_id ASC) AS seq
FROM
        VOCAB.CONCEPT_ANCESTOR ca
        INNER JOIN VOCAB.CONCEPT c on c.concept_id = ca.descendant_concept_id
		LEFT JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = c.concept_id AND Cr.relationship_id IN ('has system', 'Has specimen'))
WHERE
    ca.ancestor_concept_id IN (3000905, 3010813, 4212899) -- wbc
	AND
	(CR.concept_id_2 IN (SELECT concept_id FROM dbo.IC3_Variable_Lookup_Table_v3_beta WHERE variable_name = 'blood_sample')
	OR (CR.concept_id_2 IS NULL)) --AND (C.concept_name LIKE '%blood%' OR C.concept_name LIKE '%serum%' OR C.concept_name LIKE '%plasma%' OR C.concept_name LIKE '%capillary%' OR C.concept_name LIKE '%venous%')))
	AND
	C.standard_concept = 'S'
)
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	variable_name,
	[variable_desc]
FROM
	partitioned 
WHERE
	seq = 1 -- remove duplicate key value pairs
GO


with partitioned as (
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	CASE WHEN ca.ancestor_concept_id IN (4023217, 8717, 38004515) THEN 'ward'
		 WHEN ca.ancestor_concept_id IN (4021520, 9203) THEN 'emergency_department'
		 WHEN ca.ancestor_concept_id = 4331156 THEN 'procedure_suite'
		 WHEN ca.ancestor_concept_id = 4021813 THEN 'operating_room'
		 WHEN ca.ancestor_concept_id = 4134563 THEN 'post_anesthesia_care_unit'
		 WHEN ca.ancestor_concept_id = 4305525 THEN 'intermediate_care/stepdown_unit'
		 WHEN ca.ancestor_concept_id = 4134848 THEN 'or_holding_unit'
		 WHEN ca.ancestor_concept_id = 4139502 THEN 'home'
		 WHEN ca.ancestor_concept_id = 4021524 THEN 'morgue' END	as variable_name,
		 NULL [variable_desc],
	ROW_NUMBER() OVER(PARTITION BY concept_id, CASE WHEN ca.ancestor_concept_id IN (4023217, 8717, 38004515) THEN 'ward'
		 WHEN ca.ancestor_concept_id IN (4021520, 9203) THEN 'emergency_department'
		 WHEN ca.ancestor_concept_id = 4331156 THEN 'procedure_suite'
		 WHEN ca.ancestor_concept_id = 4021813 THEN 'operating_room'
		 WHEN ca.ancestor_concept_id = 4134563 THEN 'post_anesthesia_care_unit'
		 WHEN ca.ancestor_concept_id = 4305525 THEN 'intermediate_care/stepdown_unit'
		 WHEN ca.ancestor_concept_id = 4134848 THEN 'or_holding_unit'
		 WHEN ca.ancestor_concept_id = 4139502 THEN 'home'
		 WHEN ca.ancestor_concept_id = 4021524 THEN 'morgue' END ORDER BY  domain_id ASC) AS seq
FROM 
	VOCAB.CONCEPT c
	INNER JOIN VOCAB.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
WHERE
	standard_concept = 'S'
	AND
	ca.ancestor_concept_id IN (
								4023217, 8717, 38004515, --Ward	
								4021520, 9203, -- ED
								4331156, -- Procedure Suite
								4021813, -- Operating Room
								4134563, -- PACU
								4305525, -- IMC
								4134848, --OR HOLDING UNIT
								4139502, -- HOME
								4021524 -- MORGUE
								)
	AND
	c.concept_name NOT LIKE '%nonmedical%'
	AND
	c.concept_id NOT IN (4075631) -- location inside building
	AND
	c.concept_id NOT IN (SELECT descendant_concept_id FROM VOCAB.CONCEPT_ANCESTOR ca1 WHERE ca1.ancestor_concept_id IN (32037, -- Intensive Care
								-- 581379, -- Inpatient Critical Care Facility
								4148981)  -- Intesive care unit
	)
)
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    CASE WHEN variable_name IN ('ward', 'emergency_department', 'procedure_suite', 'operating_room',
								'post_anesthesia_care_unit', 'intermediate_care/stepdown_unit', 'or_holding_unit') THEN 'visit_detail' ELSE 'visit_occurrence' END [domain_id],
	[ancestor_concept_id],
	variable_name,
	[variable_desc]
FROM
	partitioned 
WHERE
	seq = 1 -- remove duplicate key value pairs
GO



INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	NULL [ancestor_concept_id],
     CASE WHEN concept_id = 2000000027 THEN 'surgery'
			WHEN concept_id = 2000000030	THEN 'sched_post_op_location'
			WHEN concept_id = 2000000031	THEN 'sched_room'
			WHEN concept_id = 2000000032	THEN 'sched_trauma_room_y_n'
			WHEN concept_id = 2000000042	THEN 'sched_primary_procedure'
			WHEN concept_id = 4162211	THEN 'sched_start_datetime'
			WHEN concept_id = 2000000043 THEN 'sched_surgeon_provider_id'
			WHEN concept_id = 4199571 THEN 'asa_score' END [variable_name],
			NULL [variable_desc]
FROM
	VOCAB.CONCEPT C
WHERE
 concept_id IN (2000000027,  2000000030, 2000000031, 2000000032, 2000000042, 4162211, 2000000043, 4199571)
GO





INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	NULL [ancestor_concept_id],
    'gcs_eye_score' [variable_name],
			NULL [variable_desc]
FROM
	VOCAB.CONCEPT C
WHERE
	concept_id IN (4084277, 3016335)
GO


INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	'rbc_transfusion' [variable_name],
			NULL [variable_desc]
FROM
   VOCAB.CONCEPT_ANCESTOR ca
   INNER JOIN VOCAB.CONCEPT c on ca.descendant_concept_id = c.concept_id
WHERE
   ca.ancestor_concept_id IN (4054726) -- Red blood cells, blood product
GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	[ancestor_concept_id],
	'rbc_transfusion' [variable_name],
			NULL [variable_desc]
FROM
   VOCAB.CONCEPT_ANCESTOR ca
   INNER JOIN VOCAB.CONCEPT c on ca.descendant_concept_id = c.concept_id
WHERE
   ca.ancestor_concept_id IN (45875997) -- Blood Product
	AND
	c.concept_name LIKE '%given%'
	AND
	(c.concept_name LIKE '%red blood%' OR c.concept_name LIKE '%rbc%' OR c.concept_name LIKE '%erythrocyte%')
GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	ancestor_concept_id,
	'cpr' [variable_name],
	NULL [variable_desc]
FROM
   VOCAB.CONCEPT_ANCESTOR ca
   INNER JOIN VOCAB.CONCEPT c on ca.descendant_concept_id = c.concept_id
WHERE
   ca.ancestor_concept_id IN (4320484) -- Cardiac resuscitation
							   
GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	ancestor_concept_id,
	'primary_procedure' [variable_name],
	NULL [variable_desc]
FROM
   VOCAB.CONCEPT_ANCESTOR ca
   INNER JOIN VOCAB.CONCEPT c on ca.descendant_concept_id = c.concept_id
WHERE
   ca.ancestor_concept_id IN (4161019) -- Primary Procedure
GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	ancestor_concept_id,
	'intubation' [variable_name],
	NULL [variable_desc]
FROM
   VOCAB.CONCEPT_ANCESTOR ca
   INNER JOIN VOCAB.CONCEPT c on ca.descendant_concept_id = c.concept_id
WHERE
   ca.ancestor_concept_id IN (4013354) -- Insertion of endotracheal tube
	AND
	(c.concept_name NOT LIKE '%nas%'
	AND
	c.concept_name NOT LIKE '%glos%'
	AND
	c.concept_name NOT LIKE '%glot%')

GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	ancestor_concept_id,
	'arterial_catheter' [variable_name],
	NULL [variable_desc]
FROM
   VOCAB.CONCEPT_ANCESTOR ca
   INNER JOIN VOCAB.CONCEPT c on ca.descendant_concept_id = c.concept_id
WHERE
   ca.ancestor_concept_id IN (4213288) -- Insertion of catheter into artery
GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	ancestor_concept_id,
	'central_venous_catheter' [variable_name],
	NULL [variable_desc]
FROM
   VOCAB.CONCEPT_ANCESTOR ca
   INNER JOIN VOCAB.CONCEPT c on ca.descendant_concept_id = c.concept_id
WHERE
   ca.ancestor_concept_id IN (4052413) -- Central venous cannula insertion
GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	ancestor_concept_id,
	'pulmonary_artery_catheter' [variable_name],
	NULL [variable_desc]
FROM
   VOCAB.CONCEPT_ANCESTOR ca
   INNER JOIN VOCAB.CONCEPT c on ca.descendant_concept_id = c.concept_id
WHERE
   ca.ancestor_concept_id IN (4305794, -- Introduction of catheter into pulmonary artery
								2313886) -- Insertion and placement of flow directed catheter (eg, Swan-Ganz) for monitoring purposes
GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
	C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	ancestor_concept_id,
	'bronchoscopy' [variable_name],
	NULL [variable_desc]
FROM
   VOCAB.CONCEPT_ANCESTOR ca
   INNER JOIN VOCAB.CONCEPT c on ca.descendant_concept_id = c.concept_id
WHERE
   ca.ancestor_concept_id IN (4032404) -- Bronchoscopy
GO


INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	ancestor_concept_id,
	'cardioversion_electric' [variable_name],
	NULL [variable_desc]
FROM
   VOCAB.CONCEPT_ANCESTOR ca
   INNER JOIN VOCAB.CONCEPT c on ca.descendant_concept_id = c.concept_id
WHERE
   ca.ancestor_concept_id IN (4078793) -- Direct current cardioversion
GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	ancestor_concept_id,
	'chest_tube' [variable_name],
	NULL [variable_desc]
FROM
   VOCAB.CONCEPT_ANCESTOR ca
   INNER JOIN VOCAB.CONCEPT c on ca.descendant_concept_id = c.concept_id
WHERE
   ca.ancestor_concept_id IN (4141919) -- Insertion of pleural tube drain
	AND
	c.concept_id <> 2108503
GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	NULL ancestor_concept_id,
	'sofa_score' [variable_name],
	NULL [variable_desc]
FROM
   VOCAB.CONCEPT c
WHERE
   concept_id IN (36684937, -- Sequential Organ Failure Assessment score
   			   37394663, -- SOFA (Sequential Organ Failure Assessment) score
   			   1616852, -- SOFA Total Score
   			   1616328) -- Sequential Organ Failure Assessment SOFA
GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	NULL ancestor_concept_id,
	'resp_sofa_subscore' [variable_name],
	NULL [variable_desc]
FROM
   VOCAB.CONCEPT c
WHERE
   concept_id IN (1616907)
GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	NULL ancestor_concept_id,
	'cns_sofa_subscore' [variable_name],
	NULL [variable_desc]
FROM
   VOCAB.CONCEPT c
WHERE
   concept_id IN (1616439)
GO

-- LEFT OFF HERE

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	NULL ancestor_concept_id,
	'cardio_sofa_subscore' [variable_name],
	NULL [variable_desc]
FROM
   VOCAB.CONCEPT c
WHERE
   concept_id IN (1617534)
GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	NULL ancestor_concept_id,
	'liver_sofa_subscore' [variable_name],
	NULL [variable_desc]
FROM
   VOCAB.CONCEPT c
WHERE
   concept_id IN (1617043)
GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	NULL ancestor_concept_id,
	'coag_sofa_subscore' [variable_name],
	NULL [variable_desc]
FROM
   VOCAB.CONCEPT c
WHERE
   concept_id IN (1616896)
GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	NULL ancestor_concept_id,
	'renal_sofa_subscore' [variable_name],
	NULL [variable_desc]
FROM
   VOCAB.CONCEPT c
WHERE
   concept_id IN (1616355)
GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	NULL ancestor_concept_id,
	'seen_in_ed_yn' [variable_name],
	NULL [variable_desc]
FROM
   VOCAB.CONCEPT c
WHERE
   concept_id IN (262)
GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	NULL ancestor_concept_id,
	'gcs_score' [variable_name],
	NULL [variable_desc]
FROM
   VOCAB.CONCEPT c
WHERE
   concept_id IN (4093836, 4296538, 3032652, 3007194)
GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	NULL ancestor_concept_id,
	'gcs_motor_score' [variable_name],
	NULL [variable_desc]
FROM
   VOCAB.CONCEPT c
WHERE
   concept_id IN (3008223, 4083352)
GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	NULL ancestor_concept_id,
	'gcs_verbal_score' [variable_name],
	NULL [variable_desc]
FROM
   VOCAB.CONCEPT c
WHERE
   concept_id IN (3009094, 4084912)
GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	ancestor_concept_id,
	'pao2' [variable_name],
	NULL [variable_desc]
FROM
	VOCAB.CONCEPT_ANCESTOR CA
   INNER JOIN VOCAB.CONCEPT c on CA.descendant_concept_id = C.concept_id
WHERE
   ca.ancestor_concept_id IN (37392673, -- Arterial O2 level
   						   3027801, -- Oxygen [Partial pressure] in Arterial blood
   						   3022803 -- Oxygen [Partial pressure] adjusted to patient's actual temperature in Arterial blood
   						   )

GO


INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
    drug_concept_id [concept_id],
    drug_concept_code [concept_code],
    drug_name [concept_name],
     [vocabulary_id],
	concept_class_id,
    [domain_id],
	NULL ancestor_concept_id,
	ingredient_category [variable_name],
	NULL [variable_desc]
FROM
	dbo.IC3_DRUG_LOOKUP_TABLE_v2 l
	INNER JOIN VOCAB.CONCEPT C on C.concept_id = l.drug_concept_id

GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT [concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	NULL [ancestor_concept_id],
	LOWER(source_value) [variable_name],
	NULL [variable_desc]
FROM
	[PRISMAP].[OMOP_MAPPING] m
  INNER JOIN VOCAB.CONCEPT C on C.concept_id = m.standard_concept_id
  WHERE
	source_column_name LIKE '%name%'
	AND
	standard_concept_id <> 0
	AND
	LOWER(source_value) NOT IN (SELECT DISTINCT variable_name FROM dbo.IC3_Variable_Lookup_Table_v3_beta)
GO


INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	ancestor_concept_id,
	'cv_mi' [variable_name],
	NULL [variable_desc]
FROM
	VOCAB.CONCEPT_ANCESTOR CA
   INNER JOIN VOCAB.CONCEPT c on CA.descendant_concept_id = C.concept_id
WHERE
   ca.ancestor_concept_id IN (4329847) -- Myocardial Infarction concept chosen based on OHDSI implementation of CCI

GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	ancestor_concept_id,
	'pvd' [variable_name],
	NULL [variable_desc]
FROM
	VOCAB.CONCEPT_ANCESTOR CA
   INNER JOIN VOCAB.CONCEPT c on CA.descendant_concept_id = C.concept_id
WHERE
   ca.ancestor_concept_id IN (321052) -- Peripheral Vascular Disease concept chosen based on OHDSI implementation of CCI

GO

with partitioned as (
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	ancestor_concept_id,
	'cerebrovascular_disease' [variable_name],
	NULL [variable_desc],
	ROW_NUMBER() OVER(PARTITION BY concept_id ORDER BY  domain_id ASC) AS seq
FROM
	VOCAB.CONCEPT_ANCESTOR CA
   INNER JOIN VOCAB.CONCEPT c on CA.descendant_concept_id = C.concept_id
WHERE
   ca.ancestor_concept_id IN (381591, 434056) -- Cerebrovascular disease concepts chosen based on OHDSI implementation of CCI
)
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	variable_name,
	[variable_desc]
FROM
	partitioned 
WHERE
	seq = 1 -- remove duplicate key value pairs
GO
	

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	ancestor_concept_id,
	'dementia' [variable_name],
	NULL [variable_desc]
FROM
	VOCAB.CONCEPT_ANCESTOR CA
   INNER JOIN VOCAB.CONCEPT c on CA.descendant_concept_id = C.concept_id
WHERE
   ca.ancestor_concept_id IN (4182210) -- Dementia concept chosen based on OHDSI implementation of CCI

GO


INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	ancestor_concept_id,
	'chronic_pulmonary_disease' [variable_name],
	NULL [variable_desc]
FROM
	VOCAB.CONCEPT_ANCESTOR CA
   INNER JOIN VOCAB.CONCEPT c on CA.descendant_concept_id = C.concept_id
WHERE
   ca.ancestor_concept_id IN (4063381) -- Chronic pulmonary disease concept chosen based on OHDSI implementation of CCI

GO


with partitioned as (
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	ancestor_concept_id,
	'rheumatologic_disease' [variable_name],
	NULL [variable_desc],
	ROW_NUMBER() OVER(PARTITION BY concept_id ORDER BY  domain_id ASC) AS seq
FROM
	VOCAB.CONCEPT_ANCESTOR CA
   INNER JOIN VOCAB.CONCEPT c on CA.descendant_concept_id = C.concept_id
WHERE
   ca.ancestor_concept_id IN (257628, 134442, 80800, 80809, 256197, 255348) -- Rheumatologic disease concepts chosen based on OHDSI implementation of CCI
)
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	variable_name,
	[variable_desc]
FROM
	partitioned 
WHERE
	seq = 1 -- remove duplicate key value pairs
GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	ancestor_concept_id,
	'pud' [variable_name],
	NULL [variable_desc]
FROM
	VOCAB.CONCEPT_ANCESTOR CA
   INNER JOIN VOCAB.CONCEPT c on CA.descendant_concept_id = C.concept_id
WHERE
   ca.ancestor_concept_id IN (4247120) -- Peptic ulcer disease concept chosen based on OHDSI implementation of CCI

GO


with partitioned as (
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	ancestor_concept_id,
	'mild_liver_disease' [variable_name],
	NULL [variable_desc],
	ROW_NUMBER() OVER(PARTITION BY concept_id ORDER BY  domain_id ASC) AS seq
FROM
	VOCAB.CONCEPT_ANCESTOR CA
   INNER JOIN VOCAB.CONCEPT c on CA.descendant_concept_id = C.concept_id
WHERE
   ca.ancestor_concept_id IN (4064161, 4212540) -- Mild liver disease concepts chosen based on OHDSI implementation of CCI
)
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	variable_name,
	[variable_desc]
FROM
	partitioned 
WHERE
	seq = 1 -- remove duplicate key value pairs
GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	ancestor_concept_id,
	'diabetes_mild_to_moderate' [variable_name],
	NULL [variable_desc]
FROM
	VOCAB.CONCEPT_ANCESTOR CA
   INNER JOIN VOCAB.CONCEPT c on CA.descendant_concept_id = C.concept_id
WHERE
   ca.ancestor_concept_id IN (201820) -- Diabetes (mild to moderate) concept chosen based on OHDSI implementation of CCI

GO

--Diabetes with chronic complications
with partitioned as (
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	ancestor_concept_id,
	'diabetes_with_chronic_complications' [variable_name],
	NULL [variable_desc],
	ROW_NUMBER() OVER(PARTITION BY concept_id ORDER BY  domain_id ASC) AS seq
FROM
	VOCAB.CONCEPT_ANCESTOR CA
   INNER JOIN VOCAB.CONCEPT c on CA.descendant_concept_id = C.concept_id
WHERE
   ca.ancestor_concept_id IN (443767, 442793) -- Diabetes with chronic complications concepts chosen based on OHDSI implementation of CCI
)
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	variable_name,
	NULL [variable_desc]
FROM
	partitioned 
WHERE
	seq = 1 -- remove duplicate key value pairs
GO



--Hemiplegia or paraplegia
with partitioned as (
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	ancestor_concept_id,
	'hemiplegia_or_paraplegia' [variable_name],
	NULL [variable_desc],
	ROW_NUMBER() OVER(PARTITION BY concept_id ORDER BY  domain_id ASC) AS seq
FROM
	VOCAB.CONCEPT_ANCESTOR CA
   INNER JOIN VOCAB.CONCEPT c on CA.descendant_concept_id = C.concept_id
WHERE
   ca.ancestor_concept_id IN (192606, 374022) -- Hemoplegia or paralegia concepts chosen based on OHDSI implementation of CCI
)
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	variable_name,
	NULL [variable_desc]
FROM
	partitioned 
WHERE
	seq = 1 -- remove duplicate key value pairs
GO

--Renal disease
with partitioned as (
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	ancestor_concept_id,
	'renal_disease' [variable_name],
	NULL [variable_desc],
	ROW_NUMBER() OVER(PARTITION BY concept_id ORDER BY  domain_id ASC) AS seq
FROM
	VOCAB.CONCEPT_ANCESTOR CA
   INNER JOIN VOCAB.CONCEPT c on CA.descendant_concept_id = C.concept_id
WHERE
   ca.ancestor_concept_id IN (4030518) -- Renal disease concepts chosen based on OHDSI implementation of CCI
)
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	variable_name,
	[variable_desc]
FROM
	partitioned 
WHERE
	seq = 1 -- remove duplicate key value pairs
GO


--Any malignancy
with partitioned as (
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	ancestor_concept_id,
	'any_malignancy' [variable_name],
	NULL [variable_desc],
	ROW_NUMBER() OVER(PARTITION BY concept_id ORDER BY  domain_id ASC) AS seq
FROM
	VOCAB.CONCEPT_ANCESTOR CA
   INNER JOIN VOCAB.CONCEPT c on CA.descendant_concept_id = C.concept_id
WHERE
   ca.ancestor_concept_id IN (443392) -- Any malignancy concepts chosen based on OHDSI implementation of CCI
)
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	variable_name,
	[variable_desc]
FROM
	partitioned 
WHERE
	seq = 1 -- remove duplicate key value pairs
GO



--Moderate to severe liver disease
with partitioned as (
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	ancestor_concept_id,
	'moderate_to_severe_liver_disease' [variable_name],
	NULL [variable_desc],
	ROW_NUMBER() OVER(PARTITION BY concept_id ORDER BY  domain_id ASC) AS seq
FROM
	VOCAB.CONCEPT_ANCESTOR CA
   INNER JOIN VOCAB.CONCEPT c on CA.descendant_concept_id = C.concept_id
WHERE
   ca.ancestor_concept_id IN (4245975, 4029488, 192680, 24966) -- Moderate to severe liver disease concepts chosen based on OHDSI implementation of CCI
)
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	variable_name,
	[variable_desc]
FROM
	partitioned 
WHERE
	seq = 1 -- remove duplicate key value pairs
GO

--Metastatic solid tumor
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	ancestor_concept_id,
	'metastatic_solid_tumor' [variable_name],
	NULL [variable_desc]
FROM
	VOCAB.CONCEPT_ANCESTOR CA
   INNER JOIN VOCAB.CONCEPT c on CA.descendant_concept_id = C.concept_id
WHERE
   ca.ancestor_concept_id IN (432851) -- Metastatic solid tumor concept chosen based on OHDSI implementation of CCI

GO

-- Cardiac arrhythmia
with partitioned as (
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	ancestor_concept_id,
	'cardiac_arrhythmia' [variable_name],
	NULL [variable_desc],
	ROW_NUMBER() OVER(PARTITION BY concept_id ORDER BY  domain_id ASC) AS seq
FROM
	VOCAB.CONCEPT_ANCESTOR CA
   INNER JOIN VOCAB.CONCEPT c on CA.descendant_concept_id = C.concept_id
WHERE
   ca.ancestor_concept_id IN (44784217, -- SNOMED Cardiac arrhythmia
							  4262984, --Abnormal cardiac rate SNOMED
							  4169095, -- Bradycardia SNOMED
							  4262985, -- Irregular heart rate SNOMED
							  444070, --Tachycardia SNOMED
							  4155081) --Disorder of cardiac pacemaker system SNOMED
)
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	variable_name,
	[variable_desc]
FROM
	partitioned 
WHERE
	seq = 1 -- remove duplicate key value pairs
GO




--AIDS
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	ancestor_concept_id,
	'aids' [variable_name],
	NULL [variable_desc]
FROM
	VOCAB.CONCEPT_ANCESTOR CA
   INNER JOIN VOCAB.CONCEPT c on CA.descendant_concept_id = C.concept_id
WHERE
   ca.ancestor_concept_id IN (439727) -- AIDS concept chosen based on OHDSI implementation of CCI

GO

-- ETOH Abuse
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	ancestor_concept_id,
	'alcohol_abuse' [variable_name],
	NULL [variable_desc]
FROM
	VOCAB.CONCEPT_ANCESTOR CA
   INNER JOIN VOCAB.CONCEPT c on CA.descendant_concept_id = C.concept_id
WHERE
   ca.ancestor_concept_id IN (433753) -- SNOMED ETOH abuse

GO

-- Nutritional Anemia (non-iron)
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	ancestor_concept_id,
	'anemia_non-iron_deficiency' [variable_name],
	NULL [variable_desc]
FROM
	VOCAB.CONCEPT_ANCESTOR CA
   INNER JOIN VOCAB.CONCEPT c on CA.descendant_concept_id = C.concept_id
WHERE
   ca.ancestor_concept_id IN (4280354) -- SNOMED Nutritional Anemia
   AND
   ca.descendant_concept_id NOT IN (SELECT DISTINCT descendant_concept_id FROM VOCAB.CONCEPT_ANCESTOR WHERE ancestor_concept_id = 436659) -- IRON deficiency Anemia

GO

-- Nutritional Anemia iron/chronic blood losss
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	ancestor_concept_id,
	'anemia_iron_deficiency_chronic_blood_loss' [variable_name],
	NULL [variable_desc]
FROM
	VOCAB.CONCEPT_ANCESTOR CA
   INNER JOIN VOCAB.CONCEPT c on CA.descendant_concept_id = C.concept_id
WHERE
   ca.ancestor_concept_id IN (436659, 432875) -- SNOMED IRON deficiency Anemia, Anemia due to chronic blood loss

GO

-- AutoImmune Disease
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	ancestor_concept_id,
	'autoimmune_disease' [variable_name],
	NULL [variable_desc]
FROM
	VOCAB.CONCEPT_ANCESTOR CA
   INNER JOIN VOCAB.CONCEPT c on CA.descendant_concept_id = C.concept_id
WHERE
   ca.ancestor_concept_id IN (434621) -- SNOMED Autoimmune disease

GO

-- Leukemia
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	ancestor_concept_id,
	'leukemia' [variable_name],
	NULL [variable_desc]
FROM
	VOCAB.CONCEPT_ANCESTOR CA
   INNER JOIN VOCAB.CONCEPT c on CA.descendant_concept_id = C.concept_id
WHERE
   ca.ancestor_concept_id IN (317510) -- SNOMED Leukemia

GO



-- Lymphoma
with partitioned as (
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	ancestor_concept_id,
	'lymphoma' [variable_name],
	NULL [variable_desc],
	ROW_NUMBER() OVER(PARTITION BY concept_id ORDER BY  domain_id ASC) AS seq
FROM
	VOCAB.CONCEPT_ANCESTOR CA
   INNER JOIN VOCAB.CONCEPT c on CA.descendant_concept_id = C.concept_id
WHERE
   ca.ancestor_concept_id IN (4161665, -- (snomed lymphoma finding)
							  432571)-- Malignant lymphoma
)
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	variable_name,
	[variable_desc]
FROM
	partitioned 
WHERE
	seq = 1 -- remove duplicate key value pairs
GO

-- Solid Tumor In Situ
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	NULL [ancestor_concept_id],
	'solid_tumor_in_situ' [variable_name],
	NULL [variable_desc]
FROM
	VOCAB.CONCEPT C
	INNER JOIN VOCAB.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = C.concept_id AND CR.relationship_id IN ('Has asso morph',
																										   'Asso morph of',
																										   'Dir morph of',
																										   'Specimen morph of',
																										   'Has dir morph',
																										   'Has specimen morph')
												 AND Cr.concept_id_2 IN (SELECT descendant_concept_id FROM VOCAB.CONCEPT_ANCESTOR WHERE ancestor_concept_id = 4133294)) -- In situ neoplasm (morphology) There was no good ancestor in concept ancestor table
WHERE
	C.concept_id NOT IN (SELECT descendant_concept_id FROM VOCAB.CONCEPT_ANCESTOR WHERE ancestor_concept_id = 443392)
GO

-- Malignant tumor without metastisis
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	ancestor_concept_id,
	'malignant_solid_tumor_without_metastisis' [variable_name],
	NULL [variable_desc]
FROM
	VOCAB.CONCEPT_ANCESTOR CA
   INNER JOIN VOCAB.CONCEPT c on CA.descendant_concept_id = C.concept_id
WHERE
   ca.ancestor_concept_id IN (443392) -- SNOMED Malignant Neoplastic disease
   AND
   C.concept_id NOT IN (SELECT DISTINCT concept_id_1 FROM VOCAB.CONCEPT_RELATIONSHIP WHERE relationship_id IN ('Has asso morph',
																										   'Asso morph of',
																										   'Dir morph of',
																										   'Specimen morph of',
																										   'Has dir morph',
																										   'Has specimen morph') AND concept_id_2 IN (SELECT descendant_concept_id FROM VOCAB.CONCEPT_ANCESTOR WHERE ancestor_concept_id = 4032806)) -- Matastic Neoplasm SNOMED

GO


-- Electrolyte/fluid disturbance
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	ancestor_concept_id,
	'lytes' [variable_name],
	NULL [variable_desc]
FROM
	VOCAB.CONCEPT_ANCESTOR CA
   INNER JOIN VOCAB.CONCEPT c on CA.descendant_concept_id = C.concept_id
WHERE
   ca.ancestor_concept_id IN (441830) -- SNOMED Disorder of fluid AND/OR electrolyte
GO


-- Coagulopathy
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	ancestor_concept_id,
	'coagulopathy' [variable_name],
	NULL [variable_desc]
FROM
	VOCAB.CONCEPT_ANCESTOR CA
   INNER JOIN VOCAB.CONCEPT c on CA.descendant_concept_id = C.concept_id
WHERE
   ca.ancestor_concept_id IN (432585) -- SNOMED Blood coagulation disorder
GO


-- Depression
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	ancestor_concept_id,
	'depression' [variable_name],
	NULL [variable_desc]
FROM
	VOCAB.CONCEPT_ANCESTOR CA
   INNER JOIN VOCAB.CONCEPT c on CA.descendant_concept_id = C.concept_id
WHERE
   ca.ancestor_concept_id IN (440383) -- SNOMED Depressive disorder
GO


-- DRUG abuse
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	ancestor_concept_id,
	'drug_abuse' [variable_name],
	NULL [variable_desc]
FROM
	VOCAB.CONCEPT_ANCESTOR CA
   INNER JOIN VOCAB.CONCEPT c on CA.descendant_concept_id = C.concept_id
WHERE
   ca.ancestor_concept_id IN (436954) -- SNOMED Drug Abuse
GO



-- complicated hypertension
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	ancestor_concept_id,
	'complicated_hypertension' [variable_name],
	NULL [variable_desc]
FROM
	VOCAB.CONCEPT_ANCESTOR CA
   INNER JOIN VOCAB.CONCEPT c on CA.descendant_concept_id = C.concept_id
WHERE
   ca.ancestor_concept_id IN (42709887) -- SNOMED Hypertensive complication
GO

-- Essential Hypertension
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	ancestor_concept_id,
	'essential_hypertension' [variable_name],
	NULL [variable_desc]
FROM
	VOCAB.CONCEPT_ANCESTOR CA
   INNER JOIN VOCAB.CONCEPT c on CA.descendant_concept_id = C.concept_id
WHERE
   ca.ancestor_concept_id IN (320128) -- SNOMED Essential Hypertension
GO

-- Chronic lung disease
with partitioned as (
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	ancestor_concept_id,
	'chronic_lung_disease' [variable_name],
	NULL [variable_desc],
	ROW_NUMBER() OVER(PARTITION BY concept_id ORDER BY  domain_id ASC) AS seq
FROM
	VOCAB.CONCEPT_ANCESTOR CA
   INNER JOIN VOCAB.CONCEPT c on CA.descendant_concept_id = C.concept_id
WHERE
   ca.ancestor_concept_id IN (4186898, 4063381) -- SNOMED Chronic lung disease, Chronic disease of respiratory system
   AND
   ca.descendant_concept_id NOT IN (SELECT descendant_concept_id FROM VOCAB.CONCEPT_ANCESTOR WHERE ancestor_concept_id = 254068) -- Disorder of upper respiratory system
)
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	variable_name,
	NULL [variable_desc]
FROM
	partitioned 
WHERE
	seq = 1 -- remove duplicate key value pairs
GO

-- Movement disorder
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	ancestor_concept_id,
	'neuro_movement_disorder' [variable_name],
	NULL [variable_desc]
FROM
	VOCAB.CONCEPT_ANCESTOR CA
   INNER JOIN VOCAB.CONCEPT c on CA.descendant_concept_id = C.concept_id
WHERE
   ca.ancestor_concept_id IN (372604) -- SNOMED Movement disorder
   AND
   ca.descendant_concept_id NOT IN (SELECT descendant_concept_id FROM VOCAB.CONCEPT_ANCESTOR WHERE ancestor_concept_id = 440377) -- SNOMED Paralysis
GO

-- CMR_NEURO_OTH	Other neurological disorders
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	ancestor_concept_id,
	'other_neurological_disorder' [variable_name],
	NULL [variable_desc]
FROM
	VOCAB.CONCEPT_ANCESTOR CA
   INNER JOIN VOCAB.CONCEPT c on CA.descendant_concept_id = C.concept_id
WHERE
   ca.ancestor_concept_id IN (376337) -- SNOMED Disorder of nervous system
   AND
   ca.descendant_concept_id NOT IN (SELECT descendant_concept_id FROM VOCAB.CONCEPT_ANCESTOR WHERE ancestor_concept_id = 372604) --SNOMED Movement disorder
   AND
   ca.descendant_concept_id NOT IN (SELECT descendant_concept_id FROM VOCAB.CONCEPT_ANCESTOR WHERE ancestor_concept_id = 377091) -- SNOMED seizure
   AND
   ca.descendant_concept_id NOT IN (SELECT descendant_concept_id FROM VOCAB.CONCEPT_ANCESTOR WHERE ancestor_concept_id = 440377) -- SNOMED Paralysis
GO


--CMR_NEURO_SEIZ	Seizures and epilepsy 
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	ancestor_concept_id,
	'neuro_seizure' [variable_name],
	NULL [variable_desc]
FROM
	VOCAB.CONCEPT_ANCESTOR CA
   INNER JOIN VOCAB.CONCEPT c on CA.descendant_concept_id = C.concept_id
WHERE
   ca.ancestor_concept_id IN (377091) -- SNOMED seizure
GO


--CMR_OBESE	Obesity		^E66
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	ancestor_concept_id,
	'obesity' [variable_name],
	NULL [variable_desc]
FROM
	VOCAB.CONCEPT_ANCESTOR CA
   INNER JOIN VOCAB.CONCEPT c on CA.descendant_concept_id = C.concept_id
WHERE
   ca.ancestor_concept_id IN (433736) -- SNOMED Obesity
GO

--CMR_PARALYSIS	Paralysis ^G041|^G114|^G801|^G802|^G81|^G82|^G830|^G831|^G832|^G833|^G834|^G839
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	ancestor_concept_id,
	'paralysis' [variable_name],
	NULL [variable_desc]
FROM
	VOCAB.CONCEPT_ANCESTOR CA
   INNER JOIN VOCAB.CONCEPT c on CA.descendant_concept_id = C.concept_id
WHERE
   ca.ancestor_concept_id IN (440377) -- SNOMED Paralysis
GO

--CMR_PSYCHOSES	Psychoses	^F20|^F22|^F23|^F24|^F25|^F28|^F29|^F302|^F312|^F315
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	ancestor_concept_id,
	'psychotic_disorder' [variable_name],
	NULL [variable_desc]
FROM
	VOCAB.CONCEPT_ANCESTOR CA
   INNER JOIN VOCAB.CONCEPT c on CA.descendant_concept_id = C.concept_id
WHERE
   ca.ancestor_concept_id IN (436073) -- SNOMED Psychotic disorder
GO

--CMR_PULMCIRC	Pulmonary circulation disease ^I26|^I27|^I280|^I288|^I289
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	ancestor_concept_id,
	'pulmonary_circulation_disease' [variable_name],
	NULL [variable_desc]
FROM
	VOCAB.CONCEPT_ANCESTOR CA
   INNER JOIN VOCAB.CONCEPT c on CA.descendant_concept_id = C.concept_id
WHERE
   ca.ancestor_concept_id IN (433208) -- SNOMED Disorder of pulmonary circulation
GO

--CMR_RENLFL_MOD	Renal (kidney) failure and disease, moderate 
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	ancestor_concept_id,
	'moderate_kidney_failure' [variable_name],
	NULL [variable_desc]
FROM
	VOCAB.CONCEPT_ANCESTOR CA
   INNER JOIN VOCAB.CONCEPT c on CA.descendant_concept_id = C.concept_id
WHERE
   ca.ancestor_concept_id IN (443597) -- SNOMED Chronic kidney disease stage 3
UNION
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	NULL ancestor_concept_id,
	'moderate_kidney_failure' [variable_name],
	NULL [variable_desc]
FROM
	VOCAB.CONCEPT c
WHERE
   c.concept_id IN (46271022, -- Chronic kidney disease
					192359) -- Renal Failure Syndrome
GO


--CMR_RENLFL_SEV	Renal (kidney) failure and disease, severe 
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	ancestor_concept_id,
	'severe_kidney_failure' [variable_name],
	NULL [variable_desc]
FROM
	VOCAB.CONCEPT_ANCESTOR CA
   INNER JOIN VOCAB.CONCEPT c on CA.descendant_concept_id = C.concept_id
WHERE
   ca.ancestor_concept_id IN (443612, --SNOMED	431857002	Chronic kidney disease stage 4
							  443611, -- SNOMED	433146000	Chronic kidney disease stage 5
							  193782, -- SNOMED End-stage renal disease
							  42539502) -- SNOMED Kidney Transplant presente
GO

--CMR_THYROID_HYPO	Hypothyroidism	^E00|^E01|^E02|^E03|^E890
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	ancestor_concept_id,
	'hypothyroidism' [variable_name],
	NULL [variable_desc]
FROM
	VOCAB.CONCEPT_ANCESTOR CA
   INNER JOIN VOCAB.CONCEPT c on CA.descendant_concept_id = C.concept_id
WHERE
   ca.ancestor_concept_id IN (140673) -- SNOMED	40930008	Hypothyroidism
GO

--CMR_THYROID_OTH	Other thyroid disorders
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	ancestor_concept_id,
	'other_thyroid_disorders' [variable_name],
	NULL [variable_desc]
FROM
	VOCAB.CONCEPT_ANCESTOR CA
   INNER JOIN VOCAB.CONCEPT c on CA.descendant_concept_id = C.concept_id
WHERE
   ca.ancestor_concept_id IN (141253) -- SNOMED Disorder of thyroid gland
   AND
   ca.descendant_concept_id NOT IN (SELECT descendant_concept_id FROM VOCAB.CONCEPT_ANCESTOR WHERE ancestor_concept_id = 140673) -- SNOMED	40930008	Hypothyroidism
GO

--CMR_ULCER_PEPTIC	Peptic ulcer with bleeding --SNOMED 4271696 Peptic Ulcer with hemmhorage
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	ancestor_concept_id,
	'peptic_ulcer_with_bleeding' [variable_name],
	NULL [variable_desc]
FROM
	VOCAB.CONCEPT_ANCESTOR CA
   INNER JOIN VOCAB.CONCEPT c on CA.descendant_concept_id = C.concept_id
WHERE
   ca.ancestor_concept_id IN (4271696) -- SNOMED Peptic Ulcer with hemmhorage
GO

--CMR_VALVE	Valvular disease ^A520|^I05|^I06|^I07|^I08|^I091|^I098|^I34|^I35|^I36|^I37|^I38|^I39|^Q230|^Q231|^Q232|^Q233|^Z952|^Z953|^Z954
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	ancestor_concept_id,
	'heart_valve_disease' [variable_name],
	NULL [variable_desc]
FROM
	VOCAB.CONCEPT_ANCESTOR CA
   INNER JOIN VOCAB.CONCEPT c on CA.descendant_concept_id = C.concept_id
WHERE
   ca.ancestor_concept_id IN (4281749) -- SNOMED Heart valve disorder
GO

--CMR_WGHTLOSS	Weight loss		^E40|^E41|^E42|^E43|^E44|^E45|^E46|^R634|^R64
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT DISTINCT
    C.[concept_id],
    C.[concept_code],
    C.[concept_name],
    C.[vocabulary_id],
	C.concept_class_id,
    C.[domain_id],
	ancestor_concept_id,
	'malnutrition_macronutrients_weightloss' [variable_name],
	NULL [variable_desc]
FROM
	VOCAB.CONCEPT_ANCESTOR CA
   INNER JOIN VOCAB.CONCEPT c on CA.descendant_concept_id = C.concept_id
WHERE
   ca.ancestor_concept_id IN (433163) -- SNOMED Deficiency of macronutrients
GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	[ancestor_concept_id],
	'english_language' as variable_name,
	NULL [variable_desc]
FROM 
	dbo.IC3_Variable_Lookup_Table_v3_beta
WHERE
	variable_name = 'language'
	AND
	concept_name LIKE '%english%'

GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	NULL [ancestor_concept_id],
	'surgical_admission' as variable_name,
	NULL [variable_desc]
FROM
	VOCAB.CONCEPT
WHERE
	concept_id IN (38003911, --	Adult Reconstructive Orthopaedic Surgery	NUCC
					761960, --	Bariatric surgery service	SNOMED
					44777667, --	Breast surgery (includes suspected neoplasms, cysts etc, does not include cosmetic surgery)	HES Specialty
					4150867, --	Breast surgery service	SNOMED
					37207445, --	Breast surgical oncology service	SNOMED
					38004497, --	Cardiac Surgery	Medicare Specialty
					4148673, --	Cardiac surgery service	SNOMED
					4147265, --	Cardiothoracic surgery service	SNOMED
					38004471, --	Colorectal Surgery	Medicare Specialty
					4149157, --	Colorectal surgery service	SNOMED
					4148666, --	Community surgical fitting service	SNOMED
					45756764, --	Complex General Surgical Oncology	ABMS
					45756765, --	Congenital Cardiac Surgery	ABMS
					44814158, --	Dental surgery assistance service	SNOMED
					4148674, --	Dental surgery service	SNOMED
					4147266, --	Endocrine surgery service	SNOMED
					45756773, --	Female Pelvic Medicine and Reconstructive Surgery	ABMS
					43125859, --	Female Pelvic Medicine and Reconstructive Surgery Obstetrician / Gynecologist	NUCC
					43125861, --	Female Pelvic Medicine and Reconstructive Surgery Urologist	NUCC
					38003913, --	Foot and Ankle Surgery	NUCC
					4150870, --	Gastrointestinal surgery service	SNOMED
					4149154, --	General dental surgery service	SNOMED
					4149156, --	General gastrointestinal surgery service	SNOMED
					38004447, --	General Surgery	Medicare Specialty
					4150871, --	General surgical service	SNOMED
					38004480, --	Hand Surgery	Medicare Specialty
					4149158, --	Hand surgery service	SNOMED
					44777668, --	Hepatobiliary & pancreatic surgery (includes liver surgery but excludes liver transplantation see transplantation surgery)	HES Specialty
					44811296, --	Hepatobiliary and pancreatic surgery service	SNOMED
					4147268, --	Hepatobiliary surgical service	SNOMED
					38004011, --	Hospice and Palliative Surgery	NUCC
					4149149, --	Hospital surgical fitting service	SNOMED
					38004504, --	Maxillofacial Surgery	Medicare Specialty
					46271779, --	Maxillofacial surgery service	SNOMED
					38003839, --	MOHS-Micrographic Surgery	NUCC
					38004459, --	Neurosurgery	Medicare Specialty
					4149159, --	Neurosurgical service	SNOMED
					44777813, --	Non-UK Provider - specialty function not known, treatment mainly surgical	HES Specialty
					903244, --	Ophthalmic Plastic and Reconstructive Surgery	NUCC
					38003680, --	Oral and Maxillofacial Dental Surgery	NUCC
					38003826, --	Oral and Maxillofacial Surgery	NUCC
					37312677, --	Oral and maxillofacial surgery service	SNOMED
					38004464, --	Oral Surgery	Medicare Specialty
					4150868, --	Oral surgery service	SNOMED
					38003910, --	Orthopaedic Hand Surgery	NUCC
					38003912, --	Orthopaedic Surgery of the Spine	NUCC
					38003915, --	Orthopaedic Trauma Surgery	NUCC
					38003914, --	Orthopedic Sports Medicine Surgery	NUCC
					38004465, --	Orthopedic Surgery	Medicare Specialty
					38003919, --	Otolaryngological Facial Plastic Surgery	NUCC
					38003923, --	Otolaryngological Surgery	NUCC
					761983, --	Outpatient surgery service	SNOMED
					44777760, --	Paediatric Cardiac Surgery	HES Specialty
					44809704, --	Paediatric cardiac surgery service	SNOMED
					44777752, --	Paediatric Gastrointestinal Surgery	HES Specialty
					44809699, --	Paediatric gastrointestinal surgery service	SNOMED
					44777756, --	Paediatric Maxillo-Facial Surgery	HES Specialty
					44809696, --	Paediatric maxillofacial surgery service	SNOMED
					44777757, --	Paediatric Neurosurgery	HES Specialty
					44809693, --	Paediatric neurosurgery service	SNOMED
					3657589, --	Paediatric oral and maxillofacial surgery service	SNOMED
					44777758, --	Paediatric Plastic Surgery	HES Specialty
					44777761, --	Paediatric Thoracic Surgery	HES Specialty
					44809682, --	Paediatric thoracic surgery service	SNOMED
					44777751, --	Paediatric Transplantation Surgery	HES Specialty
					44809681, --	Paediatric transplantation surgery service	SNOMED
					4149160, --	Pancreatic surgery service	SNOMED
					38003909, --	Pediatric Orthopaedic Surgery	NUCC
					37207442, --	Pediatric plastic surgery service	SNOMED
					45756819, --	Pediatric Surgery	ABMS
					4150873, --	Pediatric surgical service	SNOMED
					38004467, --	Plastic And Reconstructive Surgery	Medicare Specialty
					38003971, --	Plastic Surgery of the Hand	NUCC
					4147269, --	Plastic surgery service	SNOMED
					45756822, --	Plastic Surgery Within the Head and Neck	ABMS
					38003920, --	Plastic Surgery within the Head and Neck Otolaryngology	NUCC
					44811423, --	Podiatric surgery service	SNOMED
					765146, --	Reconstructive surgery service	SNOMED
					44811332, --	Spinal surgery service	SNOMED
					37207453, --	Spine orthopedic surgery service	SNOMED
					45756829, --	Surgical Critical Care	ABMS
					37116895, --	Transplant medicine service	SNOMED
					903261, --	Surgical dentistry	HES Specialty
					4147257, --	Surgical fitting service	SNOMED
					44811309, --	Blood and marrow transplantation service	SNOMED
					45756831, --	Transplant Hepatology	ABMS
					38004508, --	Surgical Oncology	Medicare Specialty
					44782506, --	Surgical oncology service	SNOMED
					45769515, --	Surgical pathology service	SNOMED
					4149152, --	Surgical service	SNOMED
					45756830, --	Thoracic and Cardiac Surgery	ABMS
					38004473, --	Thoracic Surgery	Medicare Specialty
					44811307, --	Cardiothoracic transplantation service	SNOMED
					38004023, --	Thoracic Surgery (Cardiothoracic Vascular Surgery)	NUCC
					45756820, --	Pediatric Transplant Hepatology	ABMS
					4149153, --	Thoracic surgery service	SNOMED
					38003827, --	Transplant Surgery	NUCC
					4149161, --	Transplant surgery service	SNOMED
					903279, --	Advanced Heart Failure and Transplant Cardiology	Medicare Specialty
					44777666, --	Transplantation surgery (includes renal and liver transplants, excludes cardiothoracic transplantation)	HES Specialty
					38004016, --	Trauma Surgery	NUCC
					4149162, --	Trauma surgery service	SNOMED
					903281, --	Hematopoietic Cell Transplantation and Cellular Therapy	Medicare Specialty
					44777676, --	Cardiothoracic transplantation (recognised specialist services only - includes 'outreach' facilities)	HES Specialty
					44777669, --	Upper gastrointestinal surgery	HES Specialty
					4147267, --	Upper gastrointestinal surgery service	SNOMED
					38004496, --	Vascular Surgery	Medicare Specialty
					44777800, --	Bone and marrow transplantation (previously part of clinical haematology)	HES Specialty
					4150874 --	Vascular surgery service	SNOMED
)
GO
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	NULL [ancestor_concept_id],
	'medical_admission' as variable_name,
	NULL [variable_desc]
FROM
	VOCAB.CONCEPT
WHERE
	concept_id IN (
				32417, -- Nuclear Medical Physics     ABMS
				762453, -- Otolaryngology service     SNOMED
				38004451, -- Cardiology     Medicare Specialty
				4148519, -- Adult diagnostic audiology service     SNOMED
				4149134, -- Open access service     SNOMED
				38003978, -- Occupational Preventive Medicine     NUCC
				38003983, -- Hospice and Palliative Psychiatry or Neurology     NUCC
				38004469, -- Psychiatry     Medicare Specialty
				38004501, -- Hematology     Medicare Specialty
				38004505, -- Neuropsychiatry     Medicare Specialty
				44811294, -- Intermediate care service     SNOMED
				44814050, -- Paediatric epilepsy service     SNOMED
				45756768, -- Developmental-Behavioral Pediatrics     ABMS
				45756791, -- Neuroradiology     ABMS
				45756800, -- Microbiology     ABMS
				45756817, -- Pediatric Rehabilitation Medicine     ABMS
				45771342, -- Oral pathology service     SNOMED
				4149001, -- Adult cochlear implant service     SNOMED
				4149140, -- Pharmacy service     SNOMED
				4150088, -- Assessment service     SNOMED
				37207437, -- Opioid dependence service     SNOMED
				38003843, -- Procedural Dermatology     NUCC
				38003980, -- Obesity (Bariatric) Psychiatry     NUCC
				3657579, -- Cardiac physiology service     SNOMED
				38004472, -- Pulmonary Disease     Medicare Specialty
				44777713, -- Occupatioal Medicine     HES Specialty
				44783550, -- Community based physiotherapy service     SNOMED
				37312119, -- Cardiology service     SNOMED
				45756750, -- Aerospace Medicine     ABMS
				45756771, -- Endocrinology, Diabetes and Metabolism     ABMS
				38003895, -- Nuclear Cardiology     NUCC
				45756826, -- Reproductive Endocrinology / Infertility     ABMS
				45763742, -- Dermatology service     SNOMED
				45763902, -- Endocrinology service     SNOMED
				45763903, -- Clinical physiology service     SNOMED
				45768466, -- Mycobacteriology service     SNOMED
				45773016, -- Diagnostic imaging service     SNOMED
				46273651, -- Geriatric service     SNOMED
				32412, -- Clinical Genetics and Genomics (MD)     ABMS
				903280, -- Medical Toxicology     Medicare Specialty
				4149151, -- Mental handicap psychiatry service     SNOMED
				4252366, -- Specialist multidisciplinary team     SNOMED
				37019099, -- Community midwifery service     SNOMED
				37312120, -- Pediatric cardiology service     SNOMED
				37312526, -- Internal medicine service     SNOMED
				44811421, -- Spinal injuries service     SNOMED
				45756760, -- Clinical Genetics (MD)     ABMS
				45756763, -- Clinical Neurophysiology     ABMS
				45756804, -- Pediatric Anesthesiology     ABMS
				32416, -- Laboratory Genetics and Genomics     ABMS
				3655000, -- Aerospace medical service     SNOMED
				3657569, -- Urological physiology service     SNOMED
				45756754, -- Cardiovascular Disease     ABMS
				3657576, -- Medical psychotherapy service     SNOMED
				4134829, -- Community health services     SNOMED
				4148998, -- Audiological service     SNOMED
				4150090, -- Adult hearing aid service     SNOMED
				4150235, -- Social services occupational therapy service     SNOMED
				35609176, -- Paediatric diabetes service     SNOMED
				37312681, -- Clinical genetics service     SNOMED
				38003921, -- Otolaryngic Allergy     NUCC
				38003964, -- Neuromuscular Rehabilitation Medicine     NUCC
				38004006, -- Therapeutic Radiology     NUCC
				40481526, -- Diabetes mellitus service     SNOMED
				44777674, -- Burn care     HES Specialty
				45757610, -- Pulmonary rehabilitation service     SNOMED
				45769502, -- Vascular ultrasound service     SNOMED
				45769505, -- Interventional radiology service     SNOMED
				45756805, -- Pediatric Cardiology     ABMS
				761963, -- Craniofacial service     SNOMED
				3657571, -- Inherited metabolic medicine service     SNOMED
				4147237, -- Diagnostic audiology service     SNOMED
				4148659, -- Community-based dietetics service     SNOMED
				4148660, -- Hospital-based dietetics service     SNOMED
				4148676, -- Pediatric dentistry service     SNOMED
				4150089, -- Distraction test audiological screening service     SNOMED
				4150091, -- Pediatric hearing aid service     SNOMED
				37312471, -- Critical care medicine service     SNOMED
				38003825, -- Neuromusculoskeletal Medicine and Osteopathic Manipulative Medicine (OMM)     NUCC
				38003875, -- Clinical and Laboratory Immunology     NUCC
				38003879, -- Magnetic Resonance Imaging (MRI)     NUCC
				38003937, -- Clinical Pathology / Laboratory Medicine     NUCC
				38004450, -- Anesthesiology     Medicare Specialty
				38004455, -- Gastroenterology     Medicare Specialty
				38004452, -- Dermatology     Medicare Specialty
				38004470, -- Geriatric Psychiatry     Medicare Specialty
				38004500, -- Critical care (intensivist)     Medicare Specialty
				37207432, -- Hepatology service     SNOMED
				44811856, -- NHS 111 service     SNOMED
				44811958, -- Remote triage and advice service     SNOMED
				45756767, -- Dermatopathology     ABMS
				45763734, -- Critical care physician service     SNOMED
				45769500, -- Mental health service     SNOMED
				45770602, -- School nursing service     SNOMED
				46270515, -- Rheumatology service     SNOMED
				46286288, -- Fracture liaison service     SNOMED
				32414, -- Consultation-Liaison Psychiatry     ABMS
				903278, -- Hospital Medicine     Medicare Specialty
				3657572, -- Gastrointestinal physiology service     SNOMED
				4149133, -- Pregnancy termination service     SNOMED
				4150233, -- Medical microbiology service     SNOMED
				4150237, -- Speech and language therapy service     SNOMED
				36716235, -- Pulmonary medicine service     SNOMED
				46270520, -- Pediatric gastroenterology service     SNOMED
				37310986, -- Pediatric chronic pain management service     SNOMED
				37312121, -- Pediatric otolaryngology service     SNOMED
				37312684, -- Infectious disease service     SNOMED
				38004449, -- Otolaryngology     Medicare Specialty
				38004489, -- Audiology     Medicare Specialty
				44777801, -- Haemophilia (previously part of clinical haematology)     HES Specialty
				44809683, -- Neonatal critical care service     SNOMED
				3657593, -- Paediatric hepatology service     SNOMED
				44811303, -- Dental medicine service     SNOMED
				45763901, -- Gastroenterology service     SNOMED
				44811322, -- Community mental health team     SNOMED
				44812167, -- Telehealthcare service     SNOMED
				44814048, -- Respiratory physiology service     SNOMED
				45756748, -- Adult Congenital Heart Disease     ABMS
				45773018, -- Toxicology service     SNOMED
				32419, -- Therapeutic Medical Physics     ABMS
				903232, -- Oral Medicinist     NUCC
				903274, -- Clinical Cardiac Electrophysiology     Medicare Specialty
				1314351, -- Oncology care model (ocm) monthly enhanced oncology services (meos) payment for ocm enhanced services. g9678 payments may only be made to ocm practitioners for ocm beneficiaries for the furnishment of enhanced services as defined in the ocm participa...     HCPCS
				3657565, -- Fetal medicine service     SNOMED
				4149127, -- Intensive care service     SNOMED
				4149141, -- Professional allied to medicine service     SNOMED
				4247121, -- Community learning disabilities team     SNOMED
				38003876, -- Hepatology     NUCC
				37312122, -- Pediatric respiratory therapy service     SNOMED
				38003823, -- Phlebology     NUCC
				38003854, -- Adult Family Medicine     NUCC
				38003967, -- Pain Rehabilitation Medicine     NUCC
				38003999, -- Diagnostic Neuroimaging     NUCC
				38004454, -- Interventional Pain Management (IPM)     Medicare Specialty
				42536183, -- Prosthodontic service     SNOMED
				44787794, -- Substance misuse team     SNOMED
				44808069, -- Well man service     SNOMED
				44813777, -- Programmed pulmonary rehabilitation service     SNOMED
				45756775, -- Forensic Psychiatry     ABMS
				45756810, -- Pediatric Gastroenterology     ABMS
				45756834, -- Vascular Neurology     ABMS
				45769504, -- Obstetric ultrasound service     SNOMED
				45769523, -- Hematology service     SNOMED
				46270519, -- Pediatric endocrinology service     SNOMED
				46270523, -- Pediatric pulmonology service     SNOMED
				762434, -- Sleep service     SNOMED
				762435, -- Wound care service     SNOMED
				762454, -- Adolescent medicine service     SNOMED
				903276, -- Interventional Cardiology     Medicare Specialty
				903256, -- Sports Medicine     Medicare Specialty
				4148641, -- Speech-reading training service     SNOMED
				4150106, -- Obstetrics service     SNOMED
				4149000, -- Aural rehabilitation service     SNOMED
				38003905, -- Obstetrics     NUCC
				36716236, -- Psychosomatic medicine service     SNOMED
				37207443, -- Vascular imaging service     SNOMED
				38003897, -- In Vivo and In Vitro Nuclear Medicine     NUCC
				38003899, -- Obesity (Bariatric) Obstetrics / Gynecology     NUCC
				38004510, -- Emergency Medicine     Medicare Specialty
				44777724, -- Non-UK Provider - specialty function not known, treatment mainly medical     HES Specialty
				44777796, -- Eating Disorders     HES Specialty
				38003903, -- Hospice and Palliative Obstetrics / Gynecology     NUCC
				44811447, -- Well baby service     SNOMED
				44812159, -- Telehealth monitoring service     SNOMED
				44813909, -- Paediatric audiological medicine service     SNOMED
				44814161, -- Dispensing optometry service     SNOMED
				45756802, -- Pathology - Pediatric     ABMS
				45756812, -- Pediatric Infectious Diseases     ABMS
				45756816, -- Pediatric Radiology     ABMS
				45770598, -- Acute medicine service     SNOMED
				762428, -- Reproductive service     SNOMED
				765504, -- Weight loss service     SNOMED
				765548, -- Newborn service     SNOMED
				38003900, -- Critical Care Obstetrics / Gynecology     NUCC
				3657570, -- Sleep medicine service     SNOMED
				3657577, -- Aviation and space medicine service     SNOMED
				4149132, -- Obstetrics and gynecology service     SNOMED
				4147259, -- Forensic psychiatry service     SNOMED
				4147261, -- Breast screening service     SNOMED
				4147549, -- Cytology service     SNOMED
				4148669, -- Community rehabilitation service     SNOMED
				4150094, -- Counseling service     SNOMED
				35609080, -- Health visiting service     SNOMED
				38003968, -- Sports Rehabilitation Medicine     NUCC
				38004458, -- Neurology     Medicare Specialty
				38004466, -- Pathology     Medicare Specialty
				38004479, -- Nephrology     Medicare Specialty
				42536182, -- Endodontic service     SNOMED
				38004461, -- Obstetrics / Gynecology     Medicare Specialty
				903242, -- Pediatric Ophthalmology and Strabismus     NUCC
				44808155, -- NHS 24     SNOMED
				903238, -- Glaucoma Ophthalmology     NUCC
				32418, -- Pediatric Hospital Medicine     ABMS
				764911, -- Pediatric medical toxicology service     SNOMED
				2721444, -- Respite care, in the home, per diem     HCPCS
				903239, -- Retina Ophthalmology     NUCC
				4149137, -- Acute pain service     SNOMED
				36716425, -- Physical medicine and rehabilitation service     SNOMED
				37310992, -- Adult chronic pain management service     SNOMED
				44777755, -- Paediatric Ophthalmology     HES Specialty
				38003942, -- Hospice and Palliative Pediatric Medicine     NUCC
				38004008, -- Radiological Physics     NUCC
				44808040, -- Paediatric cystic fibrosis service     SNOMED
				44808062, -- Eating disorders service     SNOMED
				44809705, -- Paediatric burns care service     SNOMED
				44811422, -- Specialist rehabilitation service     SNOMED
				45756753, -- Brain Injury Medicine     ABMS
				45756783, -- Medical Physics     ABMS
				45756808, -- Pediatric Emergency Medicine     ABMS
				45768463, -- Parasitology service     SNOMED
				45773015, -- Nursing service     SNOMED
				46270522, -- Pediatric nephrology service     SNOMED
				46286268, -- Pharmacy First service     SNOMED
				762423, -- Genetics service     SNOMED
				765418, -- Anticoagulation service     SNOMED
				3657574, -- Ophthalmic and vision science service     SNOMED
				903269, -- Medical virology     HES Specialty
				4147252, -- Occupational therapy service     SNOMED
				4147262, -- Magnetic resonance imaging service     SNOMED
				4148658, -- Hospital-based podiatry service     SNOMED
				4150095, -- Diagnostic investigation service     SNOMED
				37017759, -- Midwifery service     SNOMED
				37207450, -- HIV (human immunodeficiency virus) social work service     SNOMED
				37312475, -- Histopathology service     SNOMED
				38003830, -- Allergy     NUCC
				38003852, -- Adolescent Family Medicine     NUCC
				38003858, -- Sports Family Medicine     NUCC
				38004692, -- Clinical Laboratory     Medicare Specialty
				44777763, -- Paediatric Intensive Care     HES Specialty
				44807945, -- Anticoagulant service     SNOMED
				45756821, -- Pediatric Urology     ABMS
				45769510, -- Dermatopathology service     SNOMED
				45769512, -- Clinical pathology service     SNOMED
				45770650, -- Public health dentistry service     SNOMED
				46273526, -- Pediatric emergency medical service     SNOMED
				32411, -- Clinical Cytogenetics and Genomics     ABMS
				764910, -- Pediatric radiology service     SNOMED
				903243, -- Cornea and External Ophthalmology     NUCC
				903240, -- Uveitis and Ocular Inflammatory Disease Ophthalmology     NUCC
				3657597, -- Rare disease service     SNOMED
				4148668, -- Computerized tomography service     SNOMED
				4150092, -- Tinnitus management service     SNOMED
				4148801, -- Ophthalmology service     SNOMED
				44809690, -- Paediatric ophthalmology service     SNOMED
				4150863, -- Radiology service     SNOMED
				36716237, -- Dentistry service     SNOMED
				36716399, -- Legal medicine service     SNOMED
				37312527, -- Emergency ambulance service     SNOMED
				38003841, -- Clinical and Laboratory Dermatological Immunology     NUCC
				903241, -- Neuro-Ophthalmology     NUCC
				38003959, -- Sleep Pediatrics     NUCC
				38004675, -- Physician / Diagnostic Radiology     Medicare Specialty
				44777695, -- Perinatal Psychiatry     HES Specialty
				44777768, -- Paediatric Clinical Immunology and Allergy     HES Specialty
				44808057, -- Dental hygiene service     SNOMED
				44811432, -- Dementia assessment service     SNOMED
				45756772, -- Epilepsy     ABMS
				45757601, -- Acute care inpatient service     SNOMED
				45763904, -- Clinical pharmacology service     SNOMED
				761966, -- Life management service     SNOMED
				761985, -- Women's health service     SNOMED
				903277, -- Dentistry     Medicare Specialty
				3657575, -- Paediatric inherited metabolic medicine service     SNOMED
				4148645, -- Endoscopy service     SNOMED
				38004463, -- Ophthalmology     Medicare Specialty
				4148997, -- Child assessment service     SNOMED
				44811205, -- Medical ophthalmology service     SNOMED
				4150234, -- Dietetics service     SNOMED
				37207429, -- Adult dermatology service     SNOMED
				37207431, -- Adult hematology service     SNOMED
				37312473, -- Prosthetic service     SNOMED
				38003681, -- Dental Oral and Maxillofacial Radiology     NUCC
				38003864, -- Obesity (Bariatric) Medicine     NUCC
				44809680, -- Paediatric trauma and orthopaedics service     SNOMED
				38004462, -- Hospice And Palliative Care     Medicare Specialty
				38004511, -- Interventional Radiology     Medicare Specialty
				44777753, -- Paediatric Trauma and Orthopaedics     HES Specialty
				44811325, -- Transient ischaemic attack service     SNOMED
				44811434, -- Complex specialised rehabilitation service     SNOMED
				45756751, -- Anesthesiology Critical Care Medicine     ABMS
				45756796, -- Pathology - Chemical     ABMS
				4147255, -- Hospital orthotics service     SNOMED
				903264, -- Acute internal medicine     HES Specialty
				4147250, -- Arts therapy services     SNOMED
				4147253, -- Community-based occupational therapy service     SNOMED
				4147254, -- Optometry service     SNOMED
				4148999, -- Pediatric diagnostic audiology service     SNOMED
				4149125, -- Colposcopy service     SNOMED
				4149135, -- Pediatric service     SNOMED
				4149143, -- Podiatry service     SNOMED
				4191848, -- Cancer primary healthcare multidisciplinary team     SNOMED
				38003676, -- Dental Oral and Maxillofacial Pathology     NUCC
				38003958, -- Sports Pediatric Medicine     NUCC
				38003995, -- Sports Psychiatry or Neurology     NUCC
				38004474, -- Urology     Medicare Specialty
				40483762, -- Acute care hospice service     SNOMED
				44777784, -- Midwife episode     HES Specialty
				44808430, -- Out of hours service     SNOMED
				44811452, -- Paediatric clinical immunology and allergy service     SNOMED
				44812115, -- Mental health home treatment team     SNOMED
				44814170, -- Perinatal mental health service     SNOMED
				45756786, -- Neonatal-Perinatal Medicine     ABMS
				45756825, -- Radiology     ABMS
				45756833, -- Vascular and Interventional Radiology     ABMS
				45757046, -- Spinal Cord Injury Medicine     ABMS
				45769501, -- Respiratory therapy service     SNOMED
				45769508, -- Cytogenetics service     SNOMED
				762416, -- Long-term care service     SNOMED
				762724, -- Dialysis service     SNOMED
				903246, -- Obesity (Bariatric) Pediatric Medicine     NUCC
				903247, -- Brain Injury Rehabilitation Medicine     NUCC
				3657592, -- Paediatric palliative medicine service     SNOMED
				3657594, -- Rehabilitation medicine service     SNOMED
				4148655, -- Clinical biochemistry service     SNOMED
				4148996, -- Anesthetic service     SNOMED
				4149002, -- Hearing aid service     SNOMED
				36676631, -- Physical medicine service     SNOMED
				37312474, -- Addiction service     SNOMED
				38003678, -- Periodontics     NUCC
				38003835, -- Hospice and Palliative Anesthesiology     NUCC
				38003974, -- Occupational-Environmental Preventive Medicine     NUCC
				38003976, -- Preventive Sports Medicine     NUCC
				38004498, -- Addiction Medicine     Medicare Specialty
				40481548, -- Home hospice service     SNOMED
				43125856, -- Dentist Anesthesiologist     NUCC
				44777778, -- Paediatric Neuro-disability     HES Specialty
				44811435, -- Clinical psychology service     SNOMED
				45756778, -- Internal Medicine - Critical Care Medicine     ABMS
				45756795, -- Pathology - Anatomic     ABMS
				45756801, -- Pathology - Molecular Genetic     ABMS
				45763726, -- Care of elderly service     SNOMED
				45767674, -- Domiciliary physiotherapy service     SNOMED
				45770620, -- Blood banking and transfusion service     SNOMED
				32577, -- Physician     Provider
				37311325, -- Prosthetic and orthotic service     SNOMED
				4148675, -- Orthodontics service     SNOMED
				3662255, -- Community sexual and reproductive health service     SNOMED
				4148640, -- Cochlear implant service     SNOMED
				4148656, -- Art therapy service     SNOMED
				4149147, -- Hospital-based speech and language therapy service     SNOMED
				4149150, -- Liaison psychiatry service     SNOMED
				4150087, -- Accident and Emergency service     SNOMED
				4150236, -- Play therapy service     SNOMED
				4150869, -- Restorative dentistry service     SNOMED
				4252363, -- Behavioral intervention team     SNOMED
				37117769, -- Cardiac rehabilitation service     SNOMED
				38003902, -- Gynecology     NUCC
				38003918, -- Sleep Otolaryngology     NUCC
				38004491, -- Rheumatology     Medicare Specialty
				43125860, -- Behavioral Neurology and Neuropsychiatry     NUCC
				44777680, -- Diabetic medicine     HES Specialty
				44777687, -- Tropical medicine     HES Specialty
				44809679, -- Paediatric urology service     SNOMED
				44809702, -- Paediatric dermatology service     SNOMED
				4150872, -- Orthopedic service     SNOMED
				44777734, -- Orthoptics     HES Specialty
				44812158, -- Telecare monitoring service     SNOMED
				45756781, -- Medical Biochemical Genetics     ABMS
				45757611, -- Employee health service     SNOMED
				45769516, -- Serology service     SNOMED
				45770600, -- Community nursing service     SNOMED
				32415, -- Diagnostic Medical Physics     ABMS
				4150107, -- Community pediatric service     SNOMED
				4150229, -- Special care baby service     SNOMED
				4150230, -- Pain management service     SNOMED
				4150875, -- Ultrasonography service     SNOMED
				36716368, -- Vascular medicine service     SNOMED
				38003836, -- Pain Anesthesiology     NUCC
				38003855, -- Obesity (Bariatric) Family Medicine     NUCC
				38003682, -- Orthodontics and Dentofacial Orthopedics     NUCC
				44777684, -- Clinical Microbiology     HES Specialty
				44777706, -- Blood transfusion     HES Specialty
				44777719, -- Clinical immunology     HES Specialty
				44777774, -- Paediatric Metabolic disease     HES Specialty
				44811429, -- Local specialist rehabilitation service     SNOMED
				44811438, -- Tropical medicine service     SNOMED
				45756752, -- Blood Banking / Transfusion Medicine     ABMS
				45757590, -- Sports medicine service     SNOMED
				45773571, -- Histology service     SNOMED
				32413, -- Clinical Molecular Genetics and Genomics     ABMS
				765173, -- Private nursing service     SNOMED
				3657580, -- Paediatric audiovestibular medicine service     SNOMED
				4160057, -- Medical referral service     SNOMED
				37312477, -- Sexual health service     SNOMED
				37312525, -- Pediatric clinical genetics service     SNOMED
				37312676, -- Nuclear medicine service     SNOMED
				38003675, -- General Dentistry     NUCC
				38003847, -- Hospice and Palliative Emergency Medicine     NUCC
				38003849, -- Emergency Sports Medicine     NUCC
				38003859, -- Sleep Family Medicine     NUCC
				38003947, -- Pediatric Allergy / Immunology     NUCC
				38004478, -- Geriatric Medicine     Medicare Specialty
				38004484, -- Infectious Disease     Medicare Specialty
				44808063, -- Electrocardiography service     SNOMED
				44809678, -- Paediatric clinical haematology service     SNOMED
				4150858, -- Orthoptics service     SNOMED
				45756813, -- Pediatric Nephrology     ABMS
				45763735, -- General medical service     SNOMED
				45769518, -- Immunology service     SNOMED
				761984, -- Skilled nursing service     SNOMED
				4149148, -- Hospital orthoptics service     SNOMED
				3654282, -- Clinical immunology service     SNOMED
				3657595, -- Stroke medicine service     SNOMED
				4147247, -- Occupational health service     SNOMED
				765928, -- Pediatric orthopedic service     SNOMED
				4148670, -- Young disabled service     SNOMED
				37312683, -- Adult mental health service     SNOMED
				38004000, -- Hospice and Palliative Radiology     NUCC
				38004448, -- Allergy / Immunology     Medicare Specialty
				44811439, -- Trauma and orthopaedics service     SNOMED
				38004468, -- Physical Medicine And Rehabilitation     Medicare Specialty
				44808144, -- Well woman service     SNOMED
				44808431, -- Community child health service     SNOMED
				4148665, -- Orthotics service     SNOMED
				45756755, -- Child Abuse Pediatrics     ABMS
				45773017, -- Anatomic pathology service     SNOMED
				46270521, -- Pediatric infectious disease service     SNOMED
				45756794, -- Orthopaedic Sports Medicine     ABMS
				3654999, -- Neonatal service     SNOMED
				3657583, -- Vascular physiology service     SNOMED
				4147258, -- Public health service     SNOMED
				4148663, -- Community-based speech and language therapy service     SNOMED
				3657573, -- Orthogeriatric medicine service     SNOMED
				4149126, -- Family planning service     SNOMED
				36716232, -- Chiropractic service     SNOMED
				37310987, -- Narcotic addiction service with chronic pain management     SNOMED
				4148664, -- Community orthoptics service     SNOMED
				37312680, -- Neurology service     SNOMED
				4147256, -- Community orthotics service     SNOMED
				38003973, -- Undersea and Hyperbaric Preventive Medicine     NUCC
				38004477, -- Pediatric Medicine     Medicare Specialty
				44777673, -- Orthodontics     HES Specialty
				42537293, -- Aboriginal health service     SNOMED
				44777776, -- Paediatric Interventional Radiology     HES Specialty
				44811450, -- Paediatric diabetic medicine service     SNOMED
				44813775, -- Child psychiatry service     SNOMED
				45756799, -- Pathology - Hematology     ABMS
				45756811, -- Pediatric Hematology-Oncology     ABMS
				4147240, -- Assistive listening device service     SNOMED
				4147251, -- Dance therapy service     SNOMED
				4149136, -- Pediatric neurology service     SNOMED
				4150093, -- Radiotherapy service     SNOMED
				4150857, -- Child speech and language therapy service     SNOMED
				35609175, -- Perinatal psychiatry service     SNOMED
				37207433, -- Medication review service     SNOMED
				37312472, -- Child health service     SNOMED
				37312682, -- Nephrology service     SNOMED
				38003679, -- Prosthodontics     NUCC
				44811301, -- Gynaecological oncology service     SNOMED
				44811306, -- Clinical haematology service     SNOMED
				44811308, -- Burns care service     SNOMED
				44814049, -- Paediatric metabolic disease service     SNOMED
				45756762, -- Clinical Molecular Genetics     ABMS
				45756766, -- Cytopathology     ABMS
				45768464, -- Bacteriology service     SNOMED
				761961, -- Chemical dependency service     SNOMED
				764904, -- Pediatric allergy and immunology service     SNOMED
				903251, -- Brain Injury Psychiatry or Neurology     NUCC
				4147238, -- Audiological screening service     SNOMED
				36716215, -- Physiotherapy service     SNOMED
				37310990, -- Geriatric chronic pain management service     SNOMED
				38003677, -- Pediatric Dentistry     NUCC
				38003850, -- Emergency Medical Toxicology     NUCC
				38003963, -- Hospice and Palliative Rehabilitation Medicine     NUCC
				38003996, -- Sleep Psychiatry or Neurology     NUCC
				44808175, -- Adolescent psychiatry service     SNOMED
				44809687, -- Paediatric respiratory medicine service     SNOMED
				44811427, -- Medical virology service     SNOMED
				44812064, -- Mental health crisis resolution team     SNOMED
				44812112, -- Early intervention in psychosis team     SNOMED
				38004513, -- Gynecology / Oncology     Medicare Specialty
				45769503, -- Cardiac ultrasound service     SNOMED
				45770604, -- Oral microbiology service     SNOMED
				3657582, -- General internal medical service     SNOMED
				4147263, -- Head injury rehabilitation service     SNOMED
				4149144, -- Community-based podiatry service     SNOMED
				4150864, -- Rehabilitation service     SNOMED
				44811448, -- Paediatric medical oncology service     SNOMED
				38003943, -- Clinical and Laboratory Pediatrics Immunology     NUCC
				38004495, -- Peripheral Vascular Disease Medicine     Medicare Specialty
				44777743, -- Psychiatric Intensive Care     HES Specialty
				44809697, -- Paediatric interventional radiology service     SNOMED
				44811430, -- Haemophilia service     SNOMED
				45756747, -- Adolescent Medicine     ABMS
				45756757, -- Clinical Biochemical Genetics     ABMS
				45756788, -- Neurology with Special Qualification in Child Neurology     ABMS
				45756807, -- Pediatric Dermatology     ABMS
				45756815, -- Pediatric Pulmonology     ABMS
				45757608, -- Perinatology service     SNOMED
				3656675, -- Personal health record provider service     SNOMED
				3657590, -- Paediatric clinical pharmacology service     SNOMED
				4074187, -- Healthcare services     SNOMED
				4137050, -- Early years services     SNOMED
				4147239, -- Pediatric cochlear implant service     SNOMED
				4148654, -- Gynecology service     SNOMED
				4149163, -- Urology service     SNOMED
				4150861, -- Psychology service     SNOMED
				4249865, -- Emergency medical services     SNOMED
				38003856, -- Geriatric Family Medicine     NUCC
				38003977, -- Preventive Medical Toxicology     NUCC
				4147248, -- Pediatric oncology service     SNOMED
				44777671, -- Restorative dentistry (endodontics, periodontics and prosthodontics)     HES Specialty
				44777762, -- Paediatric Pain Management     HES Specialty
				44777766, -- Paediatric Clinical Haetology     HES Specialty
				44811420, -- Sport and exercise medicine service     SNOMED
				45769521, -- Blood bank service     SNOMED
				764912, -- Pediatric critical care service     SNOMED
				764939, -- Telehealth service     SNOMED
				903271, -- Public Health Medicine     HES Specialty
				38004509, -- Radiation Oncology     Medicare Specialty
				3657578, -- Audiovestibular medicine service     SNOMED
				44777773, -- Paediatric Medical Oncology     HES Specialty
				38004502, -- Hematology / Oncology     Medicare Specialty
				4150859, -- Psychiatry service     SNOMED
				4150866, -- Stroke service     SNOMED
				37207448, -- HIV (human immunodeficiency virus) nurse practitioner service     SNOMED
				37312123, -- Pediatric rheumatology service     SNOMED
				37312478, -- Clinical immunology and allergy service     SNOMED
				38003824, -- Neuromusculoskeletal Sports Medicine     NUCC
				38003946, -- Pediatric Neurodevelopmental Disabilities     NUCC
				38004009, -- Diagnostic Ultrasound     NUCC
				38004457, -- Osteopathic Manipulative Therapy     Medicare Specialty
				4149124, -- Clinical oncology service     SNOMED
				45756761, -- Clinical Informatics     ABMS
				45756806, -- Pediatric Critical Care Medicine     ABMS
				45756832, -- Undersea and Hyperbaric Medicine     ABMS
				45769520, -- Coagulation service     SNOMED
				46272888, -- Allergy service     SNOMED
				764909, -- Pediatric pathology service     SNOMED
				4148643, -- Complementary therapy service     SNOMED
				37311326, -- Respiratory medicine service     SNOMED
				44811304, -- Clinical microbiology service     SNOMED
				44811451, -- Palliative medicine service     SNOMED
				45756756, -- Child and Adolescent Psychiatry     ABMS
				45756814, -- Pediatric Otolaryngology     ABMS
				46285114, -- Diabetic medicine service     SNOMED
				764903, -- Pediatric anesthesiology service     SNOMED
				903265, -- Special care dentistry     HES Specialty
				903275, -- Sleep Medicine     Medicare Specialty
				3654283, -- Clinical allergy service     SNOMED
				4080737, -- Preventive service     SNOMED
				4148642, -- Hearing therapy service     SNOMED
				4148646, -- Adult intensive care service     SNOMED
				4148661, -- Hospital-based occupational therapy service     SNOMED
				4148667, -- Psychogeriatric service     SNOMED
				37312476, -- Radiation oncology service     SNOMED
				4149145, -- Hospital-based physiotherapy service     SNOMED
				4149146, -- Child physiotherapy service     SNOMED
				4150860, -- Child and adolescent psychiatry service     SNOMED
				4150862, -- Psychotherapy service     SNOMED
				4150865, -- Swallow clinic     SNOMED
				38004507, -- Medical Oncology     Medicare Specialty
				38003965, -- Spinal Cord Injury Rehabilitation Medicine     NUCC
				38003991, -- Addiction Psychiatry     NUCC
				44814024, -- Medical oncology service     SNOMED
				44777708, -- Histopathology     HES Specialty
				44777759, -- Paediatric Burns Care     HES Specialty
				45756793, -- Nuclear Radiology     ABMS
				45756803, -- Pathology-Anatomic / Pathology-Clinical     ABMS
				903263, -- Sport and exercise medicine     HES Specialty
				903266, -- Community sexual and reproductive health     HES Specialty
				37207436, -- Narcotic addiction service     SNOMED
				38003846, -- Undersea and Hyperbaric Emergency Medicine     NUCC
				38003851, -- Family Medicine     NUCC
				38003857, -- Hospice and Palliative Family Medicine     NUCC
				38003896, -- Nuclear Imaging and Therapy     NUCC
				38003960, -- Pediatric Medical Toxicology     NUCC
				38004494, -- Pain Management     Medicare Specialty
				44777712, -- Community medicine     HES Specialty
				44777808, -- Intermediate care (encompasses a range of multidisciplinary services designed to safeguard independence by maximising rehabilitation and recovery)     HES Specialty
				44808065, -- Homeopathy service     SNOMED
				44808156, -- NHS Direct     SNOMED
				44811206, -- Learning disability service     SNOMED
				44811433, -- Congenital heart disease service     SNOMED
				45756790, -- Neuropathology     ABMS
				45756798, -- Pathology - Forensic     ABMS
				45756823, -- Psychosomatic Medicine     ABMS
				45756824, -- Public Health and General Preventive Medicine     ABMS
				45769513, -- Virology service     SNOMED
				762431, -- Respite care service     SNOMED
				903248, -- Obesity (Bariatric) Preventive Medicine     NUCC
				3654350, -- Clinical neurophysiology service     SNOMED
				3654998, -- Postnatal service     SNOMED
				4147242, -- Pediatric intensive care service     SNOMED
				4147249, -- Neuropathology service     SNOMED
				4148657, -- Music therapy service     SNOMED
				4149142, -- Drama therapy service     SNOMED
				37207492, -- Periodontics service     SNOMED
				37312678, -- General practice service     SNOMED
				38003673, -- Dental Public Health     NUCC
				38003853, -- Addiction Family Medicine     NUCC
				38003930, -- Immunopathology     NUCC
				38004446, -- General Practice     Medicare Specialty
				38004456, -- Internal Medicine     Medicare Specialty
				38004485, -- Endocrinology     Medicare Specialty
				44809694, -- Paediatric neurodisability service     SNOMED
				44811426, -- Mental health dual diagnosis service     SNOMED
				44811436, -- Adult cystic fibrosis service     SNOMED
				44811444, -- Psychiatric intensive care service     SNOMED
				45756809, -- Pediatric Endocrinology     ABMS
				45769509, -- Molecular pathology service     SNOMED
				764865, -- Outpatient service     SNOMED
				903250, -- Neurocritical Care Medicine     NUCC
				3657568, -- Special care dentistry service     SNOMED
				3662309, -- School aged immunisation service     SNOMED
				4147241, -- Domiciliary visit service     SNOMED
				4149139, -- Pathology service     SNOMED
				4191843, -- Community specialist palliative care     SNOMED
				4194403, -- Specialist palliative care     SNOMED
				37312679, -- Genetic laboratory service     SNOMED
				38003828, -- Electrodiagnostic Medicine     NUCC
				38003998, -- Body Imaging     NUCC
				38004476, -- Nuclear Medicine     Medicare Specialty
				44811425, -- Mental health recovery and rehabilitation service     SNOMED
				45756780, -- Maternal and Fetal Medicine     ABMS
				45756787, -- Neurodevelopmental Disabilities     ABMS
				45756792, -- Neurotology     ABMS
				45768465, -- Mycology service     SNOMED
				45770603, -- Oral medicine service     SNOMED
				761964, -- Noninvasive vascular laboratory service     SNOMED
				762430, -- Pulmonary service     SNOMED
				3654284, -- Genitourinary medicine service     SNOMED
				3657599, -- Neuropsychiatry service     SNOMED
				3657617, -- Intensive care medicine service     SNOMED
				4147260, -- Rehabilitation psychiatry service     SNOMED
				4149138, -- Palliative care service     SNOMED
				4149155, -- Ear, nose and throat service     SNOMED
				38003833, -- Addiction Anesthesiology     NUCC
				38004025, -- Clinical Pharmacology     NUCC
				38004486, -- Podiatry     Medicare Specialty
				38004503, -- Preventive Medicine     Medicare Specialty
				42536211, -- Diabetic education service     SNOMED
				42538215, -- Neonatal intensive care service     SNOMED
				44777767, -- Paediatric Audiological Medicine     HES Specialty
				44777781, -- Paediatric neurology     HES Specialty
				44809689, -- Paediatric pain management service     SNOMED
				44811331, -- Crisis prevention assessment and treatment team     SNOMED
				45756759, -- Clinical Cytogenetics     ABMS
				45756789, -- Neuromuscular Medicine     ABMS
				45756818, -- Pediatric Rheumatology     ABMS
				33005, -- Psychiatry or Neurology     Provider
				4148639, -- Neonatal audiological screening service     SNOMED
				4148644, -- Mental health counseling service     SNOMED
				38003674, -- Endodontics     NUCC
				38003994, -- Pain Psychiatry or Neurology     NUCC
				44808429, -- Industrial therapy service     SNOMED
				44811952, -- Assertive outreach team     SNOMED
				45756797 -- Pathology - Clinical     ABMS
	)

	GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	NULL [ancestor_concept_id],
	'surgery_type_burn' as variable_name,
	NULL [variable_desc]
FROM
	VOCAB.CONCEPT
WHERE
	concept_id IN (
	44777674, -- Burn care     HES Specialty
	44811308 -- Burns care service     SNOMED
)

GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	NULL [ancestor_concept_id],
	'surgery_type_cardiothoracic' as variable_name,
	NULL [variable_desc]
FROM
	VOCAB.CONCEPT
WHERE
	concept_id IN (
	38004497, -- Cardiac Surgery     Medicare Specialty
	4148673, -- Cardiac surgery service     SNOMED
	4147265, -- Cardiothoracic surgery service     SNOMED
	45756830, -- Thoracic and Cardiac Surgery     ABMS
	38004473, -- Thoracic Surgery     Medicare Specialty
	4149153 -- Thoracic surgery service     SNOMED
)
GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	NULL [ancestor_concept_id],
	'surgery_type_ent' as variable_name,
	NULL [variable_desc]
FROM
	VOCAB.CONCEPT
WHERE
	concept_id IN (
762453, -- Otolaryngology service     SNOMED
38004449, -- Otolaryngology     Medicare Specialty
38003923) -- Otolaryngological Surgery     NUCC



INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	NULL [ancestor_concept_id],
	'surgery_type_gi' as variable_name,
	NULL [variable_desc]
FROM
	VOCAB.CONCEPT
WHERE
	concept_id IN (
38004471, -- Colorectal Surgery     Medicare Specialty
4149157, -- Colorectal surgery service     SNOMED
4150870, -- Gastrointestinal surgery service     SNOMED
4149156, -- General gastrointestinal surgery service     SNOMED
44777668, -- Hepatobiliary & pancreatic surgery (includes liver surgery but excludes liver transplantation see transplantation surgery)     HES Specialty
44811296, -- Hepatobiliary and pancreatic surgery service     SNOMED
4147268, -- Hepatobiliary surgical service     SNOMED
4149160, -- Pancreatic surgery service     SNOMED
44777669, -- Upper gastrointestinal surgery     HES Specialty
4147267) -- Upper gastrointestinal surgery service     SNOMED

GO
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	NULL [ancestor_concept_id],
	'surgery_type_interventional_cardiology' as variable_name,
	NULL [variable_desc]
FROM
	VOCAB.CONCEPT
WHERE
	concept_id IN (903276) -- Interventional Cardiology     Medicare Specialty
GO
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	NULL [ancestor_concept_id],
	'surgery_type_medicine_gi' as variable_name,
	NULL [variable_desc]
FROM
	VOCAB.CONCEPT
WHERE
	concept_id IN (38004455, -- Gastroenterology     Medicare Specialty
37207432, -- Hepatology service     SNOMED
3657572, -- Gastrointestinal physiology service     SNOMED
45763901, -- Gastroenterology service     SNOMED
38003876 -- Hepatology     NUCC
)

GO
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	NULL [ancestor_concept_id],
	'surgery_type_neurosurgery' as variable_name,
	NULL [variable_desc]
FROM
	VOCAB.CONCEPT
WHERE
	concept_id IN (
	38004459, -- Neurosurgery     Medicare Specialty
4149159, -- Neurosurgical service     SNOMED
44811332) -- Spinal surgery service     SNOMED

GO
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	NULL [ancestor_concept_id],
	'surgery_type_ob_gyn' as variable_name,
	NULL [variable_desc]
FROM
	VOCAB.CONCEPT
WHERE
	concept_id IN (
	43125859, -- Female Pelvic Medicine and Reconstructive Surgery Obstetrician / Gynecologist     NUCC
4150106, -- Obstetrics service     SNOMED
38003905, -- Obstetrics     NUCC
38003903, -- Hospice and Palliative Obstetrics / Gynecology     NUCC
38003900, -- Critical Care Obstetrics / Gynecology     NUCC
4149132, -- Obstetrics and gynecology service     SNOMED
38004461) -- Obstetrics / Gynecology     Medicare Specialty

GO
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	NULL [ancestor_concept_id],
	'surgery_type_ophthalmology' as variable_name,
	NULL [variable_desc]
FROM
	VOCAB.CONCEPT
WHERE
	concept_id IN (
	903238, -- Glaucoma Ophthalmology     NUCC
903239, -- Retina Ophthalmology     NUCC
3657574, -- Ophthalmic and vision science service     SNOMED
903243, -- Cornea and External Ophthalmology     NUCC
903240, -- Uveitis and Ocular Inflammatory Disease Ophthalmology     NUCC
903244, -- Ophthalmic Plastic and Reconstructive Surgery     NUCC
4148801, -- Ophthalmology service     SNOMED
903241, -- Neuro-Ophthalmology     NUCC
38004463, -- Ophthalmology     Medicare Specialty
44811205) -- Medical ophthalmology service     SNOMED

GO
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	NULL [ancestor_concept_id],
	'surgery_type_ortho' as variable_name,
	NULL [variable_desc]
FROM
	VOCAB.CONCEPT
WHERE
	concept_id IN (
	38003911, -- Adult Reconstructive Orthopaedic Surgery     NUCC
4147255, -- Hospital orthotics service     SNOMED
38003910, -- Orthopaedic Hand Surgery     NUCC
37311325, -- Prosthetic and orthotic service     SNOMED
4148675, -- Orthodontics service     SNOMED
38003912, -- Orthopaedic Surgery of the Spine     NUCC
38003915, -- Orthopaedic Trauma Surgery     NUCC
38003914, -- Orthopedic Sports Medicine Surgery     NUCC
38004465, -- Orthopedic Surgery     Medicare Specialty
4150872, -- Orthopedic service     SNOMED
44777734, -- Orthoptics     HES Specialty
38003682, -- Orthodontics and Dentofacial Orthopedics     NUCC
4150858, -- Orthoptics service     SNOMED
4149148, -- Hospital orthoptics service     SNOMED
44811439, -- Trauma and orthopaedics service     SNOMED
4148665, -- Orthotics service     SNOMED
45756794, -- Orthopaedic Sports Medicine     ABMS
3657573, -- Orthogeriatric medicine service     SNOMED
4148664, -- Community orthoptics service     SNOMED
4147256, -- Community orthotics service     SNOMED
44777673, -- Orthodontics     HES Specialty
37207453) -- Spine orthopedic surgery service     SNOMED

GO
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	NULL [ancestor_concept_id],
	'surgery_type_pediatric_surgery' as variable_name,
	NULL [variable_desc]
FROM
	VOCAB.CONCEPT
WHERE
	concept_id IN (
	45756765, -- Congenital Cardiac Surgery     ABMS
46270520, -- Pediatric gastroenterology service     SNOMED
37312121, -- Pediatric otolaryngology service     SNOMED
3657593, -- Paediatric hepatology service     SNOMED
45756810, -- Pediatric Gastroenterology     ABMS
903242, -- Pediatric Ophthalmology and Strabismus     NUCC
44777755, -- Paediatric Ophthalmology     HES Specialty
44809705, -- Paediatric burns care service     SNOMED
45756821, -- Pediatric Urology     ABMS
44809690, -- Paediatric ophthalmology service     SNOMED
44809680, -- Paediatric trauma and orthopaedics service     SNOMED
44777753, -- Paediatric Trauma and Orthopaedics     HES Specialty
44809679, -- Paediatric urology service     SNOMED
44777760, -- Paediatric Cardiac Surgery     HES Specialty
44809704, -- Paediatric cardiac surgery service     SNOMED
44777752, -- Paediatric Gastrointestinal Surgery     HES Specialty
44809699, -- Paediatric gastrointestinal surgery service     SNOMED
44777756, -- Paediatric Maxillo-Facial Surgery     HES Specialty
44809696, -- Paediatric maxillofacial surgery service     SNOMED
765928, -- Pediatric orthopedic service     SNOMED
44777757, -- Paediatric Neurosurgery     HES Specialty
44809693, -- Paediatric neurosurgery service     SNOMED
3657589, -- Paediatric oral and maxillofacial surgery service     SNOMED
44777758, -- Paediatric Plastic Surgery     HES Specialty
44777761, -- Paediatric Thoracic Surgery     HES Specialty
44809682, -- Paediatric thoracic surgery service     SNOMED
44777751, -- Paediatric Transplantation Surgery     HES Specialty
44809681, -- Paediatric transplantation surgery service     SNOMED
38003909, -- Pediatric Orthopaedic Surgery     NUCC
37207442, -- Pediatric plastic surgery service     SNOMED
45756819, -- Pediatric Surgery     ABMS
4150873, -- Pediatric surgical service     SNOMED
45756814, -- Pediatric Otolaryngology     ABMS
44777759, -- Paediatric Burns Care     HES Specialty
45756820) -- Pediatric Transplant Hepatology     ABMS

GO
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	NULL [ancestor_concept_id],
	'surgery_type_plastic' as variable_name,
	NULL [variable_desc]
FROM
	VOCAB.CONCEPT
WHERE
	concept_id IN (
	44777667, -- Breast surgery (includes suspected neoplasms, cysts etc, does not include cosmetic surgery)     HES Specialty
4150867, -- Breast surgery service     SNOMED
45756773, -- Female Pelvic Medicine and Reconstructive Surgery     ABMS
43125861, -- Female Pelvic Medicine and Reconstructive Surgery Urologist     NUCC
38003919, -- Otolaryngological Facial Plastic Surgery     NUCC
38004467, -- Plastic And Reconstructive Surgery     Medicare Specialty
38003971, -- Plastic Surgery of the Hand     NUCC
4147269, -- Plastic surgery service     SNOMED
45756822, -- Plastic Surgery Within the Head and Neck     ABMS
38003920, -- Plastic Surgery within the Head and Neck Otolaryngology     NUCC
765146) -- Reconstructive surgery service     SNOMED

GO

INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	NULL [ancestor_concept_id],
	'surgery_type_surgical_oncology' as variable_name,
	NULL [variable_desc]
FROM
	VOCAB.CONCEPT
WHERE
	concept_id IN (
	37207445, -- Breast surgical oncology service     SNOMED
45756764, -- Complex General Surgical Oncology     ABMS
38004508, -- Surgical Oncology     Medicare Specialty
44782506) -- Surgical oncology service     SNOMED


GO
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	NULL [ancestor_concept_id],
	'surgery_type_transplant' as variable_name,
	NULL [variable_desc]
FROM
	VOCAB.CONCEPT
WHERE
	concept_id IN (
	37116895, -- Transplant medicine service     SNOMED
44811309, -- Blood and marrow transplantation service     SNOMED
45756831, -- Transplant Hepatology     ABMS
44811307, -- Cardiothoracic transplantation service     SNOMED
38003827, -- Transplant Surgery     NUCC
4149161, -- Transplant surgery service     SNOMED
903279, -- Advanced Heart Failure and Transplant Cardiology     Medicare Specialty
44777666, -- Transplantation surgery (includes renal and liver transplants, excludes cardiothoracic transplantation)     HES Specialty
903281, -- Hematopoietic Cell Transplantation and Cellular Therapy     Medicare Specialty
44777676, -- Cardiothoracic transplantation (recognised specialist services only - includes 'outreach' facilities)     HES Specialty
44777800) -- Bone and marrow transplantation (previously part of clinical haematology)     HES Specialty


GO
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	NULL [ancestor_concept_id],
	'surgery_type_trauma' as variable_name,
	NULL [variable_desc]
FROM
	VOCAB.CONCEPT
WHERE
	concept_id IN (45756829, -- Surgical Critical Care     ABMS
38004016, -- Trauma Surgery     NUCC
4149162) -- Trauma surgery service     SNOMED


GO
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	NULL [ancestor_concept_id],
	'surgery_type_urology' as variable_name,
	NULL [variable_desc]
FROM
	VOCAB.CONCEPT
WHERE
	concept_id IN (
	38004474, -- Urology     Medicare Specialty
4149163) -- Urology service     SNOMED


GO
INSERT INTO dbo.IC3_Variable_Lookup_Table_v3_beta
SELECT 
	[concept_id],
    [concept_code],
    [concept_name],
    [vocabulary_id],
	concept_class_id,
    [domain_id],
	NULL [ancestor_concept_id],
	'surgery_type_vascular' as variable_name,
	NULL [variable_desc]
FROM
	VOCAB.CONCEPT
WHERE
	concept_id IN (
	38004023, -- Thoracic Surgery (Cardiothoracic Vascular Surgery)     NUCC
38004496, -- Vascular Surgery     Medicare Specialty
4150874) -- Vascular surgery service     SNOMED

GO








CREATE NONCLUSTERED INDEX [var_name_IC3_Variable_Lookup_Table_v3_beta] ON dbo.IC3_Variable_Lookup_Table_v3_beta
(
	[variable_name] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO

CREATE NONCLUSTERED INDEX [id_domain_IC3_Variable_Lookup_Table_v3_beta] ON dbo.IC3_Variable_Lookup_Table_v3_beta
(
	[concept_id] ASC,
	[domain_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO

CREATE NONCLUSTERED INDEX [concept_id_IC3_Variable_Lookup_Table_v3_beta] ON dbo.IC3_Variable_Lookup_Table_v3_beta
(
	[concept_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO

