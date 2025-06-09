DROP TABLE LoOkUp_ScHeMa.IC3_Variable_Lookup_Table;
GO
CREATE TABLE LoOkUp_ScHeMa.[IC3_Variable_Lookup_Table](
	[concept_id] [int] NOT NULL,
	[concept_code] [varchar](50) NOT NULL,
	[concept_name] [varchar](255) NOT NULL,
	[vocabulary_id] [varchar](20) NOT NULL,
	[domain] [varchar](20) NOT NULL,
	[variable_name] [varchar](50) NOT NULL,
 CONSTRAINT [id_var_IC3_Variable_Lookup_Table] UNIQUE NONCLUSTERED 
(
	[concept_id] ASC,
	[variable_name] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

INSERT INTO LoOkUp_ScHeMa.IC3_Variable_Lookup_Table
SELECT
	*
FROM
((SELECT
	F.concept_id,
	F.concept_code,
	F.concept_name,
	F.vocabulary_id,
	--F.ancestor_concept_id,
	F.domain,
	LOWER(F.variable_name) [variable_name]
FROM
(
SELECT c.concept_id,
	c.concept_name,
	c.vocabulary_id,
	c.concept_code,
	NULL ancestor_concept_id,
	'Observation' [domain],
	'Language' as variable_name
FROM 
	VoCaB_ScHeMa.CONCEPT c
WHERE
	c.concept_id IN (
		4267143, -- language
		4182347, -- world language
		4052785  -- Language Spoken
	)
UNION
SELECT
	c.concept_id,
	c.concept_name,
	c.vocabulary_id,
	c.concept_code,
	ca.ancestor_concept_id,
	c.domain_id [domain],
	'icu' as variable_name
FROM 
	VoCaB_ScHeMa.CONCEPT c
	INNER JOIN VoCaB_ScHeMa.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
WHERE
	standard_concept = 'S'
	AND
	ca.ancestor_concept_id IN (
		32037, -- Intensive Care
								581379, -- Inpatient Critical Care Facility
								4148981  -- Intesive care unit
	)
	AND
	c.concept_name NOT LIKE '%30%'
	AND
	c.concept_name NOT LIKE '%Telemetry%'
UNION
SELECT
	c.concept_id,
	c.concept_name,
	c.vocabulary_id,
	c.concept_code,
	ca.ancestor_concept_id,
	c.domain_id [domain],
	'inpatient_hospital_encounter' as variable_name
FROM 
	VoCaB_ScHeMa.CONCEPT c
	INNER JOIN VoCaB_ScHeMa.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
WHERE
	standard_concept = 'S'
	AND
	ca.ancestor_concept_id IN (
		38004515, -- Hospital
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
UNION
SELECT
	c.concept_id,
	c.concept_name,
	c.vocabulary_id,
	c.concept_code,
	ca.ancestor_concept_id,
	c.domain_id [domain],
	'cv_cardiac_arrest' as variable_name
FROM 
	VoCaB_ScHeMa.CONCEPT c
	INNER JOIN VoCaB_ScHeMa.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
WHERE
	standard_concept = 'S'
	AND
	ca.ancestor_concept_id IN (
		321042 -- SNOMED Cardiac Arrest
	)
UNION
SELECT
	c.concept_id,
	c.concept_name,
	c.vocabulary_id,
	c.concept_code,
	ca.ancestor_concept_id,
	c.domain_id [domain],
	'cv_hypo_no_shock' as variable_name
FROM 
	VoCaB_ScHeMa.CONCEPT c
	INNER JOIN VoCaB_ScHeMa.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
WHERE
	standard_concept = 'S'
	AND
	ca.ancestor_concept_id IN (
		317002 -- SNOMED Low Blood Pressure
	)
UNION
SELECT
	c.concept_id,
	c.concept_name,
	c.vocabulary_id,
	c.concept_code,
	ca.ancestor_concept_id,
	c.domain_id [domain],
	'cv_shock' as variable_name
FROM 
	VoCaB_ScHeMa.CONCEPT c
	INNER JOIN VoCaB_ScHeMa.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
WHERE
	standard_concept = 'S'
	AND
	ca.ancestor_concept_id IN (
		201965 -- SNOMED SHOCK
	)
UNION
SELECT
	c.concept_id,
	c.concept_name,
	c.vocabulary_id,
	c.concept_code,
	ca.ancestor_concept_id,
	c.domain_id [domain],
	'cv_hf' as variable_name
FROM 
	VoCaB_ScHeMa.CONCEPT c
	INNER JOIN VoCaB_ScHeMa.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
WHERE
	standard_concept = 'S'
	AND
	ca.ancestor_concept_id IN (
		316139 -- SNOMED Heart Failure
	)
UNION
SELECT
	c.concept_id,
	c.concept_name,
	c.vocabulary_id,
	c.concept_code,
	ca.ancestor_concept_id,
	c.domain_id [domain],
	CASE WHEN c.concept_id IN (SELECT descendant_concept_id FROM VoCaB_ScHeMa.CONCEPT_ANCESTOR WHERE ancestor_concept_id = 381316) THEN 'neuro_stroke'
	WHEN c.concept_id IN (SELECT DISTINCT descendant_concept_id FROM VoCaB_ScHeMa.CONCEPT_ANCESTOR WHERE ancestor_concept_id IN (374022, 192606, 374377)) THEN 'neuro_plegia_paralytic'
	WHEN c.concept_id IN (SELECT descendant_concept_id FROM VoCaB_ScHeMa.CONCEPT_ANCESTOR WHERE ancestor_concept_id = 373995) THEN 'delirium_icd'
	ELSE 'neuro_other' END [variable_name]
FROM 
	VoCaB_ScHeMa.CONCEPT c
	INNER JOIN VoCaB_ScHeMa.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
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

	UNION
SELECT
	c.concept_id,
	c.concept_name,
	c.vocabulary_id,
	c.concept_code,
	ca.ancestor_concept_id,
	c.domain_id [domain],
	CASE WHEN c.concept_id IN (SELECT descendant_concept_id FROM VoCaB_ScHeMa.CONCEPT_ANCESTOR WHERE ancestor_concept_id = 437474) THEN 'surg_infection'
	WHEN c.concept_id IN (SELECT DISTINCT descendant_concept_id FROM VoCaB_ScHeMa.CONCEPT_ANCESTOR WHERE ancestor_concept_id IN (442018)) THEN 'proc_graft_implant_foreign_body'
	WHEN c.concept_id IN (SELECT descendant_concept_id FROM VoCaB_ScHeMa.CONCEPT_ANCESTOR WHERE ancestor_concept_id IN (4002836, 37116361)) THEN 'proc_hemorrhage_hematoma_seroma'
	WHEN c.concept_id IN (SELECT descendant_concept_id FROM VoCaB_ScHeMa.CONCEPT_ANCESTOR WHERE ancestor_concept_id = 136580) THEN 'mech_wound'
	ELSE 'proc_non_hemorrhagic_technical' END [variable_name]
FROM 
	VoCaB_ScHeMa.CONCEPT c
	INNER JOIN VoCaB_ScHeMa.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
WHERE
	standard_concept = 'S'
	AND
	ca.ancestor_concept_id IN (37116361, --Accidental wound during procedure
								4300243, -- POSTOP COmplication
								36712819, -- Post Procedure abcess
								136580 -- SNOMED Dehiscence of surgical wound 
								)
UNION
SELECT
	c.concept_id,
	c.concept_name,
	c.vocabulary_id,
	c.concept_code,
	ca.ancestor_concept_id,
	c.domain_id [domain],
	'sepsis' as variable_name
FROM 
	VoCaB_ScHeMa.CONCEPT c
	INNER JOIN VoCaB_ScHeMa.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
WHERE
	standard_concept = 'S'
	AND
	ca.ancestor_concept_id IN (
		132797 -- SEPSIS
	)
	AND
	c.concept_name NOT LIKE '%chronic%'

UNION
SELECT
	c.concept_id,
	c.concept_name,
	c.vocabulary_id,
	c.concept_code,
	ca.ancestor_concept_id,
	c.domain_id [domain],
	CASE WHEN c.concept_id IN (SELECT descendant_concept_id FROM VoCaB_ScHeMa.CONCEPT_ANCESTOR WHERE ancestor_concept_id = 440417) THEN 'vte_pe'
	ELSE 'vte_deep_super_vein' END [variable_name]
FROM 
	VoCaB_ScHeMa.CONCEPT c
	INNER JOIN VoCaB_ScHeMa.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
WHERE
	standard_concept = 'S'
	AND
	ca.ancestor_concept_id IN (440417, -- PE
								4327889, -- VTE
								4133004, -- DVT
								318775 -- Venous Embolism
								)
UNION

SELECT
	c.concept_id,
	c.concept_name,
	c.vocabulary_id,
	c.concept_code,
	ca.ancestor_concept_id,
	c.domain_id [domain],
	'ckd' [variable_name]
FROM 
	VoCaB_ScHeMa.CONCEPT c
	INNER JOIN VoCaB_ScHeMa.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
WHERE
	standard_concept = 'S'
	AND
	ca.ancestor_concept_id IN (46271022 -- CKD
	)
UNION

SELECT
	c.concept_id,
	c.concept_name,
	c.vocabulary_id,
	c.concept_code,
	ca.ancestor_concept_id,
	c.domain_id [domain],
	'aki' [variable_name]
FROM 
	VoCaB_ScHeMa.CONCEPT c
	INNER JOIN VoCaB_ScHeMa.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
WHERE
	standard_concept = 'S'
	AND
	ca.ancestor_concept_id IN (197320, -- Acute renal failure syndrome SNOMED
								36716946  -- Acute renal insufficiency SNOMED
								)
UNION

SELECT
	c.concept_id,
	c.concept_name,
	c.vocabulary_id,
	c.concept_code,
	NULL ancestor_concept_id,
	'Observation' [domain],
	'Smoking_Status' as variable_name
FROM 
	VoCaB_ScHeMa.CONCEPT c
	INNER JOIN VoCaB_ScHeMa.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
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
	c.concept_id,
	c.concept_name,
	c.vocabulary_id,
	c.concept_code,
	ca.ancestor_concept_id,
	c.domain_id [domain],
	'mechanical_ventilation' as variable_name
FROM 
	VoCaB_ScHeMa.CONCEPT c
	INNER JOIN VoCaB_ScHeMa.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
WHERE
	standard_concept = 'S'
	AND
	ca.ancestor_concept_id IN (
		40493026, -- SNOMED MECH VENT
		45768197 -- SNOMED Ventilator
	)

UNION

SELECT c.concept_id,
	c.concept_name,
	c.vocabulary_id,
	c.concept_code,
	NULL ancestor_concept_id,
	c.domain_id [domain],
	'cam' as variable_name
FROM 
	VoCaB_ScHeMa.CONCEPT c
WHERE
	c.concept_id IN (
		42539072,
		44807161
	)
UNION

SELECT
	c.concept_id,
	c.concept_name,
	c.vocabulary_id,
	c.concept_code,
	ca.ancestor_concept_id,
	c.domain_id [domain],
	'dialysis' as variable_name
FROM 
	VoCaB_ScHeMa.CONCEPT c
	INNER JOIN VoCaB_ScHeMa.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
WHERE
	standard_concept = 'S'
	AND
	ca.ancestor_concept_id IN (
		4032243,  -- dialysis procedure snomed
		4265605, -- diaysis catheter
		4021071 -- dialysis fluid
	)
	AND NOT
	(c.concept_name LIKE '%Extracorporeal%'
	 AND
	 c.concept_name NOT LIKE '%dialysis%'
	 AND
	 c.concept_name NOT LIKE '%hemofiltration%')

UNION
SELECT
	c.concept_id,
	c.concept_name,
	c.vocabulary_id,
	c.concept_code,
	ca.ancestor_concept_id,
	c.domain_id [domain],
	'dialysis' as variable_name
FROM 
	VoCaB_ScHeMa.CONCEPT c
	INNER JOIN VoCaB_ScHeMa.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
WHERE
	standard_concept = 'S'
	AND
	ca.ancestor_concept_id IN (
		4269440 -- Dialysis Observable
	)
UNION

SELECT
	c.concept_id,
	c.concept_name,
	c.vocabulary_id,
	c.concept_code,
	NULL ancestor_concept_id,
	'Observation' [domain],
	'Marital_Status' as variable_name
FROM 
	VoCaB_ScHeMa.CONCEPT c
WHERE
	c.concept_id IN (
		3046344, -- LOINC Marital Status (check levels)
		4053609, -- SNOMED Marital Status (check levels)
		40766231 -- Alternate LOINC Marital Status (check levels)
	)

UNION

SELECT
	c.concept_id,
	c.concept_name,
	c.vocabulary_id,
	c.concept_code,
	NULL ancestor_concept_id,
	'Observation' [domain],
	'Admit_Priority' as variable_name
FROM 
	VoCaB_ScHeMa.CONCEPT c
WHERE
	c.concept_id IN (	
		46235349,  -- Admission Priority
		46236615  --Admission priority
	)

UNION

SELECT c.concept_id,
	c.concept_name,
	c.vocabulary_id,
	c.concept_code,
	NULL ancestor_concept_id,
	'Observation' [domain],
	CASE WHEN c.concept_id = 2000000033	THEN 'Admitting_Service'
		WHEN c.concept_id = 2000000034 THEN	'Discharge_Service'
		WHEN c.concept_id = 2000000035	THEN 'Scheduled_Surgical_Service'
		WHEN c.concept_id = 4149152 THEN 'Surgical_Service'
		WHEN c.concept_id = 2000000036	THEN 'Scheduled_Service'
		ELSE NULL END as variable_name

FROM 
	VoCaB_ScHeMa.CONCEPT c
	--INNER JOIN VoCaB_ScHeMa.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
WHERE
	standard_concept = 'S'
	AND
	c.concept_id IN (
		2000000033,	-- 'Admitting Service'
		2000000034, --	'Discharge Service'
		2000000035,	-- 'Scheduled Surgical Service'
		4149152, -- 'Surgical Service'
		2000000036  -- Scheduled Service
	)
	--AND
	--ca.ancestor_concept_id IN (
	--	4074187,  -- SNOMED Healthcare Services
	--	32577  -- Provider Specialty
	--)

UNION

SELECT c.concept_id,
	c.concept_name,
	c.vocabulary_id,
	c.concept_code,
	NULL ancestor_concept_id,
	'Measurement' [domain],
	'Weight' as variable_name
FROM 
	VoCaB_ScHeMa.CONCEPT c
	INNER JOIN VoCaB_ScHeMa.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
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

UNION

SELECT
	c.concept_id,
	c.concept_name,
	c.vocabulary_id,
	c.concept_code,
	NULL ancestor_concept_id,
	'Measurement' [domain],
	'Height' as variable_name
FROM 
	VoCaB_ScHeMa.CONCEPT c
WHERE
	c.concept_id IN (	
		607590,  -- Body Height SNOMED
		3036277 -- Body Height LOINC
	)

UNION

SELECT
	c.concept_id,
	c.concept_name,
	c.vocabulary_id,
	c.concept_code,
	NULL ancestor_concept_id,
	'Observation' [domain],
	'Scheduled_Anesthesia_Type' as variable_name
FROM 
	VoCaB_ScHeMa.CONCEPT c
WHERE
	c.concept_id = 2000000028

UNION

SELECT
	c.concept_id,
	c.concept_name,
	c.vocabulary_id,
	c.concept_code,
	ca.ancestor_concept_id,
	'Procedure' [domain],
	'Anesthesia_Type' as variable_name
FROM 
	VoCaB_ScHeMa.CONCEPT c
	INNER JOIN VoCaB_ScHeMa.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
WHERE
	ca.ancestor_concept_id IN (
		4174669,
		4303995,
		4100052,
		4160439,
		4097211,
		4078199,
		4332593,
		4228073,
		4335024,
		4200133
	)

UNION

SELECT c.concept_id,
	c.concept_name,
	c.vocabulary_id,
	c.concept_code,
	ca.ancestor_concept_id,
	'Measurement' [domain],
	'Urine_Output' as variable_name
FROM 
	VoCaB_ScHeMa.CONCEPT c
	INNER JOIN VoCaB_ScHeMa.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
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

UNION

SELECT
	c.concept_id,
	c.concept_name,
	c.vocabulary_id,
	c.concept_code,
	NULL ancestor_concept_id,
	'Measurement' [domain],
	'Blood_Loss' as variable_name
FROM 
	VoCaB_ScHeMa.CONCEPT c
WHERE
	c.concept_id = 21493943

UNION

SELECT
	c.concept_id,
	c.concept_name,
	c.vocabulary_id,
	c.concept_code,
	NULL ancestor_concept_id,
	'Measurement' [domain],
	'Body_Temperature' as variable_name
FROM 
	VoCaB_ScHeMa.CONCEPT c
	INNER JOIN VoCaB_ScHeMa.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
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

UNION

SELECT
	c.concept_id,
	c.concept_name,
	c.vocabulary_id,
	c.concept_code,
	ca.ancestor_concept_id,
	'Measurement' [domain],
	'SpO2' as variable_name
FROM 
	VoCaB_ScHeMa.CONCEPT c
	INNER JOIN VoCaB_ScHeMa.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
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

UNION

SELECT
	c.concept_id,
	c.concept_name,
	c.vocabulary_id,
	c.concept_code,
	NULL ancestor_concept_id,
	'Measurement' [domain],
	'FiO2' as variable_name
FROM 
	VoCaB_ScHeMa.CONCEPT c
	INNER JOIN VoCaB_ScHeMa.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
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

UNION

SELECT
	c.concept_id,
	c.concept_name,
	c.vocabulary_id,
	c.concept_code,
	NULL ancestor_concept_id,
	'Measurement' [domain],
	'ETCO2' as variable_name
FROM 
	VoCaB_ScHeMa.CONCEPT c
	INNER JOIN VoCaB_ScHeMa.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
WHERE
	ca.ancestor_concept_id IN (
		4353938,  -- SNOMED ETCO2
		3035357,  -- ETCO2 LOINC
		21490569, -- LOINC Airway Adapter
		3035115 -- LOINC Calculated
	)
	AND
	c.standard_concept = 'S'

UNION

SELECT
	c.concept_id,
	c.concept_name,
	c.vocabulary_id,
	c.concept_code,
	NULL ancestor_concept_id,
	'Measurement' [domain],
	'Mac_40' as variable_name
FROM 
	VoCaB_ScHeMa.CONCEPT c
WHERE
	c.concept_id = 3661605

UNION

SELECT
	c.concept_id,
	c.concept_name,
	c.vocabulary_id,
	c.concept_code,
	NULL ancestor_concept_id,
	'Measurement' [domain],
	'PEEP' as variable_name
FROM 
	VoCaB_ScHeMa.CONCEPT c
	INNER JOIN VoCaB_ScHeMa.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
WHERE
	ca.ancestor_concept_id IN (
		37075680,  -- LOINC PEEP
		3022875,  -- LOINC PEEP
		44782659  -- SNOMED PEEP
	)
	AND
	c.standard_concept = 'S'

UNION

SELECT
	c.concept_id,
	c.concept_name,
	c.vocabulary_id,
	c.concept_code,
	NULL ancestor_concept_id,
	'Measurement' [domain],
	'O2_Flow_Rate' as variable_name
FROM 
	VoCaB_ScHeMa.CONCEPT c
	INNER JOIN VoCaB_ScHeMa.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
WHERE
	ca.ancestor_concept_id IN (
		4141684,  -- SNOMED O2 Flow
		3005629  -- LOINC O2 Flow
	)
	AND
	c.standard_concept = 'S'



UNION

SELECT
	c.concept_id,
	c.concept_name,
	c.vocabulary_id,
	c.concept_code,
	NULL ancestor_concept_id,
	'Measurement' [domain],
	'Tidal_Volume' as variable_name
FROM 
	VoCaB_ScHeMa.CONCEPT c
	INNER JOIN VoCaB_ScHeMa.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
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

UNION

SELECT
	c.concept_id,
	c.concept_name,
	c.vocabulary_id,
	c.concept_code,
	NULL ancestor_concept_id,
	'Measurement' [domain],
	'Tidal_Volume_Inspired' as variable_name
FROM 
	VoCaB_ScHeMa.CONCEPT c
	INNER JOIN VoCaB_ScHeMa.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
WHERE
	ca.ancestor_concept_id IN (
		44782826,  -- SNOMED Inspiratory Tidal Volume
		3033780,  -- LOINC Tidal Volume Inspired
		3016166  -- LOINC Tidal Volume Inspired spont + mech
	)
	AND
	c.standard_concept = 'S'

UNION

SELECT
	c.concept_id,
	c.concept_name,
	c.vocabulary_id,
	c.concept_code,
	NULL ancestor_concept_id,
	'Measurement' [domain],
	'Tidal_Volume_Exhaled' as variable_name
FROM 
	VoCaB_ScHeMa.CONCEPT c
	INNER JOIN VoCaB_ScHeMa.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
WHERE
	ca.ancestor_concept_id IN (
		44782827, -- SNOMED Expiratory Tidal Volume
		21490789,  -- LOINC Expired Tidal Volume
		3015016  -- LOINC Tidal Volume Expired spont + mech
	)
	AND
	c.standard_concept = 'S'

UNION

SELECT
	c.concept_id,
	c.concept_name,
	c.vocabulary_id,
	c.concept_code,
	NULL ancestor_concept_id,
	'Measurement' [domain],
	'Respiratory_Rate' as variable_name
FROM 
	VoCaB_ScHeMa.CONCEPT c
	INNER JOIN VoCaB_ScHeMa.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
WHERE
	ca.ancestor_concept_id IN (
		4313591, -- Respiratory Rate SNOMED
		3024171  -- Respiratory Rate LOINC
	)
	AND
	c.standard_concept = 'S'
	AND
	c.concept_name NOT LIKE '%baseline%'

UNION

SELECT
	c.concept_id,
	c.concept_name,
	c.vocabulary_id,
	c.concept_code,
	NULL ancestor_concept_id,
	'Measurement' [domain],
	'PIP' as variable_name
FROM 
	VoCaB_ScHeMa.CONCEPT c
WHERE
	c.concept_id = 4101694

UNION

SELECT
	c.concept_id,
	c.concept_name,
	c.vocabulary_id,
	c.concept_code,
	NULL ancestor_concept_id,
	'Measurement' [domain],
	'Heart_Rate' as variable_name
FROM 
	VoCaB_ScHeMa.CONCEPT c
	INNER JOIN VoCaB_ScHeMa.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
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

UNION

SELECT DISTINCT
	c.concept_id,
	c.concept_name,
	c.vocabulary_id,
	c.concept_code,
	NULL ancestor_concept_id,
	'Measurement' [domain],
	'Diastolic_Blood_Pressure' as variable_name
FROM 
	VoCaB_ScHeMa.CONCEPT c
	INNER JOIN VoCaB_ScHeMa.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
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

UNION

SELECT DISTINCT
	c.concept_id,
	c.concept_name,
	c.vocabulary_id,
	c.concept_code,
	NULL ancestor_concept_id,
	'Measurement' [domain],
	'Systolic_Blood_Pressure' as variable_name
FROM 
	VoCaB_ScHeMa.CONCEPT c
	INNER JOIN VoCaB_ScHeMa.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
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

UNION

SELECT DISTINCT
	c.concept_id,
	c.concept_name,
	c.vocabulary_id,
	c.concept_code,
	NULL ancestor_concept_id,
	'Measurement' [domain],
	'Mean_Arterial_Pressure' as variable_name
FROM 
	VoCaB_ScHeMa.CONCEPT c
	INNER JOIN VoCaB_ScHeMa.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
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

	) F)

UNION

(SELECT
	t.concept_id,
	t.concept_code,
	t.concept_name,
	t.vocabulary_id,
	'labs' [domain],
	--t.domain,
	LOWER(t.variable_name) [variable_name]
FROM
((SELECT
        c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'Measurement' [domain], 'ALP' [variable_name]
FROM
    VoCaB_ScHeMa.CONCEPT_RELATIONSHIP cr
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on cr.concept_id_1 = c.concept_id
WHERE
    cr.relationship_id = 'Is a'
    AND
    cr.concept_id_2 = 4230636 -- alp measurement
    AND
    (c.concept_name LIKE '%blood%' OR c.concept_name LIKE '%serum%' OR c.concept_name LIKE '%plasma%')
    AND
    c.concept_name NOT LIKE '% ratio%'
	AND c.concept_name not like '% cord%'
	AND c.concept_name NOT LIKE '%total%')

UNION 
												
(SELECT
	c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'Measurement' [domain], 'aPTT' [variable_name]
FROM
    VoCaB_ScHeMa.CONCEPT_RELATIONSHIP cr
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on cr.concept_id_1 = c.concept_id
WHERE
    cr.relationship_id = 'has component'
    AND
    cr.concept_id_2 =40775918)-- aptt measurement
																											
UNION

	(SELECT DISTINCT
    c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id,
	--ca.ancestor_concept_id,
	'Measurement' [domain], 'Bilirubin Total' [variable_name]
FROM
        VoCaB_ScHeMa.CONCEPT_ANCESTOR ca
        INNER JOIN VoCaB_ScHeMa.CONCEPT c on c.concept_id = ca.descendant_concept_id
    WHERE
        ca.ancestor_concept_id IN (
37398740, -- snomed bicarb
4227915 -- bicarb
)
AND
c.concept_name LIKE '%bicarb%'
AND
c.concept_name NOT LIKE '%fetus%'
AND
(c.concept_name LIKE '%blood%' OR c.concept_name LIKE '%serum%' OR c.concept_name LIKE '%plasma%' OR c.concept_name LIKE '%standard%'))

UNION
	(SELECT
        c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'Measurement' [domain], 'Bicarbonate' [variable_name]
FROM
    VoCaB_ScHeMa.CONCEPT_RELATIONSHIP cr
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on cr.concept_id_1 = c.concept_id
WHERE
    cr.relationship_id = 'has component'
    AND
    cr.concept_id_2 = 40792440--  Bicarbonate 
    AND
    (c.concept_name LIKE '%blood%' OR c.concept_name LIKE '%serum%' OR c.concept_name LIKE '%plasma%')
    AND
    c.concept_name NOT LIKE '% cord%'
    AND
    c.concept_name NOT LIKE '%Fetus%')

UNION
	(SELECT
        c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'Measurement' [domain], 'Calcium' [variable_name]
FROM
    VoCaB_ScHeMa.CONCEPT_RELATIONSHIP cr
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on cr.concept_id_1 = c.concept_id
WHERE
    cr.relationship_id = 'Is a'
                                                     
    AND
    cr.concept_id_2 = 4216722--  Calcium Measurement
    AND
    (c.concept_name LIKE '%blood%' OR c.concept_name LIKE '%serum%' OR c.concept_name LIKE '%plasma%')
    AND
    c.concept_name NOT LIKE '% cord%'
    AND
    c.concept_name NOT LIKE '%Presence%')
                                                 
UNION
	(SELECT
        c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'Measurement' [domain], 'Creatinine' [variable_name]
FROM
    VoCaB_ScHeMa.CONCEPT_RELATIONSHIP cr
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on cr.concept_id_1 = c.concept_id
WHERE
    cr.relationship_id = 'Is a'
    AND
    cr.concept_id_2 = 4324383 -- Creatinine measurement
    AND
    (c.concept_name LIKE '%blood%' OR c.concept_name LIKE '%serum%' OR c.concept_name LIKE '%plasma%')
    AND
    c.concept_name NOT LIKE '% ratio%')

UNION
	(SELECT
        c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'Measurement' [domain], 'C-reactive protein' [variable_name]
FROM
    VoCaB_ScHeMa.CONCEPT_RELATIONSHIP cr
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on cr.concept_id_1 = c.concept_id
WHERE
    cr.relationship_id = 'Is a '
    AND
    cr.concept_id_2 = 4208414--  C-reactive protein Measurement
    AND
    (c.concept_name LIKE '%blood%' OR c.concept_name LIKE '%serum%' OR c.concept_name LIKE '%plasma%')
    AND
    c.concept_name NOT LIKE '% cord%'
    AND
    c.concept_name NOT LIKE '%Presence%')

UNION
	(SELECT
        c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'Measurement' [domain], 'Eosinophils' [variable_name]
FROM
    VoCaB_ScHeMa.CONCEPT_RELATIONSHIP cr
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on cr.concept_id_1 = c.concept_id
WHERE
    cr.relationship_id = 'has component'
    AND
    cr.concept_id_2 = 40795733--  Eosinophils
    AND
    (c.concept_name LIKE '%blood%' OR c.concept_name LIKE '%serum%' OR c.concept_name LIKE '%plasma%')
    AND
    c.concept_name NOT LIKE '% cord%'
    AND
    c.concept_name NOT LIKE '%presence%')

UNION
	(SELECT
        c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'Measurement' [domain], 'Glucose ' [variable_name]
FROM
    VoCaB_ScHeMa.CONCEPT_RELATIONSHIP cr
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on cr.concept_id_1 = c.concept_id
WHERE
    cr.relationship_id = 'Is a '
    AND
    cr.concept_id_2 =4144235 -- Glucose  Measurement
    AND
    (c.concept_name LIKE '%blood%' OR c.concept_name LIKE '%serum%' OR c.concept_name LIKE '%plasma%')
    AND
    c.concept_name NOT LIKE '% cord%'
    AND
    c.concept_name NOT LIKE '%Presence%'
	AND
	c.concept_name NOT LIKE '%challenge%')


UNION
	(SELECT
        c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'Measurement' [domain], 'HBA1C' [variable_name]
FROM
    VoCaB_ScHeMa.CONCEPT_RELATIONSHIP cr
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on cr.concept_id_1 = c.concept_id
WHERE
    cr.relationship_id = 'Is a '
    AND
    cr.concept_id_2 =4184637 -- Hemoglobin A1C (HBA1C) measurement
    AND
    (c.concept_name LIKE '%blood%' OR c.concept_name LIKE '%serum%' OR c.concept_name LIKE '%plasma%')
    AND
    c.concept_name NOT LIKE '% cord%'
    AND
    c.concept_name NOT LIKE '%Presence%')

												
UNION
	(SELECT
        c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'Measurement' [domain], 'Hematocrit' [variable_name]
FROM
    VoCaB_ScHeMa.CONCEPT_RELATIONSHIP cr
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on cr.concept_id_1 = c.concept_id
WHERE
    cr.relationship_id = 'has component'
    AND
    cr.concept_id_2 = 40789179--  Hematocrit 
    AND
    (c.concept_name LIKE '%blood%' OR c.concept_name LIKE '%serum%' OR c.concept_name LIKE '%plasma%')
    AND
    c.concept_name NOT LIKE '% cord%'
    AND
    c.concept_name NOT LIKE '%presence%')

UNION
	(SELECT
        c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'Measurement' [domain], 'IMMGranulocytes' [variable_name]
FROM
    VoCaB_ScHeMa.CONCEPT_RELATIONSHIP cr
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on cr.concept_id_1 = c.concept_id
WHERE
    cr.relationship_id = 'has component'
    AND
    cr.concept_id_2 = 40776249-- IMMGranulocytes  
    AND
    (c.concept_name LIKE '%blood%' OR c.concept_name LIKE '%serum%' OR c.concept_name LIKE '%plasma%')
    AND
    c.concept_name NOT LIKE '% cord%'
    AND
    c.concept_name NOT LIKE '%presence%')

UNION
	(SELECT
        c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'Measurement' [domain], 'MDW' [variable_name]
FROM
    VoCaB_ScHeMa.CONCEPT_RELATIONSHIP cr
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on cr.concept_id_1 = c.concept_id
WHERE
    cr.relationship_id = 'has component'
    AND
    cr.concept_id_2 = 37041679--Monocyte distribution width 
    AND
    (c.concept_name LIKE '%blood%' OR c.concept_name LIKE '%serum%' OR c.concept_name LIKE '%plasma%'))
                                                   
	UNION
	(SELECT
        c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'Measurement' [domain], 'Monocytes' [variable_name]
FROM
    VoCaB_ScHeMa.CONCEPT_RELATIONSHIP cr
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on cr.concept_id_1 = c.concept_id
WHERE
    cr.relationship_id = 'has component'
    AND
    cr.concept_id_2 = 40785793-- Monocytes
    AND
    (c.concept_name LIKE '%blood%' OR c.concept_name LIKE '%serum%' OR c.concept_name LIKE '%plasma%')
    AND
    c.concept_name NOT LIKE '% cord%'
    AND
    c.concept_name NOT LIKE '%Fetus%')

UNION
	(SELECT
        c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'Measurement' [domain], 'pH' [variable_name]
FROM
    VoCaB_ScHeMa.CONCEPT_RELATIONSHIP cr
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on cr.concept_id_1 = c.concept_id
WHERE
    cr.relationship_id = 'Is a '
    AND
    cr.concept_id_2 =4215028 -- pH measurement
    AND
    (c.concept_name LIKE '%blood%' OR c.concept_name LIKE '%serum%' OR c.concept_name LIKE '%plasma%')
    AND
    c.concept_name NOT LIKE '% cord%'
    AND
    c.concept_name NOT LIKE '%Fetus%')
                                              	
UNION
		(SELECT
            c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'Measurement' [domain], 'Potassium' [variable_name]
    FROM
        VoCaB_ScHeMa.CONCEPT_RELATIONSHIP cr
        INNER JOIN VoCaB_ScHeMa.CONCEPT c on cr.concept_id_1 = c.concept_id
    WHERE
        cr.relationship_id = 'Is a '
        AND
        cr.concept_id_2 =4245152 -- Potassium measurement
        AND
        (c.concept_name LIKE '%blood%' OR c.concept_name LIKE '%serum%' OR c.concept_name LIKE '%plasma%')
        AND
        c.concept_name NOT LIKE '% cord%'
        AND
        c.concept_name NOT LIKE '%Presence%')

UNION

(SELECT
        c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'Measurement' [domain], 'Platelet' [variable_name]
    FROM
        VoCaB_ScHeMa.CONCEPT_RELATIONSHIP cr
        INNER JOIN VoCaB_ScHeMa.CONCEPT c on cr.concept_id_1 = c.concept_id
    WHERE
        cr.relationship_id = 'Has component'
        AND
        cr.concept_id_2 = 40779159 -- Platelets
        AND
        (c.concept_name LIKE '%blood%' OR c.concept_name LIKE '%serum%' OR c.concept_name LIKE '%plasma%')
        AND
        c.concept_name NOT LIKE '% cord%'
        AND
        c.concept_name NOT LIKE '%Fetus%')

UNION

   (SELECT
        c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'Measurement' [domain], 'Lactate' [variable_name]
    FROM
        VoCaB_ScHeMa.CONCEPT_RELATIONSHIP cr
        INNER JOIN VoCaB_ScHeMa.CONCEPT c on cr.concept_id_1 = c.concept_id
    WHERE
        cr.relationship_id = 'Has component'
        AND
        cr.concept_id_2 = 40785884 -- Lactate
        AND
        (c.concept_name LIKE '%blood%' OR c.concept_name LIKE '%serum%' OR c.concept_name LIKE '%plasma%')
        AND
        c.concept_name NOT LIKE '% cord%'
        AND
        c.concept_name NOT LIKE '%Fetus%')

UNION

(SELECT
    c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'Measurement' [domain], 'Bilirubin Total' [variable_name]
FROM
        VoCaB_ScHeMa.CONCEPT_ANCESTOR ca
        INNER JOIN VoCaB_ScHeMa.CONCEPT c on c.concept_id = ca.descendant_concept_id
    WHERE
        ca.ancestor_concept_id = 4118986 -- Billirubin Measurement
        AND
        (c.concept_name LIKE '%blood%' OR c.concept_name LIKE '%serum%' OR c.concept_name LIKE '%plasma%')
        AND
        c.concept_name NOT LIKE '% cord%'
        AND
        c.concept_name NOT LIKE '%Presence%'
        AND
        c.concept_name NOT LIKE '%feces%'
        AND
        c.concept_name LIKE '%total%'
		AND
		c.concept_id NOT IN (3035521, 4154343))

UNION

(SELECT
    c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'Measurement' [domain], 'Bilirubin Direct' [variable_name]
FROM
        VoCaB_ScHeMa.CONCEPT_ANCESTOR ca
        INNER JOIN VoCaB_ScHeMa.CONCEPT c on c.concept_id = ca.descendant_concept_id
    WHERE
        ca.ancestor_concept_id = 4118986 -- Billirubin Measurement
        AND
        (c.concept_name LIKE '%blood%' OR c.concept_name LIKE '%serum%' OR c.concept_name LIKE '%plasma%')
        AND
        c.concept_name NOT LIKE '% cord%'
        AND
        c.concept_name NOT LIKE '%Presence%'
        AND
        c.concept_name NOT LIKE '%feces%'
        AND
        c.concept_name LIKE '%direct%'
		AND
		c.concept_name NOT LIKE '%indirect%'
		AND
		c.concept_id NOT IN (3035521, 4154343))

UNION

(SELECT
    c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'Measurement' [domain], 'Bilirubin Indirect' [variable_name]
FROM
        VoCaB_ScHeMa.CONCEPT_ANCESTOR ca
        INNER JOIN VoCaB_ScHeMa.CONCEPT c on c.concept_id = ca.descendant_concept_id
    WHERE
        ca.ancestor_concept_id = 4118986 -- Billirubin Measurement
        AND
        (c.concept_name LIKE '%blood%' OR c.concept_name LIKE '%serum%' OR c.concept_name LIKE '%plasma%')
        AND
        c.concept_name NOT LIKE '% cord%'
        AND
        c.concept_name NOT LIKE '%Presence%'
        AND
        c.concept_name NOT LIKE '%feces%'
        AND
        c.concept_name LIKE '%indirect%'
		AND
		c.concept_name NOT LIKE '%conjugated%'
		AND
		c.concept_id NOT IN (3035521, 4154343))

UNION

(SELECT
        c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'Measurement' [domain], 'ALBUMIN_UR' [variable_name]
FROM
    VoCaB_ScHeMa.CONCEPT_RELATIONSHIP cr
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on cr.concept_id_1 = c.concept_id
WHERE
    cr.relationship_id = 'Is a'
    AND
    cr.concept_id_2 = 4152996 -- Urine albumin measurement
    AND
    (c.concept_name LIKE '%urine%')
    AND
    c.concept_name NOT LIKE '% ratio%'
    AND c.concept_name not like '% cord%'
    AND c.concept_name NOT LIKE '%challenge%'
    AND c.concept_name NOT LIKE '% post %'
    AND c.concept_name NOT LIKE '%micro%'
    AND c.concept_name NOT LIKE '%creatinine%'
    AND c.concept_name NOT LIKE '%hour%')
UNION

(SELECT
        c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'Measurement' [domain], 'ALBUMIN' [variable_name]
FROM
    VoCaB_ScHeMa.CONCEPT_RELATIONSHIP cr
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on cr.concept_id_1 = c.concept_id
WHERE
    cr.relationship_id = 'Is a'
    AND
    cr.concept_id_2 = 4097664 -- albumin measurement
    AND
    (c.concept_name LIKE '%blood%' OR c.concept_name LIKE '%serum%' OR c.concept_name LIKE '%plasma%')
    AND
    c.concept_name NOT LIKE '% ratio%'
    AND c.concept_name not like '% cord%'
    AND c.concept_name NOT LIKE '%challenge%'
    AND c.concept_name NOT LIKE '% post %')

UNION

(SELECT
c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'Measurement' [domain], 'ALBUMIN_UR_24H' [variable_name]

FROM
    VoCaB_ScHeMa.CONCEPT_ANCESTOR ca
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on c.concept_id = ca.descendant_concept_id

WHERE
    ca.ancestor_concept_id = 4017498
    AND
    c.concept_name NOT LIKE '%microalbumin%'
    AND
    c.concept_name LIKE '%24%'
    AND
    c.concept_name LIKE '%/time%')

UNION

(SELECT
c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'Measurement' [domain], 'ALT' [variable_name]

FROM
    VoCaB_ScHeMa.CONCEPT_ANCESTOR ca
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on c.concept_id = ca.descendant_concept_id

WHERE
    ca.ancestor_concept_id = 4095055
    AND
    c.concept_name NOT LIKE '%Maximum%')

UNION

(SELECT
c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'Measurement' [domain], 'Anion_Gap' [variable_name]

FROM
    VoCaB_ScHeMa.CONCEPT_ANCESTOR ca
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on c.concept_id = ca.descendant_concept_id

WHERE
    ca.ancestor_concept_id IN (4103762, 37037994, 37075673, 37066307)
    AND
    c.standard_concept = 'S')

UNION

(SELECT
c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'Measurement' [domain], 'APR_UR' [variable_name]

FROM
    VoCaB_ScHeMa.CONCEPT_ANCESTOR ca
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on c.concept_id = ca.descendant_concept_id

WHERE
    ca.ancestor_concept_id IN (40655889, 37036943, 4017498)
    AND
    c.standard_concept = 'S'
    AND
    c.concept_name LIKE '%total%'
    AND
    c.concept_name NOT LIKE '%24%'
    AND
    c.concept_name NOT LIKE '%duration%')

UNION

(SELECT
c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'Measurement' [domain], 'APR_UR_24H' [variable_name]

FROM
    VoCaB_ScHeMa.CONCEPT_ANCESTOR ca
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on c.concept_id = ca.descendant_concept_id

WHERE
    ca.ancestor_concept_id IN (40655889, 37036943, 4017498)
    AND
    c.standard_concept = 'S'
    AND
    c.concept_name LIKE '%total%'
    AND
    c.concept_name LIKE '%24%')

UNION

(SELECT
c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'Measurement' [domain], 'AST' [variable_name]

FROM
    VoCaB_ScHeMa.CONCEPT_ANCESTOR ca
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on c.concept_id = ca.descendant_concept_id

WHERE
    ca.ancestor_concept_id IN (4263457)
    AND
    c.standard_concept = 'S'
    AND
    c.concept_name NOT LIKE '%ratio%'
    AND
    (c.concept_name LIKE '%blood%' OR c.concept_name LIKE '%serum%' OR c.concept_name LIKE '%plasma%'))

UNION

(SELECT
c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'Measurement' [domain], 'BASE_DEFICIT' [variable_name]

FROM
    VoCaB_ScHeMa.CONCEPT_ANCESTOR ca
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on c.concept_id = ca.descendant_concept_id

WHERE
    ca.ancestor_concept_id IN (4194291, 4095105)
    AND
    c.standard_concept = 'S'
    AND
    c.concept_name LIKE '%deficit%')

UNION

(SELECT
c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'Measurement' [domain], 'BASE_EXCESS' [variable_name]

FROM
    VoCaB_ScHeMa.CONCEPT_ANCESTOR ca
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on c.concept_id = ca.descendant_concept_id

WHERE
    ca.ancestor_concept_id IN (4194291, 4095105)
    AND
    c.standard_concept = 'S'
    AND
    c.concept_name LIKE '%excess%')

UNION

(SELECT
c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'Measurement' [domain], 'Basophils' [variable_name]

FROM
    VoCaB_ScHeMa.CONCEPT_ANCESTOR ca
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on c.concept_id = ca.descendant_concept_id

WHERE
    ca.ancestor_concept_id IN (37042222)
    AND
    c.standard_concept = 'S')) as t
WHERE
	t.concept_name NOT LIKE '% PO'
	AND
	t.concept_name NOT LIKE '% IV'
	AND
	t.concept_name NOT LIKE '%challenge%'
	AND
	t.concept_name NOT LIKE '% post %')
UNION
(SELECT
	c.concept_id [drug_concept_id],
	c.concept_code   [drug_concept_code],
	c.concept_name   [drug_concept_name],
    --c.concept_class_id  [drug_concept_class],
	c.vocabulary_id,
	'Drug' [domain],
	--class_name,
	LOWER(drug_category) [variable_name]

FROM
	(SELECT DISTINCT
  atc.concept_id as atc_class_id,
  atc.concept_name as class_name,
  CASE WHEN atc.concept_code = 'J01G' THEN 'AMINOGLYCOSIDES'
	WHEN atc.concept_code IN ('C09A', 'C09B', 'C09D', 'C09C') THEN 'ACEIs_ARBs'
	WHEN atc.concept_code IN ('R06AE05', 'R06AE55', 'A04A','R06AA10') THEN 'ANTIEMETICS'
	WHEN atc.concept_code IN ('A01AD05','B01AC06','N02BA01') THEN 'ASPIRIN'
	WHEN atc.concept_code = 'C07' THEN 'BETA_BLOCKERS'
	WHEN atc.concept_code IN ('B05XA02', 'B05CB04', 'B05XA08') THEN 'BICARBONATES'
	WHEN atc.concept_code IN ('G01A', --	ANTIINFECTIVES AND ANTISEPTICS, EXCL. COMBINATIONS WITH CORTICOSTEROIDS
						 'S02B', --	CORTICOSTEROIDS
						 'S03B', --	CORTICOSTEROIDS
						 'S02C', --	CORTICOSTEROIDS AND ANTIINFECTIVES IN COMBINATION
						 'S03C', --	CORTICOSTEROIDS AND ANTIINFECTIVES IN COMBINATION
						 'H02B', --	CORTICOSTEROIDS FOR SYSTEMIC USE, COMBINATIONS
						 'H02A', --	CORTICOSTEROIDS FOR SYSTEMIC USE, PLAIN
						 'D07C', --	CORTICOSTEROIDS, COMBINATIONS WITH ANTIBIOTICS
						 'D07B', --	CORTICOSTEROIDS, COMBINATIONS WITH ANTISEPTICS
						 'D07X', --	CORTICOSTEROIDS, OTHER COMBINATIONS
						 'D07A' --	CORTICOSTEROIDS, PLAIN
						 ) THEN 'CORTICOSTEROIDS'
	WHEN atc.concept_code IN ('C03', --	DIURETICS
						 'S01EC01', --	acetazolamide; systemic
						 'B05BC01', --	mannitol; parenteral
						 'V04CX04', --	mannitol
						 'V04CC01', --	sorbitol
						 'B05CX04', --	mannitol
						 'A06AD16' --	mannitol; oral
						) THEN 'DIURETICS'
	WHEN atc.concept_code IN ('M01A' --	ANTIINFLAMMATORY AND ANTIRHEUMATIC PRODUCTS, NON-STEROIDS
						) THEN 'NSAIDS'
	WHEN atc.concept_code IN ('C01CA', --	Adrenergic and dopaminergic agents
							  'H01BA', --	Vasopressin and analogues
						     'C01CE02' --	milrinone; parenteral
						     ) THEN 'PRESSORS_INOTROPES'
	WHEN atc.concept_code IN ('C10AA', --	HMG CoA reductase inhibitors
						'C10BA' --	HMG CoA reductase inhibitors in combination with other lipid modifying agents
						) THEN 'STATINS'
	WHEN atc.concept_code IN ('J01XA01', --	vancomycin; parenteral
						 'A07AA09', --	vancomycin; oral
						 'S01AA28' --	vancomycin
						 ) THEN 'VANCOMYCIN'
	ELSE NULL
	END [drug_category]
FROM
	VoCaB_ScHeMa.concept atc

WHERE
	atc.vocabulary_id='ATC'
	AND
	(
	atc.concept_code = 'J01G' --AMINOGLYCOSIDES
	OR
	atc.concept_code IN ('C09A', 'C09B', 'C09D', 'C09C') --ACEIs_ARBs
	OR
	atc.concept_code IN ('R06AE05', 'R06AE55', --meclizine
						 'A04A', -- ANTIEMETICS
						 'R06AA10' -- trimethobenzamide
						 ) -- ANTIEMETICS
	OR
	atc.concept_code IN ('A01AD05',
						 'B01AC06',
						 'N02BA01') -- ASPIRIN
	OR
	atc.concept_code = 'C07' -- BETA_BLOCKERS
	OR
	atc.concept_code IN ('B05XA02', 'B05CB04',-- BICARBONATES
						 'B05XA08' -- sodium acetate
						 ) -- BICARBONATES
	OR
	atc.concept_code IN ('G01A', --	ANTIINFECTIVES AND ANTISEPTICS, EXCL. COMBINATIONS WITH CORTICOSTEROIDS
						 'S02B', --	CORTICOSTEROIDS
						 'S03B', --	CORTICOSTEROIDS
						 'S02C', --	CORTICOSTEROIDS AND ANTIINFECTIVES IN COMBINATION
						 'S03C', --	CORTICOSTEROIDS AND ANTIINFECTIVES IN COMBINATION
						 'H02B', --	CORTICOSTEROIDS FOR SYSTEMIC USE, COMBINATIONS
						 'H02A', --	CORTICOSTEROIDS FOR SYSTEMIC USE, PLAIN
						 'D07C', --	CORTICOSTEROIDS, COMBINATIONS WITH ANTIBIOTICS
						 'D07B', --	CORTICOSTEROIDS, COMBINATIONS WITH ANTISEPTICS
						 'D07X', --	CORTICOSTEROIDS, OTHER COMBINATIONS
						 'D07A' --	CORTICOSTEROIDS, PLAIN
						 ) -- CORTICOSTEROIDS
	OR
	atc.concept_code IN ('C03', --	DIURETICS
						 'S01EC01', --	acetazolamide; systemic
						 'B05BC01', --	mannitol; parenteral
						 'V04CX04', --	mannitol
						 'V04CC01', --	sorbitol
						 'B05CX04', --	mannitol
						 'A06AD16' --	mannitol; oral
						)-- DIURETICS
	OR
	atc.concept_code IN ('M01A' --	ANTIINFLAMMATORY AND ANTIRHEUMATIC PRODUCTS, NON-STEROIDS
						) -- NSAIDS
	OR
	atc.concept_code IN ('C01CA', --	Adrenergic and dopaminergic agents
						 'H01BA', --	Vasopressin and analogues
						 'C01CE02' --	milrinone; parenteral
						 )  -- PRESSORS_INOTROPES
	OR
	atc.concept_code IN ('C10AA', --	HMG CoA reductase inhibitors
						'C10BA' --	HMG CoA reductase inhibitors in combination with other lipid modifying agents
						) -- STATINS
	OR
	atc.concept_code IN ('J01XA01', --	vancomycin; parenteral
						 'A07AA09', --	vancomycin; oral
						 'S01AA28' --	vancomycin
						 ) -- VANCOMYCIN
	)) as base
	INNER JOIN VoCaB_ScHeMa.concept_ancestor ca on ca.ancestor_concept_id = base.atc_class_id
	INNER JOIN VoCaB_ScHeMa.CONCEPT c on ca.descendant_concept_id = c.concept_id
	

        WHERE 
		c.domain_id = 'Drug'

        AND  c.standard_concept = 'S'

		AND c.concept_id NOT IN (967823, 19135374, 19044522, 19011773,
								19095164,
								19070224,
								19137312,
								19044522,
								19077884,
								1517824,
								19005046) -- NaCl, zinc_sufate, lactate ringers is incorrectly labeled as a corticosteroid

        AND GETDATE() BETWEEN c.valid_start_date AND c.valid_end_date

)
) g


ORDER BY variable_name ASC

GO

INSERT INTO LoOkUp_ScHeMa.IC3_Variable_Lookup_Table
SELECT
	[concept_id]
      ,[concept_code]
      ,[concept_name]
      ,[vocabulary_id]
      ,domain_id [domain]
      ,CASE WHEN concept_id = 2000000027 THEN 'surgery'
			WHEN concept_id = 2000000030	THEN 'sched_post_op_location'
			WHEN concept_id = 2000000031	THEN 'sched_room'
			WHEN concept_id = 2000000032	THEN 'sched_trauma_room_y_n' END [variable_name]
FROM
	VoCaB_ScHeMa.CONCEPT
WHERE
	vocabulary_id = 'IC3'
	AND
	concept_id NOT IN (SELECT TOP (1000) [concept_id]
  FROM LoOkUp_ScHeMa.[IC3_Variable_Lookup_Table]
  WHERE
  vocabulary_id = 'IC3')
  AND
  concept_id IN (2000000027,  2000000030, 2000000031, 2000000032)

  GO
  INSERT INTO LoOkUp_ScHeMa.IC3_Variable_Lookup_Table
SELECT
	[concept_id]
      ,[concept_code]
      ,[concept_name]
      ,[vocabulary_id]
      ,domain_id [domain]
      ,'sched_primary_procedure' [variable_name]
FROM
	VoCaB_ScHeMa.CONCEPT
WHERE
	concept_id = 2000000042
GO

INSERT INTO LoOkUp_ScHeMa.IC3_Variable_Lookup_Table
SELECT
	[concept_id]
      ,[concept_code]
      ,[concept_name]
      ,[vocabulary_id]
      ,domain_id [domain]
      ,'sched_start_datetime' [variable_name]
FROM
	VoCaB_ScHeMa.CONCEPT
WHERE
	concept_id = 4162211
GO

INSERT INTO LoOkUp_ScHeMa.IC3_Variable_Lookup_Table
SELECT
	[concept_id]
      ,[concept_code]
      ,[concept_name]
      ,[vocabulary_id]
      ,domain_id [domain]
      ,'sched_surgeon_deiden_id' [variable_name]
FROM
	VoCaB_ScHeMa.CONCEPT
WHERE
	concept_id = 2000000043

GO

INSERT INTO LoOkUp_ScHeMa.IC3_Variable_Lookup_Table
SELECT DISTINCT
	[concept_id]
      ,[concept_code]
      ,[concept_name]
      ,[vocabulary_id]
      ,[domain]
	  , variable_name
FROM
(SELECT
	c.concept_id,
	c.concept_name,
	c.vocabulary_id,
	c.concept_code,
	ca.ancestor_concept_id,
	c.domain_id [domain],
	CASE WHEN ca.ancestor_concept_id IN (4023217, 8717, 38004515) THEN 'ward'
		 WHEN ca.ancestor_concept_id IN (4021520, 9203) THEN 'emergency_department'
		 WHEN ca.ancestor_concept_id = 4331156 THEN 'procedure_suite'
		 WHEN ca.ancestor_concept_id = 4021813 THEN 'operating_room'
		 WHEN ca.ancestor_concept_id = 4134563 THEN 'post_anesthesia_care_unit'
		 WHEN ca.ancestor_concept_id = 4305525 THEN 'intermediate_care/stepdown_unit'
		 WHEN ca.ancestor_concept_id = 4134848 THEN 'or_holding_unit'
		 WHEN ca.ancestor_concept_id = 4139502 THEN 'home'
		 WHEN ca.ancestor_concept_id = 4021524 THEN 'morgue'
	END	as variable_name
FROM 
	VoCaB_ScHeMa.CONCEPT c
	INNER JOIN VoCaB_ScHeMa.CONCEPT_ANCESTOR ca on ca.descendant_concept_id = c.concept_id
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
UNION
SELECT
	c.concept_id,
	c.concept_name,
	c.vocabulary_id,
	c.concept_code,
	NULL [ancestor_concept_id],
	c.domain_id [domain],
	'operating_room' [variable_name]
FROM 
	VoCaB_ScHeMa.CONCEPT c
WHERE
	c.concept_id = 2000000027) t

 GO

 
INSERT INTO LoOkUp_ScHeMa.IC3_Variable_Lookup_Table
SELECT DISTINCT
	xy.concept_id,
	xy.concept_code,
	xy.concept_name,
	xy.vocabulary_id,
	'Labs' [domain],
	LOWER(xy.lab_type) [variable_name]
FROM
(
(SELECT
        c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'serum_asparate' [lab_type]
FROM
    VoCaB_ScHeMa.CONCEPT_ANCESTOR ca
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on ca.descendant_concept_id = c.concept_id
WHERE
    ca.ancestor_concept_id IN (3013721, 37059000, 4263457) -- serum_asparate
	AND
	c.concept_name LIKE '%serum%'
	AND
	c.standard_concept = 'S'
	AND
	c.concept_name NOT LIKE '%presence%')

UNION

(SELECT
        c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'BUN' [lab_type]
FROM
    VoCaB_ScHeMa.CONCEPT_ANCESTOR ca
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on ca.descendant_concept_id = c.concept_id
WHERE
    ca.ancestor_concept_id IN (4017361, 4074649) -- BUN
	AND
	c.concept_name NOT LIKE '%Creatinine%'
	AND
	c.standard_concept = 'S'
	AND
	(c.concept_name LIKE '%blood%' OR c.concept_name LIKE '%serum%' OR c.concept_name LIKE '%plasma%' OR c.concept_name LIKE '%venous%' OR c.concept_name LIKE '%capillary%')
	)

UNION

(SELECT
        c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'CARBOXYHEM' [lab_type]
FROM
    VoCaB_ScHeMa.CONCEPT_ANCESTOR ca
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on ca.descendant_concept_id = c.concept_id
WHERE
    ca.ancestor_concept_id IN (4017625) -- CARBOXYHEM quantitative
	AND
	c.concept_name NOT LIKE '% cord %'
	AND
	c.concept_name NOT LIKE '% per day%'
	)

UNION

(SELECT
        c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'serum_CO2' [lab_type]
FROM
    VoCaB_ScHeMa.CONCEPT_ANCESTOR ca
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on ca.descendant_concept_id = c.concept_id
WHERE
    ca.ancestor_concept_id IN (3010140, 3015632, 4099545, 40652760, 4135963) -- serum2CO
	AND
	c.standard_concept = 'S'
	AND
	(c.concept_name LIKE '%blood%' OR c.concept_name LIKE '%serum%' OR c.concept_name LIKE '%plasma%' OR c.concept_name LIKE '%venous%' OR c.concept_name LIKE '%capillary%')
	AND
	c.concept_name NOT LIKE '%arterial%'
	AND
	c.concept_name NOT LIKE '%urine%')

UNION 

(SELECT
        c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'ESR' [lab_type]
FROM
    VoCaB_ScHeMa.CONCEPT_ANCESTOR ca
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on ca.descendant_concept_id = c.concept_id
WHERE
    ca.ancestor_concept_id IN (4212065, 3015183, 37393853) -- ESR
	AND
    c.concept_name NOT LIKE '%2H%'
    AND 
    c.concept_name NOT LIKE '%minute%' 
    ) 

UNION

(SELECT
        c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'Glucose_UR' [lab_type]
FROM
    VoCaB_ScHeMa.CONCEPT_ANCESTOR ca
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on ca.descendant_concept_id = c.concept_id
WHERE
    ca.ancestor_concept_id IN (4149883)
	AND
	c.concept_name NOT LIKE '%challenge%'
	AND
	c.concept_name NOT LIKE '%presence%'
	AND
	c.concept_name NOT LIKE '% post dose %'
	AND
	c.concept_name NOT LIKE '%hour%'
	AND
	c.concept_name NOT LIKE '%minute%'
	AND
	c.concept_name NOT LIKE '%qualitative%'
	AND
	c.concept_name NOT LIKE '%screening%'
	AND
	c.concept_name NOT LIKE '%self%'
	AND
	c.concept_name NOT LIKE '%ward%')

UNION

(SELECT
        c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'SERUM_HCO3' [lab_type]
FROM
    VoCaB_ScHeMa.CONCEPT_ANCESTOR ca
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on ca.descendant_concept_id = c.concept_id
WHERE
    ca.ancestor_concept_id IN (37057819, 4227915, 3006576) -- SERUM_HCO3
    AND
    (c.concept_name LIKE '%blood' OR c.concept_name LIKE '%serum%' OR c.concept_name LIKE '%plasma%')
    AND
    c.concept_name NOT LIKE '%base%' 
    AND 
    c.concept_name NOT LIKE '%cord%'
    AND 
    c.concept_name NOT LIKE '%vasopressin') 

UNION

(SELECT DISTINCT
        c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'hgb' [lab_type]
FROM
    VoCaB_ScHeMa.CONCEPT_ANCESTOR ca
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on ca.descendant_concept_id = c.concept_id
WHERE
    ca.ancestor_concept_id IN (3006239, 37029074, 37054839, 1002591, 40654905, 37072252, 40654905) -- hgb
	AND
	c.standard_concept = 'S'
	AND
	c.concept_name NOT LIKE '%Presence%'
)

UNION

(SELECT
        c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'hgb_ur' [lab_type]
FROM
    VoCaB_ScHeMa.CONCEPT_ANCESTOR ca
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on ca.descendant_concept_id = c.concept_id
WHERE
    ca.ancestor_concept_id IN (4016242, 2212401, 37056155, 3002719) -- hgb_ur
	AND
	c.concept_name NOT LIKE '%presence%'
)

UNION

(SELECT DISTINCT
        c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'serum_inr' [lab_type]
FROM
    VoCaB_ScHeMa.CONCEPT_ANCESTOR ca
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on ca.descendant_concept_id = c.concept_id
WHERE
    ca.ancestor_concept_id IN (3032080, 4131379, 4261078, 37042344, 37074906, 37061141) -- serum_inr
	AND
	c.standard_concept = 'S'
)

UNION 


(SELECT DISTINCT
        c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'lymphocytes' [lab_type]
FROM
    VoCaB_ScHeMa.CONCEPT_ANCESTOR ca
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on ca.descendant_concept_id = c.concept_id
WHERE
    ca.ancestor_concept_id IN (3004327, 3003215, 40487382, 37208689, 4254663) -- lymphocytes
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

UNION -- got to here, need to double check labs below this comment


(SELECT
        c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'lymphocytes_per' [lab_type]
FROM
    VoCaB_ScHeMa.CONCEPT_ANCESTOR ca
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on ca.descendant_concept_id = c.concept_id
WHERE
    ca.ancestor_concept_id IN (3037511, 3038058, 37208690, 4156652) -- lymphocytes_per
)

UNION 


(SELECT DISTINCT
        c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'mch' [lab_type]
FROM
    VoCaB_ScHeMa.CONCEPT_ANCESTOR ca
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on ca.descendant_concept_id = c.concept_id
WHERE
    ca.ancestor_concept_id IN (3012030, 37398674, 37068065) -- mch
	AND
	c.standard_concept = 'S'
)

UNION

(SELECT
        c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'mchc' [lab_type]
FROM
    VoCaB_ScHeMa.CONCEPT_ANCESTOR ca
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on ca.descendant_concept_id = c.concept_id
WHERE
    ca.ancestor_concept_id IN (3009744, 37045413, 40654759, 37393850) -- mchc
)

UNION 

(SELECT
        c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'mcv' [lab_type]
FROM
    VoCaB_ScHeMa.CONCEPT_ANCESTOR ca
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on ca.descendant_concept_id = c.concept_id
WHERE
    ca.ancestor_concept_id IN (3023599, 37065843, 37393851) -- mcv
)

UNION


(SELECT DISTINCT
        c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'methhem' [lab_type]
FROM
    VoCaB_ScHeMa.CONCEPT_ANCESTOR ca
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on ca.descendant_concept_id = c.concept_id
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
)

UNION 


(SELECT
        c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'microalbumin_ur' [lab_type]
FROM
    VoCaB_ScHeMa.CONCEPT_ANCESTOR ca
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on ca.descendant_concept_id = c.concept_id
WHERE
    ca.ancestor_concept_id IN (4263307, 3000034) -- microalbumin_ur
    AND 
    c.concept_name LIKE '%uri%'
    AND 
    c.concept_name NOT LIKE '%hour%'
)

UNION


(SELECT
        c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'mpv' [lab_type]
FROM
    VoCaB_ScHeMa.CONCEPT_ANCESTOR ca
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on ca.descendant_concept_id = c.concept_id
WHERE
    ca.ancestor_concept_id IN (3043111, 4192368, 40302423, 40452035, 40267653, 4192368) -- mpv
)

UNION 


(SELECT
        c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'neutrophil' [lab_type]
FROM
    VoCaB_ScHeMa.CONCEPT_ANCESTOR ca
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on ca.descendant_concept_id = c.concept_id
WHERE
    ca.ancestor_concept_id IN (3013650, 3017501, 37208699) -- neutrophil
)

UNION


(SELECT
        c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'neutrophil_per_band' [lab_type]
FROM
    VoCaB_ScHeMa.CONCEPT_ANCESTOR ca
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on ca.descendant_concept_id = c.concept_id
WHERE
    ca.ancestor_concept_id IN (3007591, 4100147, 3559251, 3559252) -- neutrophil_per_band
)

UNION

(SELECT
        c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'O2SATA' [lab_type]
FROM
    VoCaB_ScHeMa.CONCEPT_ANCESTOR ca
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on ca.descendant_concept_id = c.concept_id
WHERE
    ca.ancestor_concept_id IN (3013502, 3016502, 3558251, 4013965) -- O2_saturation_arterial
	AND
	c.concept_name NOT LIKE '%oximetry%'
	)

UNION


(SELECT
        c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'PCO2A' [lab_type]
FROM
    VoCaB_ScHeMa.CONCEPT_ANCESTOR ca
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on ca.descendant_concept_id = c.concept_id
WHERE
    ca.ancestor_concept_id IN (37037933, 3027946, 40308327, 4042749, 4042749) -- serum_co2_pp_arterial
	AND
	c.concept_name NOT LIKE '%moles%'
	)

UNION


(SELECT
        c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'PO2A' [lab_type]
FROM
    VoCaB_ScHeMa.CONCEPT_ANCESTOR ca
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on ca.descendant_concept_id = c.concept_id
WHERE
    ca.ancestor_concept_id IN (4094581, 3027801, 4103460, 40308324) -- serum_o2_pp_arterial
	AND
	c.vocabulary_id NOT LIKE '%CPT%'
	AND 
	c.concept_name NOT LIKE '%venous%'
	)

UNION

(SELECT
        c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'RBC' [lab_type]
FROM
    VoCaB_ScHeMa.CONCEPT_ANCESTOR ca
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on ca.descendant_concept_id = c.concept_id
WHERE
    ca.ancestor_concept_id IN (3026361, 37393849) -- RBC
	AND
	(c.concept_name LIKE '%red%' OR c.concept_name LIKE '%erythr%')
	AND 
	c.concept_name NOT LIKE '%hypochromic%'
)

UNION

(SELECT
        c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'RBC_UR' [lab_type]
FROM
    VoCaB_ScHeMa.CONCEPT_ANCESTOR ca
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on ca.descendant_concept_id = c.concept_id
WHERE
    ca.ancestor_concept_id IN (4016235, 3009105) -- RBC_UR
)

UNION 


(SELECT
        c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'RDW' [lab_type]
FROM
    VoCaB_ScHeMa.CONCEPT_ANCESTOR ca
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on ca.descendant_concept_id = c.concept_id
WHERE
    ca.ancestor_concept_id IN (40451480, 4281085, 3019897, 37397924) -- RDW
)

UNION

(SELECT
        c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'SODIUM' [lab_type]
FROM
    VoCaB_ScHeMa.CONCEPT_ANCESTOR ca
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on ca.descendant_concept_id = c.concept_id
WHERE
    ca.ancestor_concept_id IN (3000285, 3019550, 3043706, 37393103, 37392172) -- serum_na
)

UNION

(SELECT
        c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'Troponin_I' [lab_type]
FROM
    VoCaB_ScHeMa.CONCEPT_ANCESTOR ca
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on ca.descendant_concept_id = c.concept_id
WHERE
    ca.ancestor_concept_id IN (3033745, 3032971, 4007805, 4010039) -- troponin_I
)

UNION

(SELECT
        c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'Troponin_T' [lab_type]
FROM
    VoCaB_ScHeMa.CONCEPT_ANCESTOR ca
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on ca.descendant_concept_id = c.concept_id
WHERE
    ca.ancestor_concept_id IN (3019800, 40769783, 3048529, 3019572, 4005525, 4010038) -- troponin_T
)
	
UNION 


(SELECT
        c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'UACR' [lab_type]
FROM
    VoCaB_ScHeMa.CONCEPT_ANCESTOR ca
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on ca.descendant_concept_id = c.concept_id
WHERE
    ca.ancestor_concept_id IN (3034485, 37398781) -- urine albumin/Cr ratio
)

UNION

(SELECT
        c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'UAP_cat' [lab_type]
FROM
    VoCaB_ScHeMa.CONCEPT_ANCESTOR ca
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on ca.descendant_concept_id = c.concept_id
WHERE
    ca.ancestor_concept_id IN (3014051, 40760845) -- presence of protien, only LOINC value
	)
	
UNION

(SELECT
        c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'UMACR' [lab_type]
FROM
    VoCaB_ScHeMa.CONCEPT_ANCESTOR ca
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on ca.descendant_concept_id = c.concept_id
WHERE
    ca.ancestor_concept_id IN (37398777, 3001802) -- microalbumin/cr ratio
	)

UNION

(SELECT
        c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'UNCR' [lab_type]
FROM
    VoCaB_ScHeMa.CONCEPT_ANCESTOR ca
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on ca.descendant_concept_id = c.concept_id
WHERE
    ca.ancestor_concept_id IN (3018311, 4112223) -- urea nitrogen cr mass ration in serum
	)

UNION

(SELECT
        c.concept_id, c.concept_name, c.concept_code, c.vocabulary_id, 'WBC' [lab_type]
FROM
    VoCaB_ScHeMa.CONCEPT_ANCESTOR ca
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on ca.descendant_concept_id = c.concept_id
WHERE
    ca.ancestor_concept_id IN (3000905, 3010813, 4212899) -- wbc
	)
) xy;

GO

INSERT INTO LoOkUp_ScHeMa.IC3_Variable_Lookup_Table
SELECT
	[concept_id]
      ,[concept_code]
      ,[concept_name]
      ,[vocabulary_id]
      ,domain_id [domain]
      ,'asa_score' [variable_name]
FROM
	VoCaB_ScHeMa.CONCEPT
WHERE
	concept_id = 4199571
GO

INSERT INTO LoOkUp_ScHeMa.IC3_Variable_Lookup_Table
SELECT
	[concept_id]
      ,[concept_code]
      ,[concept_name]
      ,[vocabulary_id]
      ,domain_id [domain]
      ,'gcs_eye_score' [variable_name]
FROM
	VoCaB_ScHeMa.CONCEPT
WHERE
	concept_id IN (4084277, 3016335)
GO


INSERT INTO LoOkUp_ScHeMa.IC3_Variable_Lookup_Table
SELECT DISTINCT *
FROM (SELECT
        c.concept_id,
		c.concept_code,
		c.concept_name,
		c.vocabulary_id,
		c.domain_id [domain],
		'rbc_transfusion' [variable_name]
FROM
    VoCaB_ScHeMa.CONCEPT_ANCESTOR ca
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on ca.descendant_concept_id = c.concept_id
WHERE
    ca.ancestor_concept_id IN (4054726) -- Red blood cells, blood product
UNION
SELECT
        c.concept_id,
		c.concept_code,
		c.concept_name,
		c.vocabulary_id,
		c.domain_id [domain],
		'rbc_transfusion' [variable_name]
FROM
    VoCaB_ScHeMa.CONCEPT_ANCESTOR ca
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on ca.descendant_concept_id = c.concept_id
WHERE
    ca.ancestor_concept_id IN (45875997) -- Blood Product
	AND
	c.concept_name LIKE '%given%'
	AND
	(c.concept_name LIKE '%red blood%' OR c.concept_name LIKE '%rbc%' OR c.concept_name LIKE '%erythrocyte%')) yu
GO

INSERT INTO LoOkUp_ScHeMa.IC3_Variable_Lookup_Table
SELECT DISTINCT
        c.concept_id,
		c.concept_code,
		c.concept_name,
		c.vocabulary_id,
		c.domain_id [domain],
		'cpr' [variable_name]
FROM
    VoCaB_ScHeMa.CONCEPT_ANCESTOR ca
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on ca.descendant_concept_id = c.concept_id
WHERE
    ca.ancestor_concept_id IN (4320484) -- Cardiac resuscitation
							   
GO

INSERT INTO LoOkUp_ScHeMa.IC3_Variable_Lookup_Table
SELECT
        c.concept_id,
		c.concept_code,
		c.concept_name,
		c.vocabulary_id,
		c.domain_id [domain],
		'primary_procedure' [variable_name]
FROM
    VoCaB_ScHeMa.CONCEPT_ANCESTOR ca
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on ca.descendant_concept_id = c.concept_id
WHERE
    ca.ancestor_concept_id IN (4161019) -- Primary Procedure
GO

INSERT INTO LoOkUp_ScHeMa.IC3_Variable_Lookup_Table
SELECT DISTINCT
        c.concept_id,
		c.concept_code,
		c.concept_name,
		c.vocabulary_id,
		c.domain_id [domain],
		'intubation' [variable_name]
FROM
    VoCaB_ScHeMa.CONCEPT_ANCESTOR ca
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on ca.descendant_concept_id = c.concept_id
WHERE
    ca.ancestor_concept_id IN (4013354) -- Insertion of endotracheal tube
	AND
	(c.concept_name NOT LIKE '%nas%'
	AND
	c.concept_name NOT LIKE '%glos%'
	AND
	c.concept_name NOT LIKE '%glot%')

GO

INSERT INTO LoOkUp_ScHeMa.IC3_Variable_Lookup_Table
SELECT DISTINCT
        c.concept_id,
		c.concept_code,
		c.concept_name,
		c.vocabulary_id,
		c.domain_id [domain],
		'arterial_catheter' [variable_name]
FROM
    VoCaB_ScHeMa.CONCEPT_ANCESTOR ca
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on ca.descendant_concept_id = c.concept_id
WHERE
    ca.ancestor_concept_id IN (4213288) -- Insertion of catheter into artery
GO

INSERT INTO LoOkUp_ScHeMa.IC3_Variable_Lookup_Table
SELECT DISTINCT
        c.concept_id,
		c.concept_code,
		c.concept_name,
		c.vocabulary_id,
		c.domain_id [domain],
		'central_venous_catheter' [variable_name]
FROM
    VoCaB_ScHeMa.CONCEPT_ANCESTOR ca
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on ca.descendant_concept_id = c.concept_id
WHERE
    ca.ancestor_concept_id IN (4052413) -- Central venous cannula insertion
GO

INSERT INTO LoOkUp_ScHeMa.IC3_Variable_Lookup_Table
SELECT DISTINCT
        c.concept_id,
		c.concept_code,
		c.concept_name,
		c.vocabulary_id,
		c.domain_id [domain],
		'pulmonary_artery_catheter' [variable_name]
FROM
    VoCaB_ScHeMa.CONCEPT_ANCESTOR ca
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on ca.descendant_concept_id = c.concept_id
WHERE
    ca.ancestor_concept_id IN (4305794, -- Introduction of catheter into pulmonary artery
								2313886) -- Insertion and placement of flow directed catheter (eg, Swan-Ganz) for monitoring purposes
GO

INSERT INTO LoOkUp_ScHeMa.IC3_Variable_Lookup_Table
SELECT DISTINCT
        c.concept_id,
		c.concept_code,
		c.concept_name,
		c.vocabulary_id,
		c.domain_id [domain],
		'bronchoscopy' [variable_name]
FROM
    VoCaB_ScHeMa.CONCEPT_ANCESTOR ca
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on ca.descendant_concept_id = c.concept_id
WHERE
    ca.ancestor_concept_id IN (4032404) -- Bronchoscopy
GO


INSERT INTO LoOkUp_ScHeMa.IC3_Variable_Lookup_Table
SELECT DISTINCT
        c.concept_id,
		c.concept_code,
		c.concept_name,
		c.vocabulary_id,
		c.domain_id [domain],
		'cardioversion_electric' [variable_name]
FROM
    VoCaB_ScHeMa.CONCEPT_ANCESTOR ca
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on ca.descendant_concept_id = c.concept_id
WHERE
    ca.ancestor_concept_id IN (4032404) -- Direct current cardioversion
GO

INSERT INTO LoOkUp_ScHeMa.IC3_Variable_Lookup_Table
SELECT DISTINCT
        c.concept_id,
		c.concept_code,
		c.concept_name,
		c.vocabulary_id,
		c.domain_id [domain],
		'chest_tube' [variable_name]
FROM
    VoCaB_ScHeMa.CONCEPT_ANCESTOR ca
    INNER JOIN VoCaB_ScHeMa.CONCEPT c on ca.descendant_concept_id = c.concept_id
WHERE
    ca.ancestor_concept_id IN (4141919) -- Insertion of pleural tube drain
	AND
	c.concept_id <> 2108503
GO

INSERT INTO LoOkUp_ScHeMa.IC3_Variable_Lookup_Table
SELECT DISTINCT
        c.concept_id,
		c.concept_code,
		c.concept_name,
		c.vocabulary_id,
		c.domain_id [domain],
		'sofa_score' [variable_name]
FROM
    VoCaB_ScHeMa.CONCEPT c
WHERE
    concept_id IN (36684937, -- Sequential Organ Failure Assessment score
    			   37394663, -- SOFA (Sequential Organ Failure Assessment) score
    			   1616852, -- SOFA Total Score
    			   1616328) -- Sequential Organ Failure Assessment SOFA
GO

INSERT INTO LoOkUp_ScHeMa.IC3_Variable_Lookup_Table
SELECT DISTINCT
        c.concept_id,
		c.concept_code,
		c.concept_name,
		c.vocabulary_id,
		c.domain_id [domain],
		'resp_sofa_subscore' [variable_name]
FROM
    VoCaB_ScHeMa.CONCEPT c
WHERE
    concept_id IN (1616907)
GO

INSERT INTO LoOkUp_ScHeMa.IC3_Variable_Lookup_Table
SELECT DISTINCT
        c.concept_id,
		c.concept_code,
		c.concept_name,
		c.vocabulary_id,
		c.domain_id [domain],
		'cns_sofa_subscore' [variable_name]
FROM
    VoCaB_ScHeMa.CONCEPT c
WHERE
    concept_id IN (1616439)
GO

INSERT INTO LoOkUp_ScHeMa.IC3_Variable_Lookup_Table
SELECT DISTINCT
        c.concept_id,
		c.concept_code,
		c.concept_name,
		c.vocabulary_id,
		c.domain_id [domain],
		'cardio_sofa_subscore' [variable_name]
FROM
    VoCaB_ScHeMa.CONCEPT c
WHERE
    concept_id IN (1617534)
GO

INSERT INTO LoOkUp_ScHeMa.IC3_Variable_Lookup_Table
SELECT DISTINCT
        c.concept_id,
		c.concept_code,
		c.concept_name,
		c.vocabulary_id,
		c.domain_id [domain],
		'liver_sofa_subscore' [variable_name]
FROM
    VoCaB_ScHeMa.CONCEPT c
WHERE
    concept_id IN (1617043)
GO

INSERT INTO LoOkUp_ScHeMa.IC3_Variable_Lookup_Table
SELECT DISTINCT
        c.concept_id,
		c.concept_code,
		c.concept_name,
		c.vocabulary_id,
		c.domain_id [domain],
		'coag_sofa_subscore' [variable_name]
FROM
    VoCaB_ScHeMa.CONCEPT c
WHERE
    concept_id IN (1616896)
GO

INSERT INTO LoOkUp_ScHeMa.IC3_Variable_Lookup_Table
SELECT DISTINCT
        c.concept_id,
		c.concept_code,
		c.concept_name,
		c.vocabulary_id,
		c.domain_id [domain],
		'renal_sofa_subscore' [variable_name]
FROM
    VoCaB_ScHeMa.CONCEPT c
WHERE
    concept_id IN (1616355)
GO

INSERT INTO LoOkUp_ScHeMa.IC3_Variable_Lookup_Table
SELECT DISTINCT
        c.concept_id,
		c.concept_code,
		c.concept_name,
		c.vocabulary_id,
		c.domain_id [domain],
		'seen_in_ed_yn' [variable_name]
FROM
    VoCaB_ScHeMa.CONCEPT c
WHERE
    concept_id IN (262)
GO

INSERT INTO LoOkUp_ScHeMa.IC3_Variable_Lookup_Table
SELECT DISTINCT
        c.concept_id,
		c.concept_code,
		c.concept_name,
		c.vocabulary_id,
		c.domain_id [domain],
		'gcs_score' [variable_name]
FROM
    VoCaB_ScHeMa.CONCEPT c
WHERE
    concept_id IN (4093836, 4296538, 3032652, 3007194)
GO

INSERT INTO LoOkUp_ScHeMa.IC3_Variable_Lookup_Table
SELECT DISTINCT
        c.concept_id,
		c.concept_code,
		c.concept_name,
		c.vocabulary_id,
		c.domain_id [domain],
		'gcs_motor_score' [variable_name]
FROM
    VoCaB_ScHeMa.CONCEPT c
WHERE
    concept_id IN (3016335, 4083352)
GO

INSERT INTO LoOkUp_ScHeMa.IC3_Variable_Lookup_Table
SELECT DISTINCT
        c.concept_id,
		c.concept_code,
		c.concept_name,
		c.vocabulary_id,
		c.domain_id [domain],
		'gcs_verbal_score' [variable_name]
FROM
    VoCaB_ScHeMa.CONCEPT c
WHERE
    concept_id IN (3009094, 4084912)
GO

INSERT INTO LoOkUp_ScHeMa.IC3_Variable_Lookup_Table
SELECT DISTINCT
        c.concept_id,
		c.concept_code,
		c.concept_name,
		c.vocabulary_id,
		c.domain_id [domain],
		'pao2' [variable_name]
FROM
    VoCaB_ScHeMa.CONCEPT c on ca.descendant_concept_id = c.concept_id
WHERE
    ca.ancestor_concept_id IN (37392673, -- Arterial O2 level
    						   3027801, -- Oxygen [Partial pressure] in Arterial blood
    						   3022803 -- Oxygen [Partial pressure] adjusted to patient's actual temperature in Arterial blood
    						   )

GO

CREATE NONCLUSTERED INDEX [var_name_IC3_Variable_Lookup_Table] ON LoOkUp_ScHeMa.[IC3_Variable_Lookup_Table]
(
	[variable_name] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO

CREATE NONCLUSTERED INDEX [id_domain_IC3_Variable_Lookup_Table] ON LoOkUp_ScHeMa.[IC3_Variable_Lookup_Table]
(
	[concept_id] ASC,
	[domain] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO

CREATE NONCLUSTERED INDEX [concept_id_IC3_Variable_Lookup_Table] ON LoOkUp_ScHeMa.[IC3_Variable_Lookup_Table]
(
	[concept_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO

 DELETE FROM LoOkUp_ScHeMa.IC3_Variable_Lookup_Table
 WHERE concept_id IN (
 					  32037, -- Intensive Care
					  581379 -- Critical Care Facility
					  ) AND variable_name = 'ward';

 GO
