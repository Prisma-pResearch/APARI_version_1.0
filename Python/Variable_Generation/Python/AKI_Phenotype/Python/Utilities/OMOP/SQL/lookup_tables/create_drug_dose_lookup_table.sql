DROP TABLE IF EXISTS LoOkUp_ScHeMa.IC3_DRUG_LOOKUP_TABLE
GO
SELECT
	LOWER([Ingredient_Category]) [ingredient_category],
    Ingredient_concept_id [ingredient_concept_id],
    Ingredient_name [ingredient_name],
	[conversion_factor],
	[numerator_unit_concept_id],
	[numerator_unit],
	denominator_unit_concept_id,
	[denominator_unit],
    Drug_concept_id [drug_concept_id],
    Drug_name [drug_name],
    Drug_concept_code [drug_concept_code],
    Drug_concept_class [drug_concept_class],
	[route_concept_id],
	[route_name],
	[route_category]
INTO LoOkUp_ScHeMa.IC3_DRUG_LOOKUP_TABLE
FROM
(
 SELECT
 	'pressors_inotropes' [Ingredient_Category],
     A.concept_id Ingredient_concept_id,
     A.concept_Name Ingredient_name,
 	COALESCE(DS.amount_value, DS.numerator_value, 1) / COALESCE(DS.denominator_value, 1) [conversion_factor],
 	COALESCE(DS.amount_unit_concept_id, DS.numerator_unit_concept_id) [numerator_unit_concept_id],
 	NU.concept_name [numerator_unit],
 	DS.denominator_unit_concept_id,
 	DU.concept_name [denominator_unit],
     --A.concept_Code Ingredient_concept_code,
     --A.concept_Class_id Ingredient_concept_class,
     D.concept_id Drug_concept_id,
     D.concept_Name Drug_name,
     D.concept_Code Drug_concept_code,
     D.concept_Class_id Drug_concept_class,
 	DF.concept_id [route_concept_id],
 	DF.concept_name [route_name],
	l.variable_name [route_category]

 FROM
 	VoCaB_ScHeMa.concept_ancestor CA
 	INNER JOIN VoCaB_ScHeMa.concept A on CA.ancestor_concept_id = A.concept_id
 	INNER JOIN VoCaB_ScHeMa.concept D on CA.descendant_concept_id = D.concept_id
 	INNER JOIN VoCaB_ScHeMa.DRUG_STRENGTH DS on (DS.drug_concept_id = D.concept_id
 												 AND
 												 DS.ingredient_concept_id = A.concept_id)
 	LEFT JOIN VoCaB_ScHeMa.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = D.concept_id
 													   AND
 													   Cr.relationship_id LIKE '%has dose form')--= 'RxNorm has dose form')
 	LEFT JOIN VoCaB_ScHeMa.CONCEPT DF on DF.concept_id = CR.concept_id_2
 	INNER JOIN VoCaB_ScHeMa.CONCEPT NU on NU.concept_id = COALESCE(DS.amount_unit_concept_id, DS.numerator_unit_concept_id)
 	LEFT JOIN VoCaB_ScHeMa.CONCEPT DU on DU.concept_id = DS.denominator_unit_concept_id
	LEFT JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table l on (l.concept_id = DF.concept_id AND l.concept_class_id = 'Dose Form')
 WHERE
 	CA.ancestor_concept_id IN (SELECT
 									ing.concept_id
 								FROM
 									VoCaB_ScHeMa.CONCEPT atc
 									INNER JOIN VoCaB_ScHeMa.CONCEPT_ANCESTOR ca on ca.ancestor_concept_id = atc.concept_id
 									INNER JOIN VoCaB_ScHeMa.CONCEPT ing on ing.concept_id = ca.descendant_concept_id
 								WHERE

 								atc.vocabulary_id='ATC'
 								AND
 								atc.concept_code IN ('C01CA', --	Adrenergic and dopaminergic agents
 														 'H01BA', --	Vasopressin and analogues
 														 'C01CE02' --	milrinone; parenteral
 														 )  -- PRESSORS_INOTROPES
 														 AND
 								ing.concept_class_id = 'Ingredient'
 								AND
 								ing.standard_concept = 'S')
 	AND
 	D.concept_id NOT IN (SELECT descendant_concept_id FROM VoCaB_ScHeMa.CONCEPT_ANCESTOR WHERE ancestor_concept_id = 21604181) -- Anesthetics ATC Class
 UNION

 -- Vancomycin Query, Oral forms can be ignored since the pharmakinetics of the drug do not absorb through the enteral route. It is only used for GI infections, but it does not have the nephrotoxic properties since it does not circulate in the blood.
 SELECT
 	'vancomycin' [Ingredient_Category],
     A.concept_id Ingredient_concept_id,
     A.concept_Name Ingredient_name,
 	COALESCE(DS.amount_value, DS.numerator_value, 1) / COALESCE(DS.denominator_value, 1) [conversion_factor],
 	COALESCE(DS.amount_unit_concept_id, DS.numerator_unit_concept_id) [numerator_unit_concept_id],
 	NU.concept_name [numerator_unit],
 	DS.denominator_unit_concept_id,
 	DU.concept_name [denominator_unit],
     D.concept_id Drug_concept_id,
     D.concept_Name Drug_name,
     D.concept_Code Drug_concept_code,
     D.concept_Class_id Drug_concept_class,
 	DF.concept_id [route_concept_id],
 	DF.concept_name [route_name],
	l.variable_name [route_category]
 FROM
 	VoCaB_ScHeMa.concept_ancestor CA
 	INNER JOIN VoCaB_ScHeMa.concept A on CA.ancestor_concept_id = A.concept_id
 	INNER JOIN VoCaB_ScHeMa.concept D on CA.descendant_concept_id = D.concept_id
 	INNER JOIN VoCaB_ScHeMa.DRUG_STRENGTH DS on (DS.drug_concept_id = D.concept_id
 												 AND
 												 DS.ingredient_concept_id = A.concept_id)
 	LEFT JOIN VoCaB_ScHeMa.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = D.concept_id
 													   AND
 													   Cr.relationship_id LIKE '%has dose form')--= 'RxNorm has dose form')
 	LEFT JOIN VoCaB_ScHeMa.CONCEPT DF on DF.concept_id = CR.concept_id_2
 	INNER JOIN VoCaB_ScHeMa.CONCEPT NU on NU.concept_id = COALESCE(DS.amount_unit_concept_id, DS.numerator_unit_concept_id)
 	LEFT JOIN VoCaB_ScHeMa.CONCEPT DU on DU.concept_id = DS.denominator_unit_concept_id
 	LEFT JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table l on (l.concept_id = DF.concept_id AND l.concept_class_id = 'Dose Form')
 WHERE

     CA.ancestor_concept_id  IN (1707687) -- Vancomyacin
 UNION

 SELECT
 	'statins' [Ingredient_Category],
     A.concept_id Ingredient_concept_id,
     A.concept_Name Ingredient_name,
 	COALESCE(DS.amount_value, DS.numerator_value, 1) / COALESCE(DS.denominator_value, 1) [conversion_factor],
 	COALESCE(DS.amount_unit_concept_id, DS.numerator_unit_concept_id) [numerator_unit_concept_id],
 	NU.concept_name [numerator_unit],
 	DS.denominator_unit_concept_id,
 	DU.concept_name [denominator_unit],
     D.concept_id Drug_concept_id,
     D.concept_Name Drug_name,
     D.concept_Code Drug_concept_code,
     D.concept_Class_id Drug_concept_class,
 	DF.concept_id [route_concept_id],
 	DF.concept_name [route_name],
	l.variable_name [route_category]
 FROM
 	VoCaB_ScHeMa.concept_ancestor CA
 	INNER JOIN VoCaB_ScHeMa.concept A on CA.ancestor_concept_id = A.concept_id
 	INNER JOIN VoCaB_ScHeMa.concept D on CA.descendant_concept_id = D.concept_id
 	INNER JOIN VoCaB_ScHeMa.DRUG_STRENGTH DS on (DS.drug_concept_id = D.concept_id
 												 AND
 												 DS.ingredient_concept_id = A.concept_id)
 	LEFT JOIN VoCaB_ScHeMa.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = D.concept_id
 													   AND
 													   Cr.relationship_id LIKE '%has dose form')--= 'RxNorm has dose form')
 	LEFT JOIN VoCaB_ScHeMa.CONCEPT DF on DF.concept_id = CR.concept_id_2
 	INNER JOIN VoCaB_ScHeMa.CONCEPT NU on NU.concept_id = COALESCE(DS.amount_unit_concept_id, DS.numerator_unit_concept_id)
 	LEFT JOIN VoCaB_ScHeMa.CONCEPT DU on DU.concept_id = DS.denominator_unit_concept_id
 	LEFT JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table l on (l.concept_id = DF.concept_id AND l.concept_class_id = 'Dose Form')
 WHERE
 	CA.ancestor_concept_id IN (SELECT DISTINCT
 									ing.concept_id
 								FROM
 									VoCaB_ScHeMa.CONCEPT atc
 									INNER JOIN VoCaB_ScHeMa.CONCEPT_ANCESTOR ca on ca.ancestor_concept_id = atc.concept_id
 									INNER JOIN VoCaB_ScHeMa.CONCEPT ing on ing.concept_id = ca.descendant_concept_id
 								WHERE

 								atc.vocabulary_id='ATC'
 								AND
 								atc.concept_code IN ('C10AA') --	HMG CoA reductase inhibitors, aka STATINS
 								AND
 								ing.concept_class_id = 'Ingredient'
 								AND
 								ing.standard_concept = 'S')
 UNION

 SELECT
 	'nsaids' [Ingredient_Category],
     A.concept_id Ingredient_concept_id,
     A.concept_Name Ingredient_name,
 	COALESCE(DS.amount_value, DS.numerator_value, 1) / COALESCE(DS.denominator_value, 1) [conversion_factor],
 	COALESCE(DS.amount_unit_concept_id, DS.numerator_unit_concept_id) [numerator_unit_concept_id],
 	NU.concept_name [numerator_unit],
 	DS.denominator_unit_concept_id,
 	DU.concept_name [denominator_unit],
     D.concept_id Drug_concept_id,
     D.concept_Name Drug_name,
     D.concept_Code Drug_concept_code,
     D.concept_Class_id Drug_concept_class,
 	DF.concept_id [route_concept_id],
 	DF.concept_name [route_name],
	l.variable_name [route_category]
 FROM
 	VoCaB_ScHeMa.concept_ancestor CA
 	INNER JOIN VoCaB_ScHeMa.concept A on CA.ancestor_concept_id = A.concept_id
 	INNER JOIN VoCaB_ScHeMa.concept D on CA.descendant_concept_id = D.concept_id
 	INNER JOIN VoCaB_ScHeMa.DRUG_STRENGTH DS on (DS.drug_concept_id = D.concept_id
 												 AND
 												 DS.ingredient_concept_id = A.concept_id)
 	LEFT JOIN VoCaB_ScHeMa.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = D.concept_id
 													   AND
 													   Cr.relationship_id LIKE '%has dose form')--= 'RxNorm has dose form')
 	LEFT JOIN VoCaB_ScHeMa.CONCEPT DF on DF.concept_id = CR.concept_id_2
 	INNER JOIN VoCaB_ScHeMa.CONCEPT NU on NU.concept_id = COALESCE(DS.amount_unit_concept_id, DS.numerator_unit_concept_id)
 	LEFT JOIN VoCaB_ScHeMa.CONCEPT DU on DU.concept_id = DS.denominator_unit_concept_id
	LEFT JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table l on (l.concept_id = DF.concept_id AND l.concept_class_id = 'Dose Form')
 WHERE

     (
 		CA.ancestor_concept_id  IN (1118084, --     celecoxib
 									1124300, --     diclofenac
 									1177480, --     ibuprofen
 									1178663, --     indomethacin
 									1136980, --     ketorolac
 									1150345, --     meloxicam
 									1113648, --     nabumetone
 									1115008, --     naproxen
 									1146810) --     piroxicam
 		OR
 		CA.ancestor_concept_id IN (SELECT
 										--atc.concept_name,
 										--atc.concept_code,
 										--ing.*
 										DISTINCT
 										ing.concept_id
 									FROM
 										VoCaB_ScHeMa.CONCEPT atc
 										INNER JOIN VoCaB_ScHeMa.CONCEPT_ANCESTOR ca on ca.ancestor_concept_id = atc.concept_id
 										INNER JOIN VoCaB_ScHeMa.CONCEPT ing on ing.concept_id = ca.descendant_concept_id
 									WHERE

 									atc.vocabulary_id='ATC'
 									AND
 									atc.concept_code IN ('M01AB', -- Acetic acid derivatives and related substances
 														 'M01AC', -- Oxicams
 														 'M01AE', -- Propionic acid derivatives
 														 'M01AH') -- Coxibs
 									AND
 									ing.concept_class_id = 'Ingredient'
 									AND
 									ing.standard_concept = 'S'
 									AND
 									ing.concept_name NOT LIKE '%vitamin%'
 									AND
 									ing.concept_name NOT LIKE '%oil'
 									AND
 									ing.concept_name NOT IN ('magnesium', 'aluminum hydroxide', 'magnesium carbonate', 'thiamine')
 									AND
 									ing.concept_id NOT IN (SELECT descendant_concept_id FROM VoCaB_ScHeMa.CONCEPT_ANCESTOR WHERE ancestor_concept_id = 21605145) -- Antiinfectives
 									AND
 									ing.concept_id NOT IN (SELECT descendant_concept_id FROM VoCaB_ScHeMa.CONCEPT_ANCESTOR WHERE ancestor_concept_id = 35807331) -- Steriod
 									AND
 									ing.concept_id NOT IN (SELECT descendant_concept_id FROM VoCaB_ScHeMa.CONCEPT_ANCESTOR WHERE ancestor_concept_id = 21604254) -- OPIOID
 									)
 	)
 --DIURETICS
 UNION
 SELECT
 	'diuretics' [Ingredient_Category],
     A.concept_id Ingredient_concept_id,
     A.concept_Name Ingredient_name,
 	COALESCE(DS.amount_value, DS.numerator_value, 1) / COALESCE(DS.denominator_value, 1) [conversion_factor],
 	COALESCE(DS.amount_unit_concept_id, DS.numerator_unit_concept_id) [numerator_unit_concept_id],
 	NU.concept_name [numerator_unit],
 	DS.denominator_unit_concept_id,
 	DU.concept_name [denominator_unit],
     D.concept_id Drug_concept_id,
     D.concept_Name Drug_name,
     D.concept_Code Drug_concept_code,
     D.concept_Class_id Drug_concept_class,
 	DF.concept_id [route_concept_id],
 	DF.concept_name [route_name],
	l.variable_name [route_category]
 FROM
 	VoCaB_ScHeMa.concept_ancestor CA
 	INNER JOIN VoCaB_ScHeMa.concept A on CA.ancestor_concept_id = A.concept_id
 	INNER JOIN VoCaB_ScHeMa.concept D on CA.descendant_concept_id = D.concept_id
 	INNER JOIN VoCaB_ScHeMa.DRUG_STRENGTH DS on (DS.drug_concept_id = D.concept_id
 												 AND
 												 DS.ingredient_concept_id = A.concept_id)
 	LEFT JOIN VoCaB_ScHeMa.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = D.concept_id
 													   AND
 													   Cr.relationship_id LIKE '%has dose form')--= 'RxNorm has dose form')
 	LEFT JOIN VoCaB_ScHeMa.CONCEPT DF on DF.concept_id = CR.concept_id_2
 	INNER JOIN VoCaB_ScHeMa.CONCEPT NU on NU.concept_id = COALESCE(DS.amount_unit_concept_id, DS.numerator_unit_concept_id)
 	LEFT JOIN VoCaB_ScHeMa.CONCEPT DU on DU.concept_id = DS.denominator_unit_concept_id
	LEFT JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table l on (l.concept_id = DF.concept_id AND l.concept_class_id = 'Dose Form')
 WHERE
 	(
 		CA.ancestor_concept_id IN  (SELECT
 										--atc.concept_name,
 										ing.concept_id
 									FROM
 										VoCaB_ScHeMa.CONCEPT atc
 										INNER JOIN VoCaB_ScHeMa.CONCEPT_ANCESTOR ca on ca.ancestor_concept_id = atc.concept_id
 										INNER JOIN VoCaB_ScHeMa.CONCEPT ing on ing.concept_id = ca.descendant_concept_id
 									WHERE

 									atc.vocabulary_id='ATC'
 									AND
 									atc.concept_code = 'C03' -- Diuretics
 									AND
 									ing.concept_class_id = 'Ingredient'
 									AND
 									ing.standard_concept = 'S')
 		OR
 		CA.ancestor_concept_id  IN (929435, --     acetazolamide
 									991382, --     amiloride
 									932745, --     bumetanide
 									992590, --     chlorothiazide
 									1395058, --     chlorthalidone
 									987406, --     ethacrynate (previously ethacrynic acid, which is the precise ingredient form)
 									956874, --     furosemide
 									974166, --     hydrochlorothiazide
 									978555, --     indapamide
 									994058, --     mannitol
 									905273, --     methyclothiazide
 									907013, --     metolazone
 									970250, --     spironolactone
 									942350, --     torsemide
 									904542) --     triamterene
 	)
 	
 UNION
 SELECT
 	'bicarbonates' [Ingredient_Category],
     A.concept_id Ingredient_concept_id,
     A.concept_Name Ingredient_name,
 	COALESCE(DS.amount_value, DS.numerator_value, 1) / COALESCE(DS.denominator_value, 1) [conversion_factor],
 	COALESCE(DS.amount_unit_concept_id, DS.numerator_unit_concept_id) [numerator_unit_concept_id],
 	NU.concept_name [numerator_unit],
 	DS.denominator_unit_concept_id,
 	DU.concept_name [denominator_unit],
     D.concept_id Drug_concept_id,
     D.concept_Name Drug_name,
     D.concept_Code Drug_concept_code,
     D.concept_Class_id Drug_concept_class,
 	DF.concept_id [route_concept_id],
 	DF.concept_name [route_name],
	l.variable_name [route_category]
 FROM
 	VoCaB_ScHeMa.concept_ancestor CA
 	INNER JOIN VoCaB_ScHeMa.concept A on CA.ancestor_concept_id = A.concept_id
 	INNER JOIN VoCaB_ScHeMa.concept D on CA.descendant_concept_id = D.concept_id
 	INNER JOIN VoCaB_ScHeMa.DRUG_STRENGTH DS on (DS.drug_concept_id = D.concept_id
 												 AND
 												 DS.ingredient_concept_id = A.concept_id)
 	LEFT JOIN VoCaB_ScHeMa.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = D.concept_id
 													   AND
 													   Cr.relationship_id LIKE '%has dose form')--= 'RxNorm has dose form')
 	LEFT JOIN VoCaB_ScHeMa.CONCEPT DF on DF.concept_id = CR.concept_id_2
 	INNER JOIN VoCaB_ScHeMa.CONCEPT NU on NU.concept_id = COALESCE(DS.amount_unit_concept_id, DS.numerator_unit_concept_id)
 	LEFT JOIN VoCaB_ScHeMa.CONCEPT DU on DU.concept_id = DS.denominator_unit_concept_id
	LEFT JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table l on (l.concept_id = DF.concept_id AND l.concept_class_id = 'Dose Form')
 WHERE
     CA.ancestor_concept_id  IN (19077884, --     sodium acetate
 								939506) --     sodium bicarbonate
 UNION
 -- Beta Blockers, include Infusion and Enteric Routes
 SELECT
 	'beta_blockers' [Ingredient_Category],
     A.concept_id Ingredient_concept_id,
     A.concept_Name Ingredient_name,
 	COALESCE(DS.amount_value, DS.numerator_value, 1) / COALESCE(DS.denominator_value, 1) [conversion_factor],
 	COALESCE(DS.amount_unit_concept_id, DS.numerator_unit_concept_id) [numerator_unit_concept_id],
 	NU.concept_name [numerator_unit],
 	DS.denominator_unit_concept_id,
 	DU.concept_name [denominator_unit],
     D.concept_id Drug_concept_id,
     D.concept_Name Drug_name,
     D.concept_Code Drug_concept_code,
     D.concept_Class_id Drug_concept_class,
 	DF.concept_id [route_concept_id],
 	DF.concept_name [route_name],
	l.variable_name [route_category]
 FROM
 	VoCaB_ScHeMa.concept_ancestor CA
 	INNER JOIN VoCaB_ScHeMa.concept A on CA.ancestor_concept_id = A.concept_id
 	INNER JOIN VoCaB_ScHeMa.concept D on CA.descendant_concept_id = D.concept_id
 	INNER JOIN VoCaB_ScHeMa.DRUG_STRENGTH DS on (DS.drug_concept_id = D.concept_id
 												 AND
 												 DS.ingredient_concept_id = A.concept_id)
 	LEFT JOIN VoCaB_ScHeMa.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = D.concept_id
 													   AND
 													   Cr.relationship_id LIKE '%has dose form')--= 'RxNorm has dose form')
 	LEFT JOIN VoCaB_ScHeMa.CONCEPT DF on DF.concept_id = CR.concept_id_2
 	INNER JOIN VoCaB_ScHeMa.CONCEPT NU on NU.concept_id = COALESCE(DS.amount_unit_concept_id, DS.numerator_unit_concept_id)
 	LEFT JOIN VoCaB_ScHeMa.CONCEPT DU on DU.concept_id = DS.denominator_unit_concept_id
	LEFT JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table l on (l.concept_id = DF.concept_id AND l.concept_class_id = 'Dose Form')
 WHERE
 	CA.ancestor_concept_id IN  (SELECT
 									ing.concept_id
 								FROM
 									VoCaB_ScHeMa.CONCEPT atc
 									INNER JOIN VoCaB_ScHeMa.CONCEPT_ANCESTOR ca on ca.ancestor_concept_id = atc.concept_id
 									INNER JOIN VoCaB_ScHeMa.CONCEPT ing on ing.concept_id = ca.descendant_concept_id
 								WHERE

 								atc.vocabulary_id='ATC'
 								AND
 								atc.concept_code = 'C07' -- BETA_BLOCKERS
 								AND
 								ing.concept_class_id = 'Ingredient'
 								AND
 								ing.standard_concept = 'S')
 UNION
 SELECT
 	'asprin' [Ingredient_Category],
     A.concept_id Ingredient_concept_id,
     A.concept_Name Ingredient_name,
 	COALESCE(DS.amount_value, DS.numerator_value, 1) / COALESCE(DS.denominator_value, 1) [conversion_factor],
 	COALESCE(DS.amount_unit_concept_id, DS.numerator_unit_concept_id) [numerator_unit_concept_id],
 	NU.concept_name [numerator_unit],
 	DS.denominator_unit_concept_id,
 	DU.concept_name [denominator_unit],
     D.concept_id Drug_concept_id,
     D.concept_Name Drug_name,
     D.concept_Code Drug_concept_code,
     D.concept_Class_id Drug_concept_class,
 	DF.concept_id [route_concept_id],
 	DF.concept_name [route_name],
	l.variable_name [route_category]
 FROM
 	VoCaB_ScHeMa.concept_ancestor CA
 	INNER JOIN VoCaB_ScHeMa.concept A on CA.ancestor_concept_id = A.concept_id
 	INNER JOIN VoCaB_ScHeMa.concept D on CA.descendant_concept_id = D.concept_id
 	INNER JOIN VoCaB_ScHeMa.DRUG_STRENGTH DS on (DS.drug_concept_id = D.concept_id
 												 AND
 												 DS.ingredient_concept_id = A.concept_id)
 	LEFT JOIN VoCaB_ScHeMa.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = D.concept_id
 													   AND
 													   Cr.relationship_id LIKE '%has dose form')--= 'RxNorm has dose form')
 	LEFT JOIN VoCaB_ScHeMa.CONCEPT DF on DF.concept_id = CR.concept_id_2
 	INNER JOIN VoCaB_ScHeMa.CONCEPT NU on NU.concept_id = COALESCE(DS.amount_unit_concept_id, DS.numerator_unit_concept_id)
 	LEFT JOIN VoCaB_ScHeMa.CONCEPT DU on DU.concept_id = DS.denominator_unit_concept_id
	LEFT JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table l on (l.concept_id = DF.concept_id AND l.concept_class_id = 'Dose Form')
 WHERE
     CA.ancestor_concept_id  IN (1112807) --     aspirin
 UNION
 SELECT
 	'antiemetics' [Ingredient_Category],
     A.concept_id Ingredient_concept_id,
     A.concept_Name Ingredient_name,
 	COALESCE(DS.amount_value, DS.numerator_value, 1) / COALESCE(DS.denominator_value, 1) [conversion_factor],
 	COALESCE(DS.amount_unit_concept_id, DS.numerator_unit_concept_id) [numerator_unit_concept_id],
 	NU.concept_name [numerator_unit],
 	DS.denominator_unit_concept_id,
 	DU.concept_name [denominator_unit],
     D.concept_id Drug_concept_id,
     D.concept_Name Drug_name,
     D.concept_Code Drug_concept_code,
     D.concept_Class_id Drug_concept_class,
 	DF.concept_id [route_concept_id],
 	DF.concept_name [route_name],
	l.variable_name [route_category]
 FROM
 	VoCaB_ScHeMa.concept_ancestor CA
 	INNER JOIN VoCaB_ScHeMa.concept A on CA.ancestor_concept_id = A.concept_id
 	INNER JOIN VoCaB_ScHeMa.concept D on CA.descendant_concept_id = D.concept_id
 	INNER JOIN VoCaB_ScHeMa.DRUG_STRENGTH DS on (DS.drug_concept_id = D.concept_id
 												 AND
 												 DS.ingredient_concept_id = A.concept_id)
 	LEFT JOIN VoCaB_ScHeMa.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = D.concept_id
 													   AND
 													   Cr.relationship_id LIKE '%has dose form')--= 'RxNorm has dose form')
 	LEFT JOIN VoCaB_ScHeMa.CONCEPT DF on DF.concept_id = CR.concept_id_2
 	INNER JOIN VoCaB_ScHeMa.CONCEPT NU on NU.concept_id = COALESCE(DS.amount_unit_concept_id, DS.numerator_unit_concept_id)
 	LEFT JOIN VoCaB_ScHeMa.CONCEPT DU on DU.concept_id = DS.denominator_unit_concept_id
	LEFT JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table l on (l.concept_id = DF.concept_id AND l.concept_class_id = 'Dose Form')
 WHERE
     (
 		CA.ancestor_concept_id  IN (936748, --     aprepitant
 									1037005, --     dronabinol
 									35605594, --     fosaprepitant
 									1000772, --     granisetron
 									994341, --     meclizine
 									1000560, --     ondansetron
 									911354, --     palonosetron
 									965748, --     scopolamine
 									942799) --     trimethobenzamide
 	OR
 		CA.ancestor_concept_id IN (SELECT DISTINCT
 										ing.concept_id
 									FROM
 										VoCaB_ScHeMa.CONCEPT atc
 										INNER JOIN VoCaB_ScHeMa.CONCEPT_ANCESTOR ca on ca.ancestor_concept_id = atc.concept_id
 										INNER JOIN VoCaB_ScHeMa.CONCEPT ing on ing.concept_id = ca.descendant_concept_id
 									WHERE

 									atc.vocabulary_id='ATC'
 									AND
 									atc.concept_code = 'A04A' -- ANTIEMETICS
 									AND
 									ing.concept_class_id = 'Ingredient'
 									AND
 									ing.standard_concept = 'S')
 	)
 UNION
 SELECT
 	'AMINOGLYCOSIDES' [Ingredient_Category],
     A.concept_id Ingredient_concept_id,
     A.concept_Name Ingredient_name,
 	COALESCE(DS.amount_value, DS.numerator_value, 1) / COALESCE(DS.denominator_value, 1) [conversion_factor],
 	COALESCE(DS.amount_unit_concept_id, DS.numerator_unit_concept_id) [numerator_unit_concept_id],
 	NU.concept_name [numerator_unit],
 	DS.denominator_unit_concept_id,
 	DU.concept_name [denominator_unit],
     D.concept_id Drug_concept_id,
     D.concept_Name Drug_name,
     D.concept_Code Drug_concept_code,
     D.concept_Class_id Drug_concept_class,
 	DF.concept_id [route_concept_id],
 	DF.concept_name [route_name],
	l.variable_name [route_category]
 FROM
 	VoCaB_ScHeMa.concept_ancestor CA
 	INNER JOIN VoCaB_ScHeMa.concept A on CA.ancestor_concept_id = A.concept_id
 	INNER JOIN VoCaB_ScHeMa.concept D on CA.descendant_concept_id = D.concept_id
 	INNER JOIN VoCaB_ScHeMa.DRUG_STRENGTH DS on (DS.drug_concept_id = D.concept_id
 												 AND
 												 DS.ingredient_concept_id = A.concept_id)
 	LEFT JOIN VoCaB_ScHeMa.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = D.concept_id
 													   AND
 													   Cr.relationship_id LIKE '%has dose form')--= 'RxNorm has dose form')
 	LEFT JOIN VoCaB_ScHeMa.CONCEPT DF on DF.concept_id = CR.concept_id_2
 	INNER JOIN VoCaB_ScHeMa.CONCEPT NU on NU.concept_id = COALESCE(DS.amount_unit_concept_id, DS.numerator_unit_concept_id)
 	LEFT JOIN VoCaB_ScHeMa.CONCEPT DU on DU.concept_id = DS.denominator_unit_concept_id
	LEFT JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table l on (l.concept_id = DF.concept_id AND l.concept_class_id = 'Dose Form')
 WHERE

     CA.ancestor_concept_id  IN (SELECT DISTINCT
 									ing.concept_id
 								FROM
 									VoCaB_ScHeMa.CONCEPT atc
 									INNER JOIN VoCaB_ScHeMa.CONCEPT_ANCESTOR ca on ca.ancestor_concept_id = atc.concept_id
 									INNER JOIN VoCaB_ScHeMa.CONCEPT ing on ing.concept_id = ca.descendant_concept_id
 								WHERE

 								atc.vocabulary_id='ATC'
 								AND
 								atc.concept_code = 'J01G' --AMINOGLYCOSIDES (e.g. amikacin|gentamicin|neomycin|tobramycin)
 								AND
 								ing.concept_class_id = 'Ingredient'
 								AND
 								ing.standard_concept = 'S')
 UNION

 SELECT
 	'ACEIs_ARBs' [Ingredient_Category],
     A.concept_id Ingredient_concept_id,
     A.concept_Name Ingredient_name,
 	COALESCE(DS.amount_value, DS.numerator_value, 1) / COALESCE(DS.denominator_value, 1) [conversion_factor],
 	COALESCE(DS.amount_unit_concept_id, DS.numerator_unit_concept_id) [numerator_unit_concept_id],
 	NU.concept_name [numerator_unit],
 	DS.denominator_unit_concept_id,
 	DU.concept_name [denominator_unit],
     D.concept_id Drug_concept_id,
     D.concept_Name Drug_name,
     D.concept_Code Drug_concept_code,
     D.concept_Class_id Drug_concept_class,
 	DF.concept_id [route_concept_id],
 	DF.concept_name [route_name],
	l.variable_name [route_category]
 FROM
 	VoCaB_ScHeMa.concept_ancestor CA
 	INNER JOIN VoCaB_ScHeMa.concept A on CA.ancestor_concept_id = A.concept_id
 	INNER JOIN VoCaB_ScHeMa.concept D on CA.descendant_concept_id = D.concept_id
 	INNER JOIN VoCaB_ScHeMa.DRUG_STRENGTH DS on (DS.drug_concept_id = D.concept_id
 												 AND
 												 DS.ingredient_concept_id = A.concept_id)
 	LEFT JOIN VoCaB_ScHeMa.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = D.concept_id
 													   AND
 													   Cr.relationship_id LIKE '%has dose form')--= 'RxNorm has dose form')
 	LEFT JOIN VoCaB_ScHeMa.CONCEPT DF on DF.concept_id = CR.concept_id_2
 	INNER JOIN VoCaB_ScHeMa.CONCEPT NU on NU.concept_id = COALESCE(DS.amount_unit_concept_id, DS.numerator_unit_concept_id)
 	LEFT JOIN VoCaB_ScHeMa.CONCEPT DU on DU.concept_id = DS.denominator_unit_concept_id
	LEFT JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table l on (l.concept_id = DF.concept_id AND l.concept_class_id = 'Dose Form')
 WHERE

     CA.ancestor_concept_id  IN (SELECT DISTINCT
 									ing.concept_id
 								FROM
 									VoCaB_ScHeMa.CONCEPT atc
 									INNER JOIN VoCaB_ScHeMa.CONCEPT_ANCESTOR ca on ca.ancestor_concept_id = atc.concept_id
 									INNER JOIN VoCaB_ScHeMa.CONCEPT ing on ing.concept_id = ca.descendant_concept_id
 								WHERE

 								atc.vocabulary_id='ATC'
 								AND
 								atc.concept_code IN ('C09A', 'C09C') --ACEIs_ARBs
 								AND
 								ing.concept_class_id = 'Ingredient'
 								AND
 								ing.standard_concept = 'S')
 	
 UNION

 SELECT
 	'OPIOIDS' [Ingredient_Category],
     A.concept_id Ingredient_concept_id,
     A.concept_Name Ingredient_name,
 	COALESCE(DS.amount_value, DS.numerator_value, 1) / COALESCE(DS.denominator_value, 1) [conversion_factor],
 	COALESCE(DS.amount_unit_concept_id, DS.numerator_unit_concept_id) [numerator_unit_concept_id],
 	NU.concept_name [numerator_unit],
 	DS.denominator_unit_concept_id,
 	DU.concept_name [denominator_unit],
     D.concept_id Drug_concept_id,
     D.concept_Name Drug_name,
     D.concept_Code Drug_concept_code,
     D.concept_Class_id Drug_concept_class,
 	DF.concept_id [route_concept_id],
 	DF.concept_name [route_name],
	l.variable_name [route_category]
 FROM
 	VoCaB_ScHeMa.concept_ancestor CA
 	INNER JOIN VoCaB_ScHeMa.concept A on CA.ancestor_concept_id = A.concept_id
 	INNER JOIN VoCaB_ScHeMa.concept D on CA.descendant_concept_id = D.concept_id
 	INNER JOIN VoCaB_ScHeMa.DRUG_STRENGTH DS on (DS.drug_concept_id = D.concept_id
 												 AND
 												 DS.ingredient_concept_id = A.concept_id)
 	LEFT JOIN VoCaB_ScHeMa.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = D.concept_id
 													   AND
 													   Cr.relationship_id LIKE '%has dose form')--= 'RxNorm has dose form')
 	LEFT JOIN VoCaB_ScHeMa.CONCEPT DF on DF.concept_id = CR.concept_id_2
 	INNER JOIN VoCaB_ScHeMa.CONCEPT NU on NU.concept_id = COALESCE(DS.amount_unit_concept_id, DS.numerator_unit_concept_id)
 	LEFT JOIN VoCaB_ScHeMa.CONCEPT DU on DU.concept_id = DS.denominator_unit_concept_id
	LEFT JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table l on (l.concept_id = DF.concept_id AND l.concept_class_id = 'Dose Form')
 WHERE
     CA.ancestor_concept_id  IN (SELECT DISTINCT
 									ing.concept_id
 								FROM
 									VoCaB_ScHeMa.CONCEPT atc
 									INNER JOIN VoCaB_ScHeMa.CONCEPT_ANCESTOR ca on ca.ancestor_concept_id = atc.concept_id
 									INNER JOIN VoCaB_ScHeMa.CONCEPT ing on ing.concept_id = ca.descendant_concept_id
									
 								WHERE

 								atc.vocabulary_id='ATC'
 								AND
 								atc.concept_code IN ('N02A') -- OPIOIDS
 								AND
 								ing.concept_class_id = 'Ingredient'
 								AND
 								ing.standard_concept = 'S')

UNION

SELECT
 	'CORTICOSTEROIDS' [Ingredient_Category],
     A.concept_id Ingredient_concept_id,
     A.concept_Name Ingredient_name,
 	COALESCE(DS.amount_value, DS.numerator_value, 1) / COALESCE(DS.denominator_value, 1) [conversion_factor],
 	COALESCE(DS.amount_unit_concept_id, DS.numerator_unit_concept_id) [numerator_unit_concept_id],
 	NU.concept_name [numerator_unit],
 	DS.denominator_unit_concept_id,
 	DU.concept_name [denominator_unit],
     D.concept_id Drug_concept_id,
     D.concept_Name Drug_name,
     D.concept_Code Drug_concept_code,
     D.concept_Class_id Drug_concept_class,
 	DF.concept_id [route_concept_id],
 	DF.concept_name [route_name],
	l.variable_name [route_category]
 FROM
 	VoCaB_ScHeMa.concept_ancestor CA
 	INNER JOIN VoCaB_ScHeMa.concept A on CA.ancestor_concept_id = A.concept_id
 	INNER JOIN VoCaB_ScHeMa.concept D on CA.descendant_concept_id = D.concept_id
 	INNER JOIN VoCaB_ScHeMa.DRUG_STRENGTH DS on (DS.drug_concept_id = D.concept_id
 												 AND
 												 DS.ingredient_concept_id = A.concept_id)
 	LEFT JOIN VoCaB_ScHeMa.CONCEPT_RELATIONSHIP CR on (CR.concept_id_1 = D.concept_id
 													   AND
 													   Cr.relationship_id LIKE '%has dose form')--= 'RxNorm has dose form')
 	LEFT JOIN VoCaB_ScHeMa.CONCEPT DF on DF.concept_id = CR.concept_id_2
 	INNER JOIN VoCaB_ScHeMa.CONCEPT NU on NU.concept_id = COALESCE(DS.amount_unit_concept_id, DS.numerator_unit_concept_id)
 	LEFT JOIN VoCaB_ScHeMa.CONCEPT DU on DU.concept_id = DS.denominator_unit_concept_id
	LEFT JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table l on (l.concept_id = DF.concept_id AND l.concept_class_id = 'Dose Form')
 WHERE
     CA.ancestor_concept_id  IN (SELECT DISTINCT
									--ing.concept_name,
 									ing.concept_id
 								FROM
 									VoCaB_ScHeMa.CONCEPT atc
 									INNER JOIN VoCaB_ScHeMa.CONCEPT_ANCESTOR ca on ca.ancestor_concept_id = atc.concept_id
 									INNER JOIN VoCaB_ScHeMa.CONCEPT ing on ing.concept_id = ca.descendant_concept_id
									
 								WHERE

 								atc.vocabulary_id='ATC'
 								AND
 								atc.concept_code IN ('H2A', 'H02A') -- CORTICOSTEROIDS FOR SYSTEMIC USE, PLAIN
 								AND
 								ing.concept_class_id = 'Ingredient'
 								AND
 								ing.standard_concept = 'S')
) F

GO

INSERT INTO LoOkUp_ScHeMa.IC3_Variable_Lookup_Table
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
	LoOkUp_ScHeMa.IC3_DRUG_LOOKUP_TABLE l
	INNER JOIN VoCaB_ScHeMa.CONCEPT C on C.concept_id = l.drug_concept_id

GO