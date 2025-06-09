/******
ingredient_name_drug_search_drug_exposure_from_visit_occurrence_INNER_lookup_join Query prepared at 2024-03-13 13:32:28.745783 by query_builder.py

retrieves variables from the following row_ids: 157||158||159||160
******/
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = 'parent_visit_occurrence_id') 
SELECT 
	C.subject_id,
	drugE.person_id,
	drugE.visit_occurrence_id,
	drugE.visit_detail_id,
	conversion_factor,
	numerator_unit_concept_id,
	numerator_unit,
	denominator_unit_concept_id,
	denominator_unit,
	drugE.route_concept_id,
	ingredient_name [variable_name],
	drugE.drug_exposure_start_datetime [drug_exposure_start_datetime],
	drugE.drug_exposure_end_datetime [drug_exposure_end_datetime],
	drugE.quantity [quantity]
 FROM 
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vop on vop.visit_occurrence_id = po.visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vo on vo.visit_occurrence_id = vop.parent_visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.drug_exposure drugE on (drugE.person_id = po.person_id)
	INNER JOIN LoOkUp_ScHeMa.IC3_DRUG_LOOKUP_TABLE L on (L.drug_concept_id = drugE.drug_concept_id AND L.ingredient_name IN ('epinephrine', 'norepinephrine', 'dobutamine', 'dopamine'))
 WHERE 
	(
	drugE.drug_exposure_start_datetime BETWEEN vo.parent_visit_start_datetime AND vo.parent_visit_end_datetime
	AND
	(drugE.quantity <> 0)
	)
	AND
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY;
ELSE
SELECT 
	C.subject_id,
	drugE.person_id,
	drugE.visit_occurrence_id,
	drugE.visit_detail_id,
	conversion_factor,
	numerator_unit_concept_id,
	numerator_unit,
	denominator_unit_concept_id,
	denominator_unit,
	drugE.route_concept_id,
	ingredient_name [variable_name],
	drugE.drug_exposure_start_datetime [drug_exposure_start_datetime],
	drugE.drug_exposure_end_datetime [drug_exposure_end_datetime],
	drugE.quantity [quantity]
 FROM 
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vo on vo.visit_occurrence_id = po.visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.drug_exposure drugE on (drugE.person_id = po.person_id)
	INNER JOIN LoOkUp_ScHeMa.IC3_DRUG_LOOKUP_TABLE L on (L.drug_concept_id = drugE.drug_concept_id AND L.ingredient_name IN ('epinephrine', 'norepinephrine', 'dobutamine', 'dopamine'))
 WHERE 
	(
	drugE.drug_exposure_start_datetime BETWEEN visit_start_datetime AND visit_end_datetime
	AND
	(drugE.quantity <> 0)
	)
	AND
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY;