/******
drug_exposure_from_-365_days_before_visit_start_datetime_to_visit_start_datetime_INNER_lookup_join Query prepared at 2024-03-13 13:32:28.743197 by query_builder.py

retrieves variables from the following row_ids: 208||209||210||211||212||213||214||215||216||217||218||219||220||221
******/
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = 'parent_visit_occurrence_id') 
SELECT 
	C.subject_id,
	drugE.person_id,
	drugE.visit_occurrence_id,
	drugE.visit_detail_id,
	drugE.route_concept_id,
	L.ingredient_category [variable_name],
	drugE.drug_exposure_start_datetime [drug_exposure_start_datetime],
	drugE.drug_exposure_end_datetime [drug_exposure_end_datetime],
	drugE.drug_concept_id [drug_concept_id]
 FROM 
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vop on vop.visit_occurrence_id = po.visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vo on vo.visit_occurrence_id = vop.parent_visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.drug_exposure drugE on (drugE.person_id = po.person_id)
	INNER JOIN LoOkUp_ScHeMa.IC3_DRUG_LOOKUP_TABLE L on (L.drug_concept_id = drugE.drug_concept_id AND L.ingredient_category IN ('asprin', 'statins', 'AMINOGLYCOSIDES', 'ACEIs_ARBs', 'diuretics', 'nsaids', 'pressors_inotropes', 'OPIOIDS', 'vancomycin', 'beta_blockers', 'antiemetics', 'bicarbonates'))
 WHERE 
	drugE.drug_exposure_start_datetime BETWEEN DATEADD(DAY, -365, vo.parent_visit_start_datetime) AND vo.parent_visit_start_datetime
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
	drugE.route_concept_id,
	L.ingredient_category [variable_name],
	drugE.drug_exposure_start_datetime [drug_exposure_start_datetime],
	drugE.drug_exposure_end_datetime [drug_exposure_end_datetime],
	drugE.drug_concept_id [drug_concept_id]
 FROM 
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vo on vo.visit_occurrence_id = po.visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.drug_exposure drugE on (drugE.person_id = po.person_id)
	INNER JOIN LoOkUp_ScHeMa.IC3_DRUG_LOOKUP_TABLE L on (L.drug_concept_id = drugE.drug_concept_id AND L.ingredient_category IN ('asprin', 'statins', 'AMINOGLYCOSIDES', 'ACEIs_ARBs', 'diuretics', 'nsaids', 'pressors_inotropes', 'OPIOIDS', 'vancomycin', 'beta_blockers', 'antiemetics', 'bicarbonates'))
 WHERE 
	drugE.drug_exposure_start_datetime BETWEEN DATEADD(DAY, -365, visit_start_datetime) AND visit_start_datetime
	AND
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY;