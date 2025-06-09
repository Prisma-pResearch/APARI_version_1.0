/******
condition_occurrence_from_before_visit_occurrence_to_visit_start_date_LEFT_lookup_join Query prepared at 2024-03-13 13:32:28.725084 by query_builder.py

retrieves variables from the following row_ids: 171||172||173||174||175||176||177||178||179||180||181||182||183||184||185||186||187||188||189||190||191||192||193||194||195||196||197||198||199||200||201||202||203||204||205||206
******/
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = 'parent_visit_occurrence_id') 
SELECT 
	C.subject_id,
	co.person_id,
	co.visit_occurrence_id,
	co.visit_detail_id,
	variable_name,
	CASE WHEN co.condition_status_concept_id IN (32890, 32901, 32907) THEN 1 ELSE NULL END [poa],
	co.condition_start_date [condition_start_date],
	co.condition_end_date [condition_end_date],
	co.condition_concept_id [condition_concept_id],
	co.condition_concept_id [hemoplegia_or_paralegia],
	co.condition_concept_id [anemia_deficiency]
 FROM 
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vop on vop.visit_occurrence_id = po.visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vo on vo.visit_occurrence_id = vop.parent_visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.condition_occurrence co on (co.person_id = po.person_id)
	LEFT JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table L on (L.concept_id = co.condition_concept_id AND L.variable_name IN ('charlson_comorbidity_mi', 'cv_hf', 'pvd', 'cerebrovascular_disease', 'dementia', 'chronic_pulmonary_disease', 'rheumatologic_disease', 'pud', 'mild_liver_disease', 'diabetes_mild_to_moderate', 'diabetes_with_chronic_complications', 'renal_disease', 'malignant_solid_tumor_without_metastisis', 'moderate_to_severe_liver_disease', 'metastatic_solid_tumor', 'aids', 'alcohol_abuse', 'anemia_iron_deficiency_chronic_blood_loss', 'cardiac_arrhythmia', 'coagulopathy', 'depression', 'drug_abuse', 'lytes', 'complicated_hypertension', 'hypothyroidism', 'essential_hypertension', 'lymphoma', 'obesity', 'other_neurological_disorder', 'pulmonary_circulation_disease', 'psychotic_disorder', 'heart_valve_disease', 'malnutrition_macronutrients_weightloss'))
 WHERE 
	co.condition_start_date <= CONVERT(DATE, vo.parent_visit_start_datetime)
	AND
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY;
ELSE
SELECT 
	C.subject_id,
	co.person_id,
	co.visit_occurrence_id,
	co.visit_detail_id,
	variable_name,
	CASE WHEN co.condition_status_concept_id IN (32890, 32901, 32907) THEN 1 ELSE NULL END [poa],
	co.condition_start_date [condition_start_date],
	co.condition_end_date [condition_end_date],
	co.condition_concept_id [condition_concept_id],
	co.condition_concept_id [hemoplegia_or_paralegia],
	co.condition_concept_id [anemia_deficiency]
 FROM 
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vo on vo.visit_occurrence_id = po.visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.condition_occurrence co on (co.person_id = po.person_id)
	LEFT JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table L on (L.concept_id = co.condition_concept_id AND L.variable_name IN ('charlson_comorbidity_mi', 'cv_hf', 'pvd', 'cerebrovascular_disease', 'dementia', 'chronic_pulmonary_disease', 'rheumatologic_disease', 'pud', 'mild_liver_disease', 'diabetes_mild_to_moderate', 'diabetes_with_chronic_complications', 'renal_disease', 'malignant_solid_tumor_without_metastisis', 'moderate_to_severe_liver_disease', 'metastatic_solid_tumor', 'aids', 'alcohol_abuse', 'anemia_iron_deficiency_chronic_blood_loss', 'cardiac_arrhythmia', 'coagulopathy', 'depression', 'drug_abuse', 'lytes', 'complicated_hypertension', 'hypothyroidism', 'essential_hypertension', 'lymphoma', 'obesity', 'other_neurological_disorder', 'pulmonary_circulation_disease', 'psychotic_disorder', 'heart_valve_disease', 'malnutrition_macronutrients_weightloss'))
 WHERE 
	co.condition_start_date <= visit_start_date
	AND
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY;