/******
procedure_occurrence_from_procedure_end_datetime_to_visit_end_datetime_INNER_lookup_join Query prepared at 2024-03-13 13:32:28.817564 by query_builder.py

retrieves variables from the following row_ids: 37||38||39||40||41||42||43||44
******/
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = 'parent_visit_occurrence_id') 
SELECT 
	C.subject_id,
	po.person_id,
	po.visit_occurrence_id,
	po.visit_detail_id,
	variable_name,
	po.procedure_datetime [procedure_datetime],
	po.procedure_end_datetime [procedure_end_datetime],
	po.procedure_concept_id [procedure_concept_id]
 FROM 
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence pop on pop.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.person_id = pop.person_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vop on vop.visit_occurrence_id = po.visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vo on vo.visit_occurrence_id = vop.parent_visit_occurrence_id
	INNER JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table L on (L.concept_id = po.procedure_concept_id AND L.variable_name IN ('cpr', 'intubation', 'arterial_catheter', 'central_venous_catheter', 'pulmonary_artery_catheter', 'bronchoscopy', 'cardioversion_electric', 'chest_tube'))
 WHERE 
	po.procedure_date BETWEEN CONVERT(DATE, po.procedure_end_datetime) AND CONVERT(DATE, vo.parent_visit_end_datetime)
	AND
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY;
 ELSE
	SELECT 
	C.subject_id,
	po.person_id,
	po.visit_occurrence_id,
	po.visit_detail_id,
	variable_name,
	po.procedure_datetime [procedure_datetime],
	po.procedure_end_datetime [procedure_end_datetime],
	po.procedure_concept_id [procedure_concept_id]
 FROM 
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence pop on pop.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.person_id = pop.person_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vo on vo.visit_occurrence_id = po.visit_occurrence_id
	INNER JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table L on (L.concept_id = po.procedure_concept_id AND L.variable_name IN ('cpr', 'intubation', 'arterial_catheter', 'central_venous_catheter', 'pulmonary_artery_catheter', 'bronchoscopy', 'cardioversion_electric', 'chest_tube'))
 WHERE 
	po.procedure_date BETWEEN CONVERT(DATE, po.procedure_end_datetime) AND CONVERT(DATE, visit_end_datetime)
	AND
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY;