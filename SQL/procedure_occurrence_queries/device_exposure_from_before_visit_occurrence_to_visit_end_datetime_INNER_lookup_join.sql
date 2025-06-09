/******
device_exposure_from_before_visit_occurrence_to_visit_end_datetime_INNER_lookup_join Query prepared at 2024-03-13 13:32:28.741197 by query_builder.py

retrieves variables from the following row_ids: 91
******/
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = 'parent_visit_occurrence_id') 
SELECT 
	C.subject_id,
	devE.person_id,
	devE.visit_occurrence_id,
	devE.visit_detail_id,
	variable_name,
	devE.device_exposure_start_datetime [device_exposure_start_datetime],
	devE.device_exposure_end_datetime [device_exposure_end_datetime],
	devE.device_concept_id [device_concept_id]
 FROM 
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vop on vop.visit_occurrence_id = po.visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vo on vo.visit_occurrence_id = vop.parent_visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.device_exposure devE on (devE.person_id = po.person_id)
	INNER JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table L on (L.concept_id = devE.device_concept_id AND L.variable_name IN ('dialysis'))
 WHERE 
	devE.device_exposure_start_datetime <= vo.parent_visit_end_datetime
	AND
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY;
ELSE
SELECT 
	C.subject_id,
	devE.person_id,
	devE.visit_occurrence_id,
	devE.visit_detail_id,
	variable_name,
	devE.device_exposure_start_datetime [device_exposure_start_datetime],
	devE.device_exposure_end_datetime [device_exposure_end_datetime],
	devE.device_concept_id [device_concept_id]
 FROM 
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vo on vo.visit_occurrence_id = po.visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.device_exposure devE on (devE.person_id = po.person_id)
	INNER JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table L on (L.concept_id = devE.device_concept_id AND L.variable_name IN ('dialysis'))
 WHERE 
	devE.device_exposure_start_datetime <= visit_end_datetime
	AND
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY;