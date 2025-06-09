/******
device_exposure_from_visit_occurrence_INNER_lookup_join Query prepared at 2024-03-13 13:32:28.742197 by query_builder.py

retrieves variables from the following row_ids: 48||49||50||83||112||138||165
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
	devE.unit_concept_id [unit_concept_id],
	devE.quantity [value_as_number],
	CASE WHEN ([device_exposure_start_datetime] < procedure_datetime) AND variable_name = 'rbc_transfusion' THEN 'preop_rbc_volume'
         WHEN ([device_exposure_start_datetime] BETWEEN procedure_datetime AND procedure_end_datetime) AND variable_name = 'rbc_transfusion'THEN 'intraop_rbc_volume'
         WHEN ([device_exposure_start_datetime] > procedure_end_datetime) AND variable_name = 'rbc_transfusion' THEN 'postop_rbc_volume' ELSE NULL END [transfusion],
	devE.device_concept_id [device_concept_id],
	devE.visit_detail_id [visit_detail_id]
 FROM 
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vop on vop.visit_occurrence_id = po.visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vo on vo.visit_occurrence_id = vop.parent_visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.device_exposure devE on (devE.person_id = po.person_id)
	INNER JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table L on (L.concept_id = devE.device_concept_id AND L.variable_name IN ('rbc_transfusion', 'mechanical_ventilation'))
 WHERE 
	devE.device_exposure_start_datetime BETWEEN vo.parent_visit_start_datetime AND vo.parent_visit_end_datetime
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
	devE.unit_concept_id [unit_concept_id],
	devE.quantity [value_as_number],
	CASE WHEN ([device_exposure_start_datetime] < procedure_datetime) AND variable_name = 'rbc_transfusion' THEN 'preop_rbc_volume'
         WHEN ([device_exposure_start_datetime] BETWEEN procedure_datetime AND procedure_end_datetime) AND variable_name = 'rbc_transfusion' THEN 'intraop_rbc_volume'
         WHEN ([device_exposure_start_datetime] > procedure_end_datetime) AND variable_name = 'rbc_transfusion' THEN 'postop_rbc_volume' ELSE NULL END [transfusion],
	devE.device_concept_id [device_concept_id],
	devE.visit_detail_id [visit_detail_id]
 FROM 
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vo on vo.visit_occurrence_id = po.visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.device_exposure devE on (devE.person_id = po.person_id)
	INNER JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table L on (L.concept_id = devE.device_concept_id AND L.variable_name IN ('rbc_transfusion', 'mechanical_ventilation'))
 WHERE 
	devE.device_exposure_start_datetime BETWEEN visit_start_datetime AND visit_end_datetime
	AND
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY;