IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = 'parent_visit_occurrence_id')
SELECT 
	C.subject_id,
	vo.[parent_visit_occurrence_id]
	  ,MAX(CASE WHEN vo.parent_discharged_to_source_value LIKE '%PLANNED READMIT%' THEN 1 ELSE 0 END) [planned_readmit]
	  , MAX(CASE WHEN vo.parent_discharged_to_source_value LIKE '%HOSPICE%' THEN 1 ELSE 0 END) [dischg_hospice]
	  , MAX(CASE WHEN vo2.visit_occurrence_id IS NOT NULL THEN 1 ELSE 0 END) [30_day_readmit]
  FROM
	ReSuLtS_ScHeMa.COHORT C
	INNER JOIN DaTa_ScHeMa.PROCEDURE_OCCURRENCE po on C.subject_id = po.procedure_occurrence_id
	INNER JOIN DaTa_ScHeMa.[VISIT_OCCURRENCE] vom on vom.visit_occurrence_id = po.visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.[VISIT_OCCURRENCE] vo on vo.visit_occurrence_id = vom.parent_visit_occurrence_id
    LEFT JOIN (SELECT
					person_id,
					visit_occurrence_id,
					parent_visit_start_datetime,
					parent_visit_end_datetime
				FROM
					DaTa_ScHeMa.[VISIT_OCCURRENCE]
				WHERE
					visit_concept_id IN (SELECT concept_id FROM LoOkUp_ScHeMa.IC3_Variable_Lookup_Table where variable_name = 'inpatient_hospital_encounter')
					AND
					parent_visit_occurrence_id = visit_occurrence_id) vo2 on (vo2.person_id = vo.person_id
																		      AND
																			  vo2.parent_visit_start_datetime BETWEEN vo.parent_visit_end_datetime AND DATEADD(DAY, 30, vo.parent_visit_end_datetime)
																			  AND
																			  vo2.visit_occurrence_id <> vo.visit_occurrence_id)

WHERE
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY

GROUP BY
	C.Subject_id,
	vo.[parent_visit_occurrence_id]
ELSE
SELECT 
	C.subject_id
	,vo.[visit_occurrence_id]
	  ,MAX(CASE WHEN vo.discharged_to_source_value LIKE '%PLANNED READMIT%' THEN 1 ELSE 0 END) [planned_readmit]
	  , MAX(CASE WHEN vo.discharged_to_source_value LIKE '%HOSPICE%' THEN 1 ELSE 0 END) [dischg_hospice]
	  , MAX(CASE WHEN vo2.visit_occurrence_id IS NOT NULL THEN 1 ELSE 0 END) [30_day_readmit]
  FROM
	ReSuLtS_ScHeMa.COHORT C
	INNER JOIN DaTa_ScHeMa.PROCEDURE_OCCURRENCE po on C.subject_id = po.procedure_occurrence_id
	INNER JOIN DaTa_ScHeMa.[VISIT_OCCURRENCE] vo on vo.visit_occurrence_id = po.visit_occurrence_id
    LEFT JOIN (SELECT
					person_id,
					visit_occurrence_id,
					visit_start_datetime,
					visit_end_datetime
				FROM
					DaTa_ScHeMa.[VISIT_OCCURRENCE]
				WHERE
					visit_concept_id IN (SELECT concept_id FROM LoOkUp_ScHeMa.IC3_Variable_Lookup_Table where variable_name = 'inpatient_hospital_encounter')) vo2 on (vo2.person_id = vo.person_id
																		      AND
																			  vo2.visit_start_datetime BETWEEN vo.visit_end_datetime AND DATEADD(DAY, 30, vo.visit_end_datetime)
																			  AND
																			  vo2.visit_occurrence_id <> vo.visit_occurrence_id)

WHERE
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY

GROUP BY
	C.Subject_id,
	vo.[visit_occurrence_id]