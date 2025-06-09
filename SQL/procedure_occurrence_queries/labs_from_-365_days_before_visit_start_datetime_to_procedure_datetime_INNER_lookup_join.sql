/******
labs_from_-365_days_before_visit_start_datetime_to_procedure_datetime_INNER_lookup_join Query prepared at 2024-03-13 13:32:28.790561 by query_builder.py

retrieves variables from the following row_ids: 242||243||257||258||259||260||261||262||263||264||265||266||267||268||269||270||271||272||273||274||275||276||277||278||279||280||281||282||283||284||285||286||287||288||289||290||291||292||293||294||295||296||297||298||299||300||301||302||303||304||305||307||308||309||310||311||312||313||314||315||316||317||318||319||320||321||322||323||324||325||326||327||328||329||330||331||332||333||334||335||336||337||338||339||340||341||342||343||344||345||346||347||348||349||350||351||352||353||354||355||356||357||358||360||361||362||363||364||365||366||367||368||369||370||371
******/
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = 'parent_visit_occurrence_id') 
SELECT 
	C.subject_id,
	m.person_id,
	m.visit_occurrence_id,
	m.visit_detail_id,
	unit_concept_id,
	variable_name,
	m.measurement_datetime [measurement_datetime],
	m.value_as_concept_id [value_as_concept_id],
	m.value_as_number [value_as_number],
	m.value_source_value [value_source_value],
	m.operator_concept_id [operator_concept_id],
	m.unit_concept_id [unit_concept_id],
	m.unit_source_concept_id [unit_source_concept_id],
	m.unit_source_value [unit_source_value]
 FROM 
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vop on vop.visit_occurrence_id = po.visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vo on vo.visit_occurrence_id = vop.parent_visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.measurement m on (m.person_id = po.person_id)
	INNER JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table L on (L.concept_id = m.measurement_concept_id AND L.variable_name IN ('albumin', 'albumin_ur', 'albumin_ur_24h', 'albumin_ur_24h_t', 'alt', 'anion_gap', 'apr_ur', 'apr_ur_24h', 'ast', 'base_deficit', 'base_excess', 'basophils', 'basophils_per', 'bicarbonate', 'bilirubin_direct', 'bilirubin_tot_ur', 'bilirubin_tot_ur_pres', 'bilirubin_total', 'bnp', 'bun', 'bun_ur', 'bun_ur_24h', 'bun_ur_24h_t', 'cacr_r_ur', 'cacr_r_ur_24h', 'calcium', 'calcium_ionized', 'calcium_ionized_corr', 'calcium_ur', 'calcium_ur_24h', 'calcium_ur_24h_t', 'carboxyhem', 'chloride', 'chloride_ur', 'chloride_ur_24h', 'chloride_ur_24h_t', 'chpd_ur_24h', 'c-reactive_protein', 'creatinine', 'creatinine_ur', 'creatinine_ur_24h', 'creatinine_ur_24h_t', 'creatinine_ur_mol', 'eosinophils', 'eosinophils_per', 'esr', 'glucose', 'glucose_post', 'glucose_t', 'glucose_ur', 'hba1c', 'hematocrit', 'hgb', 'hgb_electrophoresis', 'hgb_ur', 'lactate', 'lymphocytes', 'lymphocytes_per', 'mch', 'mchc', 'mcv', 'mdw', 'methhem', 'microalbumin_24h', 'microalbumin_24h_t', 'microalbumin_ur', 'monocytes', 'monocytes_per', 'mpv', 'neutrophil', 'neutrophil_per_band', 'neutrophils_band', 'neutrophils_per', 'o2sata', 'p_panel', 'p24', 'pao2', 'pco2a', 'ph', 'ph_ur', 'platelet', 'po2a', 'potassium', 'potassium_ur', 'potassium_ur_24h', 'potassium_ur_24h_mt', 'potassium_ur_24h_t', 'rbc', 'rbc_ur', 'rbc_ur_pres', 'rbc_ur_v', 'rdw', 'scr_ur_24h', 'serum_asparate', 'serum_co2', 'serum_hco3', 'serum_inr', 'sg', 'sodium', 'sodium_u_24hr', 'sodium_ur', 'sodium_ur_24h', 'sodium_ur_24h_t', 'troponin_i', 'troponin_t', 'uacr', 'uap_cat', 'umacr', 'uncr', 'upcr', 'wbc', 'wbc_ur', 'wbc_ur_pres', 'wbc_ur_sedim', 'wbc_ur_sedim_l'))
 WHERE 
	(
	m.measurement_datetime BETWEEN DATEADD(DAY, -365, vo.parent_visit_start_datetime) AND procedure_datetime
	AND
	variable_name IN ('albumin', 'albumin_ur', 'albumin_ur_24h', 'albumin_ur_24h_t', 'alt', 'anion_gap', 'apr_ur', 'apr_ur_24h', 'ast', 'base_deficit', 'base_excess', 'basophils', 'basophils_per', 'bicarbonate', 'bilirubin_direct', 'bilirubin_tot_ur', 'bilirubin_tot_ur_pres', 'bilirubin_total', 'bnp', 'bun', 'bun_ur', 'bun_ur_24h', 'bun_ur_24h_t', 'cacr_r_ur', 'cacr_r_ur_24h', 'calcium', 'calcium_ionized', 'calcium_ionized_corr', 'calcium_ur', 'calcium_ur_24h', 'calcium_ur_24h_t', 'carboxyhem', 'chloride', 'chloride_ur', 'chloride_ur_24h', 'chloride_ur_24h_t', 'chpd_ur_24h', 'c-reactive_protein', 'creatinine', 'creatinine_ur', 'creatinine_ur_24h', 'creatinine_ur_24h_t', 'creatinine_ur_mol', 'eosinophils', 'eosinophils_per', 'esr', 'glucose', 'glucose_post', 'glucose_t', 'glucose_ur', 'hba1c', 'hematocrit', 'hgb', 'hgb_electrophoresis', 'hgb_ur', 'lactate', 'lymphocytes', 'lymphocytes_per', 'mch', 'mchc', 'mcv', 'mdw', 'methhem', 'microalbumin_24h', 'microalbumin_24h_t', 'microalbumin_ur', 'monocytes', 'monocytes_per', 'mpv', 'neutrophil', 'neutrophil_per_band', 'neutrophils_band', 'neutrophils_per', 'o2sata', 'p_panel', 'p24', 'pao2', 'pco2a', 'ph', 'ph_ur', 'platelet', 'po2a', 'potassium', 'potassium_ur', 'potassium_ur_24h', 'potassium_ur_24h_mt', 'potassium_ur_24h_t', 'rbc', 'rbc_ur', 'rbc_ur_pres', 'rbc_ur_v', 'rdw', 'scr_ur_24h', 'serum_asparate', 'serum_co2', 'serum_hco3', 'serum_inr', 'sg', 'sodium', 'sodium_u_24hr', 'sodium_ur', 'sodium_ur_24h', 'sodium_ur_24h_t', 'troponin_i', 'troponin_t', 'uacr', 'uap_cat', 'umacr', 'uncr', 'upcr', 'wbc', 'wbc_ur', 'wbc_ur_pres', 'wbc_ur_sedim', 'wbc_ur_sedim_l')
	)
	AND
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY;
ELSE
SELECT 
	C.subject_id,
	m.person_id,
	m.visit_occurrence_id,
	m.visit_detail_id,
	unit_concept_id,
	variable_name,
	m.measurement_datetime [measurement_datetime],
	m.value_as_concept_id [value_as_concept_id],
	m.value_as_number [value_as_number],
	m.value_source_value [value_source_value],
	m.operator_concept_id [operator_concept_id],
	m.unit_concept_id [unit_concept_id],
	m.unit_source_concept_id [unit_source_concept_id],
	m.unit_source_value [unit_source_value]
 FROM 
	ReSuLtS_ScHeMa.COHORT c
	INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
	INNER JOIN DaTa_ScHeMa.visit_occurrence vo on vo.visit_occurrence_id = po.visit_occurrence_id
	INNER JOIN DaTa_ScHeMa.measurement m on (m.person_id = po.person_id)
	INNER JOIN LoOkUp_ScHeMa.IC3_Variable_Lookup_Table L on (L.concept_id = m.measurement_concept_id AND L.variable_name IN ('albumin', 'albumin_ur', 'albumin_ur_24h', 'albumin_ur_24h_t', 'alt', 'anion_gap', 'apr_ur', 'apr_ur_24h', 'ast', 'base_deficit', 'base_excess', 'basophils', 'basophils_per', 'bicarbonate', 'bilirubin_direct', 'bilirubin_tot_ur', 'bilirubin_tot_ur_pres', 'bilirubin_total', 'bnp', 'bun', 'bun_ur', 'bun_ur_24h', 'bun_ur_24h_t', 'cacr_r_ur', 'cacr_r_ur_24h', 'calcium', 'calcium_ionized', 'calcium_ionized_corr', 'calcium_ur', 'calcium_ur_24h', 'calcium_ur_24h_t', 'carboxyhem', 'chloride', 'chloride_ur', 'chloride_ur_24h', 'chloride_ur_24h_t', 'chpd_ur_24h', 'c-reactive_protein', 'creatinine', 'creatinine_ur', 'creatinine_ur_24h', 'creatinine_ur_24h_t', 'creatinine_ur_mol', 'eosinophils', 'eosinophils_per', 'esr', 'glucose', 'glucose_post', 'glucose_t', 'glucose_ur', 'hba1c', 'hematocrit', 'hgb', 'hgb_electrophoresis', 'hgb_ur', 'lactate', 'lymphocytes', 'lymphocytes_per', 'mch', 'mchc', 'mcv', 'mdw', 'methhem', 'microalbumin_24h', 'microalbumin_24h_t', 'microalbumin_ur', 'monocytes', 'monocytes_per', 'mpv', 'neutrophil', 'neutrophil_per_band', 'neutrophils_band', 'neutrophils_per', 'o2sata', 'p_panel', 'p24', 'pao2', 'pco2a', 'ph', 'ph_ur', 'platelet', 'po2a', 'potassium', 'potassium_ur', 'potassium_ur_24h', 'potassium_ur_24h_mt', 'potassium_ur_24h_t', 'rbc', 'rbc_ur', 'rbc_ur_pres', 'rbc_ur_v', 'rdw', 'scr_ur_24h', 'serum_asparate', 'serum_co2', 'serum_hco3', 'serum_inr', 'sg', 'sodium', 'sodium_u_24hr', 'sodium_ur', 'sodium_ur_24h', 'sodium_ur_24h_t', 'troponin_i', 'troponin_t', 'uacr', 'uap_cat', 'umacr', 'uncr', 'upcr', 'wbc', 'wbc_ur', 'wbc_ur_pres', 'wbc_ur_sedim', 'wbc_ur_sedim_l'))
 WHERE 
	(
	m.measurement_datetime BETWEEN DATEADD(DAY, -365, visit_start_datetime) AND procedure_datetime
	AND
	variable_name IN ('albumin', 'albumin_ur', 'albumin_ur_24h', 'albumin_ur_24h_t', 'alt', 'anion_gap', 'apr_ur', 'apr_ur_24h', 'ast', 'base_deficit', 'base_excess', 'basophils', 'basophils_per', 'bicarbonate', 'bilirubin_direct', 'bilirubin_tot_ur', 'bilirubin_tot_ur_pres', 'bilirubin_total', 'bnp', 'bun', 'bun_ur', 'bun_ur_24h', 'bun_ur_24h_t', 'cacr_r_ur', 'cacr_r_ur_24h', 'calcium', 'calcium_ionized', 'calcium_ionized_corr', 'calcium_ur', 'calcium_ur_24h', 'calcium_ur_24h_t', 'carboxyhem', 'chloride', 'chloride_ur', 'chloride_ur_24h', 'chloride_ur_24h_t', 'chpd_ur_24h', 'c-reactive_protein', 'creatinine', 'creatinine_ur', 'creatinine_ur_24h', 'creatinine_ur_24h_t', 'creatinine_ur_mol', 'eosinophils', 'eosinophils_per', 'esr', 'glucose', 'glucose_post', 'glucose_t', 'glucose_ur', 'hba1c', 'hematocrit', 'hgb', 'hgb_electrophoresis', 'hgb_ur', 'lactate', 'lymphocytes', 'lymphocytes_per', 'mch', 'mchc', 'mcv', 'mdw', 'methhem', 'microalbumin_24h', 'microalbumin_24h_t', 'microalbumin_ur', 'monocytes', 'monocytes_per', 'mpv', 'neutrophil', 'neutrophil_per_band', 'neutrophils_band', 'neutrophils_per', 'o2sata', 'p_panel', 'p24', 'pao2', 'pco2a', 'ph', 'ph_ur', 'platelet', 'po2a', 'potassium', 'potassium_ur', 'potassium_ur_24h', 'potassium_ur_24h_mt', 'potassium_ur_24h_t', 'rbc', 'rbc_ur', 'rbc_ur_pres', 'rbc_ur_v', 'rdw', 'scr_ur_24h', 'serum_asparate', 'serum_co2', 'serum_hco3', 'serum_inr', 'sg', 'sodium', 'sodium_u_24hr', 'sodium_ur', 'sodium_ur_24h', 'sodium_ur_24h_t', 'troponin_i', 'troponin_t', 'uacr', 'uap_cat', 'umacr', 'uncr', 'upcr', 'wbc', 'wbc_ur', 'wbc_ur_pres', 'wbc_ur_sedim', 'wbc_ur_sedim_l')
	)
	AND
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY;