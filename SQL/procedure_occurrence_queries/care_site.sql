
SELECT
C.subject_id,
CASE WHEN loc.zip = 32608 THEN 1 ELSE 0 END AS site_gnv,
CASE WHEN loc.zip = 32218 THEN 1 ELSE 0 END AS site_jax_north,
CASE WHEN loc.zip = 32209 THEN 1 ELSE 0 END AS site_jax,
CASE WHEN loc.zip = 32159 THEN 1 ELSE 0 END AS site_villages,
CASE WHEN loc.zip = 34748 THEN 1 ELSE 0 END AS site_leesburg
   

FROM
ReSuLtS_ScHeMa.COHORT c
INNER JOIN DaTa_ScHeMa.procedure_occurrence po on po.procedure_occurrence_id = C.subject_id
INNER JOIN DaTa_ScHeMa.visit_occurrence vop on vop.visit_occurrence_id = po.visit_occurrence_id
INNER JOIN DaTa_ScHeMa.visit_occurrence vo on vo.visit_occurrence_id = vop.parent_visit_occurrence_id
LEFT JOIN DaTa_ScHeMa.care_site care on (care.care_site_id = vo.care_site_id)
LEFT JOIN DaTa_ScHeMa.[LOCATION] loc on loc.location_id = care.location_id 

WHERE		
C.cohort_definition_id = XXXXXX
AND
C.subset_id = YYYY


