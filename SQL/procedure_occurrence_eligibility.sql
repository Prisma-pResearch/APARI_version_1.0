
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = 'parent_visit_occurrence_id') 
with partitioned as (
SELECT 
po.person_id,
po.visit_occurrence_id,
po.procedure_occurrence_id,
vo.visit_start_datetime,
vo.visit_end_datetime,
vo.visit_source_value,
vo.discharged_to_source_value as dischg_disposition,       
po.procedure_datetime as surgery_start_datetime,
po.procedure_end_datetime as surgery_end_datetime,
DATEDIFF(Year, p.birth_datetime, vo.visit_start_datetime) as age,
p.gender_source_value as gender,
p.race_source_value as race,
p.ethnicity_source_value as ethnicity,
c.care_site_name [site],
--COALESCE(LEFT(loc.zip, 5), '32608') [facility_zip],
LEFT(loc.zip, 5) as [facility_zip],
ROW_NUMBER() OVER(PARTITION BY 
vo.visit_occurrence_id ORDER BY  po.procedure_datetime ASC) AS seq


FROM

(SELECT person_id, procedure_date, count(*) [count]
FROM
    DaTa_ScHeMa.PROCEDURE_OCCURRENCE po
WHERE
    po.procedure_concept_id = 2000000027 -- custom operation
    AND
    (po.procedure_end_datetime is not null AND po.procedure_datetime is not null )  -- ensure the surgery occurred
GROUP BY
    person_id, procedure_date
HAVING
    count(*) = 1) as valid_cases
INNER JOIN DaTa_ScHeMa.PROCEDURE_OCCURRENCE po on (po.person_id = valid_cases.person_id AND po.procedure_date = valid_cases.procedure_date
                                            AND po.procedure_concept_id = 2000000027) -- custom operation
INNER JOIN DaTa_ScHeMa.visit_occurrence vop on po.visit_occurrence_id = vop.visit_occurrence_id
INNER JOIN DaTa_ScHeMa.visit_occurrence vo on  vo.visit_occurrence_id = vop.parent_visit_occurrence_id

    INNER JOIN DaTa_ScHeMa.PERSON p on po.person_id = p.person_id
    --INNER JOIN dbo.IC3_Variable_Lookup_Table l on (vo.visit_concept_id = l.concept_id
    --                                        AND
    --                                        l.variable_name IN ('inpatient_hospital_encounter'))
    LEFT JOIN DaTa_ScHeMa.care_site c on (c.care_site_id = vo.care_site_id)
    LEFT JOIN DaTa_ScHeMa.[LOCATION] loc on loc.location_id = c.location_id
    INNER JOIN VoCaB_ScHeMa.CONCEPT v_con on v_con.concept_id = vo.visit_concept_id
WHERE
	(vo.visit_start_date between '2014-07-01' and '2023-07-31' )
    AND
    DATEDIFF(Year, p.birth_datetime, vo.visit_start_datetime)>=18  --patient is an adult at time of admission
    
    AND

    DATEDIFF(hour, vo.visit_start_datetime, vo.visit_end_datetime) >= 24   -- ensure the patient stayed for at least 24 hours
    AND
    DATEDIFF(minute, po.procedure_datetime, po.procedure_end_datetime) > 20  -- ensure the surgery was longer than 20 minutes
    AND
    vo.visit_start_datetime is not null  --ensure encounter has admit  datetime
    AND
    vo.visit_end_datetime is not null   --ensure encounter has discharge datetime
    AND
	loc.zip in (32608,32209,32218,32159,34748)  -- UFH GNV, JAX, JAX North,Leesburg, Villages
    AND
    (po.procedure_end_datetime is not null AND po.procedure_datetime is not null )  -- ensure the surgery occurred
    AND
    vo.visit_concept_id IN (SELECT concept_id FROM LoOkUp_ScHeMa.IC3_Variable_Lookup_Table where variable_name = 'inpatient_hospital_encounter'))


SELECT 
    *
FROM
    partitioned 
WHERE
    seq = 1 -- Ensure only the first surgery is captured

ELSE
with partitioned as (
SELECT 
po.person_id,
po.visit_occurrence_id,
po.procedure_occurrence_id,
 --vo.visit_occurrence_source_id,
vo.visit_start_datetime,
vo.visit_end_datetime,
vo.visit_source_value,
vo.discharged_to_source_value as dischg_disposition,
-- ou.surgery_order,
-- po.procedure_date,       
po.procedure_datetime as surgery_start_datetime,
po.procedure_end_datetime as surgery_end_datetime,
-- ou.clean_death_date,
-- po.procedure_concept_id,
-- po.procedure_source_concept_id,
-- po.procedure_source_value,
-- po.provider_id [main_surgeon_dr_deiden_id],
-- ou.cpt_1_description,
DATEDIFF(Year, p.birth_datetime, vo.visit_start_datetime) as age,
p.gender_source_value as gender,
p.race_source_value as race,
p.ethnicity_source_value as ethnicity,
c.care_site_name [site],
COALESCE(LEFT(loc.zip, 5), '55422') [facility_zip],
ROW_NUMBER() OVER(PARTITION BY 
vo.visit_occurrence_id ORDER BY  po.procedure_datetime ASC) AS seq
----vo.visit_concept_id,
----v_con.concept_name,
----COUNT(*)

FROM

(SELECT person_id, procedure_date, count(*) [count]
FROM
    DaTa_ScHeMa.PROCEDURE_OCCURRENCE po
WHERE
    po.procedure_concept_id = 2000000027 -- custom operation
    AND
    (po.procedure_end_datetime is not null AND po.procedure_datetime is not null )  -- ensure the surgery occurred
GROUP BY
    person_id, procedure_date
HAVING
    count(*) = 1) as valid_cases
INNER JOIN DaTa_ScHeMa.PROCEDURE_OCCURRENCE po on (po.person_id = valid_cases.person_id AND po.procedure_date = valid_cases.procedure_date
                                            AND po.procedure_concept_id = 2000000027) -- custom operation
INNER JOIN DaTa_ScHeMa.visit_occurrence vo on po.visit_occurrence_id = vo.visit_occurrence_id

    INNER JOIN DaTa_ScHeMa.PERSON p on po.person_id = p.person_id
    --INNER JOIN dbo.IC3_Variable_Lookup_Table l on (vo.visit_concept_id = l.concept_id
    --                                        AND
    --                                        l.variable_name IN ('inpatient_hospital_encounter'))
    LEFT JOIN DaTa_ScHeMa.care_site c on (c.care_site_id = vo.care_site_id)
    LEFT JOIN DaTa_ScHeMa.[LOCATION] loc on loc.location_id = c.location_id
    INNER JOIN VoCaB_ScHeMa.CONCEPT v_con on v_con.concept_id = vo.visit_concept_id
WHERE
    (vo.visit_start_date > '2014-07-01' )  --admission within study period
    AND
    DATEDIFF(Year, p.birth_datetime, vo.visit_start_datetime)>=18  --patient is an adult at time of admission
    
    AND

    DATEDIFF(hour, vo.visit_start_datetime, vo.visit_end_datetime) >= 24   -- ensure the patient stayed for at least 24 hours
    AND
    DATEDIFF(minute, po.procedure_datetime, po.procedure_end_datetime) > 20  -- ensure the surgery was longer than 20 minutes
    AND
    vo.visit_start_datetime is not null  --ensure encounter has admit  datetime
    AND
    vo.visit_end_datetime is not null   --ensure encounter has discharge datetime
    AND
    (po.procedure_end_datetime is not null AND po.procedure_datetime is not null )  -- ensure the surgery occurred
    AND
    vo.visit_concept_id IN (SELECT concept_id FROM LoOkUp_ScHeMa.IC3_Variable_Lookup_Table where variable_name = 'inpatient_hospital_encounter'))


SELECT 
    *
FROM
    partitioned 
WHERE
    seq = 1 -- Ensure only the first surgery is captured
