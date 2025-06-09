
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = 'parent_visit_occurrence_id') 
with table1 as (SELECT
subject_id,
o.order_placed_datetime,
o.order_discontinued_datetime,
po.procedure_datetime as visit_detail_start_datetime,
po.procedure_end_datetime as visit_detail_end_datetime,
case when o.order_description in (
'DO NOT RESUSCITATE',
'DO NOT RESUSCITATE UF',
'DO NOT RESUSCITATE WITH EXCEPTIONS',
'JX DO NOT RESUSCITATE') then 'dnr_order'
when o.order_description in ('RESCIND DO NOT RESUSCITATE ORDER') then 'dnr_rescind'
when o.order_description in (
'IP CONSULT TO PALLIATIVE CARE JX',
'IP CONSULT TO HOSPICE JX',
'IP CONSULT TO PALLIATIVE AND SUPPORTIVE CARE',
'IP CONSULT TO SOCIAL WORK FOR HOSPICE',
'IP CONSULT TO PEDIATRIC PALLIATIVE AND SUPPORTIVE CARE',
'IP CONSULT TO PEDIATRIC PALLIATIVE CARE JX') then 'palliative_care_order'
when o.order_description in ('COMFORT CARE ONLY') then 'comfort_care_order' end as dnr_order_type

FROM 
ReSuLtS_ScHeMa.COHORT C
INNER JOIN DaTa_ScHeMa.PROCEDURE_OCCURRENCE po on po.procedure_occurrence_id = C.subject_id
INNER JOIN DaTa_ScHeMa.VISIT_OCCURRENCE vop on vop.visit_occurrence_id = po.visit_occurrence_id
INNER JOIN DaTa_ScHeMa.VISIT_OCCURRENCE vo on vop.parent_visit_occurrence_id = vo.visit_occurrence_id
INNER JOIN DaTa_ScHeMa.ORDERS o on (o.person_id = vo.person_id
														AND
														o.order_discontinued_datetime >= vo.parent_visit_start_datetime
														AND
														o.order_placed_datetime <= vo.parent_visit_end_datetime
														and o.order_type in ('DNR'))


WHERE
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY)

SELECT
	subject_id,
	MAX(CASE WHEN dnr_order_type = 'dnr_order' THEN 1 ELSE 0 END) [visit_dnr_order],
	MAX(CASE WHEN dnr_order_type = 'dnr_rescind' THEN 1 ELSE 0 END) [visit_dnr_order_rescinded],
	MAX(CASE WHEN dnr_order_type = 'palliative_care_order' THEN 1 ELSE 0 END) [visit_palliative_care_order],
	MAX(CASE WHEN dnr_order_type = 'comfort_care_order' THEN 1 ELSE 0 END) [visit_comfort_care_order],
	MAX(CASE WHEN dnr_order_type = 'dnr_order' AND order_placed_datetime < visit_detail_start_datetime THEN 1 ELSE 0 END) [dnr_order_placed_before_first_surgery],
	MAX(CASE WHEN dnr_order_type = 'palliative_care_order' AND order_placed_datetime < visit_detail_start_datetime THEN 1 ELSE 0 END) [palliative_care_order_placed_before_first_surgery],
	MAX(CASE WHEN dnr_order_type = 'comfort_care_order' AND order_placed_datetime < visit_detail_start_datetime THEN 1 ELSE 0 END) [comfort_care_order_placed_before_first_surgery],
	MAX(CASE WHEN dnr_order_type = 'dnr_order' AND order_placed_datetime < visit_detail_start_datetime AND order_discontinued_datetime > visit_detail_start_datetime THEN 1 ELSE 0 END) [active_dnr_at_time_of_surgery],
	MIN(CASE WHEN dnr_order_type = 'dnr_rescind' AND order_placed_datetime < visit_detail_start_datetime THEN 1 ELSE 0 END) [first_dnr_order_rescinded_before_surgery]
FROM
	table1
GROUP BY
	subject_id
--HAVING
--	MIN(CASE WHEN dnr_order_type = 'dnr_rescind' AND order_placed_datetime < visit_detail_start_datetime THEN 1 ELSE 0 END) = 1


--a. DNR order placed during admission (0/1)
--b. DNR order rescinded during admission(0/1)
--c. DNR order placed before first surgery (0/1)
--d. Palliative care order placed during admission(0/1)
--e. Comfort care order placed during admission(0/1)
--f. Palliative care order placed before first surgery (0/1)
--g.Comfort care order placed before first surgery (0/1)
--h. DNR order active at the time of first surgery (this means placed before the first surgery and not rescinded before the first surgery)
ELSE
SELECT
	subject_id,
	NULL [visit_dnr_order],
	NULL [visit_dnr_order_rescinded],
	NULL [order placed before first surgery],
	NULL [visit_palliative_care_order],
	NULL [palliative_care_order placed before first surgery],
	NULL [visit_comfort_care_order],
	NULL [comfort_care_order placed before first surgery],
	NULL [active_dnr_at_time_of_surgery],
	NULL [first_dnr_order_rescinded_before_surgery]
FROM
	ReSuLtS_ScHeMa.COHORT C
WHERE
	C.cohort_definition_id = XXXXXX
	AND
	C.subset_id = YYYY
