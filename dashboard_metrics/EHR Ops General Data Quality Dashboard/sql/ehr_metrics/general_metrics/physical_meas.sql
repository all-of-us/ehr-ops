-- This query checks against the physical measurement coverage in the measurement table
-- This doesn't include physical measurement from enrollment (confirmed)
with
person as (
select distinct mp.src_hpo_id, mp.person_id
from (select DISTINCT *  from {{curation_project}}.{{ehr_ops_dataset}}._mapping_person) mp
join {{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_person p using(person_id)
),
visit as (
select distinct mv.src_hpo_id, v.person_id
from {{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_visit_occurrence v
join {{curation_project}}.{{ehr_ops_dataset}}._mapping_visit_occurrence mv using(visit_occurrence_id)
join {{curation_project}}.{{ehr_ops_dataset}}._mapping_person p on p.person_id = v.person_id and p.src_hpo_id = mv.src_hpo_id
),
site as (
select distinct
mp.person_id,
mp.src_hpo_id
from {{curation_project}}.{{ehr_ops_dataset}}._mapping_person mp
),
measurement as (
select distinct mp.src_hpo_id, m.person_id
from {{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_measurement m
join {{curation_project}}.{{ehr_ops_dataset}}._mapping_measurement mp using(measurement_id)
join {{curation_project}}.{{ehr_ops_dataset}}._mapping_person p on p.person_id = m.person_id and p.src_hpo_id = mp.src_hpo_id
),
height as (
select distinct mp.src_hpo_id, m.person_id
from {{curation_project}}.{{ehr_ops_dataset}}._mapping_person mp
join {{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_measurement m on mp.person_id = m.person_id
join {{curation_project}}.{{ehr_ops_dataset}}._mapping_measurement mm on mm.src_hpo_id = mp.src_hpo_id and mm.measurement_id = m.measurement_id
where m.measurement_concept_id in (3036277,1029031,3019171,3014149,3023540,37020651,3035463,
3015514,3036798,3013842,3008989,1003304,37020737,1003232,1003850,4030731)
),
weight as (
select distinct mp.src_hpo_id, m.person_id
from {{curation_project}}.{{ehr_ops_dataset}}._mapping_person mp
join {{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_measurement m on mp.person_id = m.person_id
join {{curation_project}}.{{ehr_ops_dataset}}._mapping_measurement mm on mm.src_hpo_id = mp.src_hpo_id and mm.measurement_id = m.measurement_id
where m.measurement_concept_id in (3013853,3015644,3026600,3011043,3023166,40759214,40759213,3019336,46234683,40760186,40771968,3010147,21492642,
1004141,1003261,43533987,40761330,1004122,3010220,3011054,40771967,3013131,3022281,3009617,3005422,3013762,3027492,3021838,3028543,45876172,
1029318,45876173,3025315,45876171,3042378,3010914,3013747,3026659)
),
bmi as (
select distinct mp.src_hpo_id, m.person_id
from {{curation_project}}.{{ehr_ops_dataset}}._mapping_person mp
join {{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_measurement m on mp.person_id = m.person_id
join {{curation_project}}.{{ehr_ops_dataset}}._mapping_measurement mm on mm.src_hpo_id = mp.src_hpo_id and mm.measurement_id = m.measurement_id
where m.measurement_concept_id in (44783982, 40762636, 3038553)
),
heart_rate as (
select distinct mp.src_hpo_id, m.person_id
from {{curation_project}}.{{ehr_ops_dataset}}._mapping_person mp
join {{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_measurement m on mp.person_id = m.person_id
join {{curation_project}}.{{ehr_ops_dataset}}._mapping_measurement mm on mm.src_hpo_id = mp.src_hpo_id and mm.measurement_id = m.measurement_id
where m.measurement_concept_id in (3027018,37069660,3013901,3011020,3015006,3019657,3021659,36303780,40771524,3013786,3040891,
40771525,40771526,3031207,3032300,36303943,3035288,3033692,3031069,36303602,3010413,3005326,3026028,3009881,21490736,3042292,3026867,
3010183,3013100,3015625,3027998,3021545,40758551,3001376,3013883,36305351,36303801,3023766,42527141,1002621,40769110,1002782,
3023233,1003329,43533822,3003841,43533818,21490670,3000276,1002982,40758554,37039497,37039371,1003097,40758555,1003485,
1003869,1003968,40769763,1003181,1003574,1004124,1004003)
),
blood_pressure_total as (
select distinct mp.src_hpo_id, m.person_id
from {{curation_project}}.{{ehr_ops_dataset}}._mapping_person mp
join {{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_measurement m on mp.person_id = m.person_id
join {{curation_project}}.{{ehr_ops_dataset}}._mapping_measurement mm on mm.src_hpo_id = mp.src_hpo_id and mm.measurement_id = m.measurement_id
where m.measurement_concept_id in (3004249, 4152194, 3018586, 21490853, 3028737, 44789315,
3005606, 3009395, 3035856, 4248525, 3018822, 36305059, 4232915, 4292062, 21490674, 3009435, 3012888, 4154790, 3034703,
21490851, 44789316, 3017188, 3019962, 3013940, 4236281, 3018592, 36304326, 4248524, 4268883, 3027598, 3018336, 3012526, 21490675, 3017490, 21490680
)
),
blood_pressure_systolic as (
select distinct mp.src_hpo_id, m.person_id
from {{curation_project}}.{{ehr_ops_dataset}}._mapping_person mp
join {{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_measurement m on mp.person_id = m.person_id
join {{curation_project}}.{{ehr_ops_dataset}}._mapping_measurement mm on mm.src_hpo_id = mp.src_hpo_id and mm.measurement_id = m.measurement_id
where m.measurement_concept_id in (3004249, 4152194, 3018586, 21490853, 3028737, 44789315,
3005606, 3009395, 3035856, 4248525, 3018822, 36305059, 4232915, 4292062, 21490674, 3009435)
),
blood_pressure_diastolic as (
select distinct mp.src_hpo_id, m.person_id
from {{curation_project}}.{{ehr_ops_dataset}}._mapping_person mp
join {{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_measurement m on mp.person_id = m.person_id
join {{curation_project}}.{{ehr_ops_dataset}}._mapping_measurement mm on mm.src_hpo_id = mp.src_hpo_id and mm.measurement_id = m.measurement_id
where m.measurement_concept_id in (3012888, 4154790, 3034703,
21490851, 44789316, 3017188, 3019962, 3013940, 4236281, 3018592, 36304326, 4248524, 4268883
)
)

select distinct
site.src_hpo_id,
count(distinct site.person_id) as num_of_participants,
count(distinct visit.person_id) as participants_with_EHR,
count(distinct measurement.person_id) as participants_with_measurement,
count(distinct height.person_id) as participants_with_ehr_height,
case when count(distinct visit.person_id) = 0 then NULL
else round(count(distinct height.person_id)/count(distinct visit.person_id),2) end as height_rate,
count(distinct weight.person_id) as participants_with_ehr_weight,
case when count(distinct visit.person_id) = 0 then NULL
else round(count(distinct weight.person_id)/count(distinct visit.person_id), 2) end as weight_rate,
count(distinct bmi.person_id) as participants_with_ehr_bmi,
case when count(distinct visit.person_id) = 0 then NULL
else round(count(distinct bmi.person_id)/count(distinct visit.person_id), 2) end as bmi_rate,
count(distinct heart_rate.person_id) as participants_with_ehr_heart_rate,
case when count(distinct visit.person_id) = 0 then NULL
else round(count(distinct heart_rate.person_id)/count(distinct visit.person_id), 2) end as heart_rate,

count(distinct blood_pressure_total.person_id) as participants_with_ehr_blood_pressure_total,
case when count(distinct visit.person_id) = 0 then NULL
else round(count(distinct blood_pressure_total.person_id)/count(distinct visit.person_id), 2) end as bp_total_rate,

count(distinct blood_pressure_systolic.person_id) as participants_with_ehr_bp_systolic,
case when count(distinct visit.person_id) = 0 then NULL
else round(count(distinct blood_pressure_systolic.person_id)/count(distinct visit.person_id), 2) end as bp_systolic_rate,

count(distinct blood_pressure_diastolic.person_id) as participants_with_ehr_bp_diastolic_rate,
case when count(distinct visit.person_id) = 0 then NULL
else round(count(distinct blood_pressure_diastolic.person_id)/count(distinct visit.person_id), 2) end as bp_diastolic_rate


from site
left join person on site.person_id = person.person_id and site.src_hpo_id = person.src_hpo_id
left join visit on person.person_id = visit.person_id and person.src_hpo_id = visit.src_hpo_id
left join measurement on person.person_id = measurement.person_id and person.src_hpo_id = measurement.src_hpo_id
left join height on person.person_id = height.person_id and person.src_hpo_id = height.src_hpo_id
left join weight on person.person_id = weight.person_id and person.src_hpo_id = weight.src_hpo_id
left join bmi on person.person_id = bmi.person_id and person.src_hpo_id = bmi.src_hpo_id
left join heart_rate on person.person_id = heart_rate.person_id and person.src_hpo_id = heart_rate.src_hpo_id
left join blood_pressure_total on person.person_id = blood_pressure_total.person_id and person.src_hpo_id = blood_pressure_total.src_hpo_id
left join blood_pressure_systolic on person.person_id = blood_pressure_systolic.person_id and person.src_hpo_id = blood_pressure_systolic.src_hpo_id
left join blood_pressure_diastolic on person.person_id = blood_pressure_diastolic.person_id and person.src_hpo_id = blood_pressure_diastolic.src_hpo_id
group by 1
order by 1