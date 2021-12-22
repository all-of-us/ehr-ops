-- This query checks against the physical measurement coverage in the measurement table
-- This doesn't include physical measurement from enrollment (confirmed)
with
person as (
   select distinct mp.src_hpo_id, mp.person_id
   from (select DISTINCT *  from {{curation_ops_schema}}._mapping_person) mp
   join {{curation_ops_schema}}.unioned_ehr_person p using(person_id)
   ),
visit as (
   select distinct mv.src_hpo_id, v.person_id
   from {{curation_ops_schema}}.unioned_ehr_visit_occurrence v
   join {{curation_ops_schema}}._mapping_visit_occurrence mv using(visit_occurrence_id)
   join {{curation_ops_schema}}._mapping_person p on p.person_id = v.person_id and p.src_hpo_id = mv.src_hpo_id
   ),
site as (
   select distinct
   mp.person_id,
   mp.src_hpo_id
   from {{curation_ops_schema}}._mapping_person mp
),
measurement as (
   select distinct mp.src_hpo_id, m.person_id
   from {{curation_ops_schema}}.unioned_ehr_measurement m
   join {{curation_ops_schema}}._mapping_measurement mp using(measurement_id)
   join {{curation_ops_schema}}._mapping_person p on p.person_id = m.person_id and p.src_hpo_id = mp.src_hpo_id
   ),
height as (
   select distinct mp.src_hpo_id, m.person_id
   from {{curation_ops_schema}}._mapping_person mp
   join {{curation_ops_schema}}.unioned_ehr_measurement m on mp.person_id = m.person_id
   join {{curation_ops_schema}}._mapping_measurement mm on mm.src_hpo_id = mp.src_hpo_id and mm.measurement_id = m.measurement_id
   where m.measurement_concept_id in (3036277,1029031,3019171,3014149,3023540,37020651,3035463,
      3015514,3036798,3013842,3008989,36304231,1003304,37020737,1003232,1003850,4030731)
   ),
weight as (
   select distinct mp.src_hpo_id, m.person_id
    from {{curation_ops_schema}}._mapping_person mp
    join {{curation_ops_schema}}.unioned_ehr_measurement m on mp.person_id = m.person_id
    join {{curation_ops_schema}}._mapping_measurement mm on mm.src_hpo_id = mp.src_hpo_id and mm.measurement_id = m.measurement_id
    where m.measurement_concept_id in (3025315,1029318,3042378,40759214,3013747,40759213,3023166,3013762,
        3011043,3026600,3022484,40759177,36305329,46234683,3013131,1004122,3003176,3013853,3010147,40771968,
        3019336,3005422,3010220,1003206,3009617,3015644,3022281,3011054,21492642,40760186,1003642,40761330,
        40771967,1003088,1004141,1003261,3029459,3028871,3002072,3046286,3046309,3046001,3016341,3028864,
        3034205,3007541,3008900,3012860,3020933,3014618,3015141,3008303,3009664,3014140,3015512,3034737,
        3035618,3036221,3020110,3016054,3000749,3018372,3008484,3033534,3033528,3021838,3001912,3033350,
        3043735,3046620,3008128,3011956,3037845,3018298,3022724,3013673,3005565,3001647,3023704,3017459,
        3004464,3002756,3010914,3026659,43533987,45876173,45876171,3028543,45876172,3027492)
   ),
bmi as (
   select distinct mp.src_hpo_id, m.person_id
   from {{curation_ops_schema}}._mapping_person mp
   join {{curation_ops_schema}}.unioned_ehr_measurement m on mp.person_id = m.person_id
   join {{curation_ops_schema}}._mapping_measurement mm on mm.src_hpo_id = mp.src_hpo_id and mm.measurement_id = m.measurement_id
   where m.measurement_concept_id in (44783982, 40762636, 3038553)
   )
select distinct
site.src_hpo_id,
count(distinct site.person_id) as num_of_participants,
count(distinct visit.person_id) as participants_with_EHR,
count(distinct measurement.person_id) as participants_with_measurement,
count(distinct height.person_id) as participants_with_ehr_height,
case when count(distinct visit.person_id) = 0 then NULL
     else round(count(distinct height.person_id)/count(distinct visit.person_id),2)
     end as height_rate,
count(distinct weight.person_id) as participants_with_ehr_weight,
case when count(distinct visit.person_id) = 0 then NULL
     else round(count(distinct weight.person_id)/count(distinct visit.person_id), 2)
     end as weight_rate,
count(distinct bmi.person_id) as participants_with_ehr_bmi,
case when count(distinct visit.person_id) = 0 then NULL
     else round(count(distinct bmi.person_id)/count(distinct visit.person_id), 2)
     end as bmi_rate,
count(distinct heart_rate.person_id) as participants_with_ehr_heart_rate,
case when count(distinct visit.person_id) = 0 then NULL
     else round(count(distinct heart_rate.person_id)/count(distinct visit.person_id), 2)
     end as heart_rate
from site
left join person on site.person_id = person.person_id and site.src_hpo_id = person.src_hpo_id
left join visit on person.person_id = visit.person_id and person.src_hpo_id = visit.src_hpo_id
left join measurement on person.person_id = measurement.person_id and person.src_hpo_id = measurement.src_hpo_id
left join height on person.person_id = height.person_id and person.src_hpo_id = height.src_hpo_id
left join weight on person.person_id = weight.person_id and person.src_hpo_id = weight.src_hpo_id
left join bmi on person.person_id = bmi.person_id and person.src_hpo_id = bmi.src_hpo_id
left join heart_rate on person.person_id = heart_rate.person_id and person.src_hpo_id = heart_rate.src_hpo_id
group by 1
order by 1