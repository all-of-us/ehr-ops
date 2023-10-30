WITH person_list AS
 (
 SELECT person_id FROM `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_person` UNION DISTINCT
 SELECT person_id FROM `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_condition_occurrence` UNION DISTINCT
 SELECT person_id FROM `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_measurement` UNION DISTINCT
 SELECT person_id FROM `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_drug_exposure` UNION DISTINCT
 SELECT person_id FROM `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_device_exposure` UNION DISTINCT
 SELECT person_id FROM `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_aou_death` UNION DISTINCT
 SELECT person_id FROM `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_procedure_occurrence` UNION DISTINCT
 SELECT person_id FROM `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_observation` UNION DISTINCT
 SELECT person_id FROM `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_specimen` UNION DISTINCT
 SELECT person_id FROM `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_visit_occurrence`
 ),

person AS
(
SELECT person_id, src_hpo_id, 1 as person_flag FROM `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_person`
JOIN `{{curation_project}}.{{ehr_ops_dataset}}._mapping_person`
USING (person_id)
GROUP BY 1, 2
),

condition AS (
SELECT person_id, src_hpo_id, 1 as condition_flag FROM `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_condition_occurrence`
JOIN `{{curation_project}}.{{ehr_ops_dataset}}._mapping_condition_occurrence`
USING (condition_occurrence_id)
GROUP BY 1, 2
),

drug AS (
SELECT person_id, src_hpo_id, 1 as drug_flag FROM `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_drug_exposure`
JOIN `{{curation_project}}.{{ehr_ops_dataset}}._mapping_drug_exposure`
USING (drug_exposure_id)
GROUP BY 1, 2
),

device AS (
SELECT person_id, src_hpo_id, 1 as device_flag FROM `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_device_exposure`
JOIN `{{curation_project}}.{{ehr_ops_dataset}}._mapping_device_exposure`
USING (device_exposure_id)
GROUP BY 1, 2
),

death AS (
SELECT person_id, src_hpo_id, 1 as death_flag FROM `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_aou_death`
JOIN `{{curation_project}}.{{ehr_ops_dataset}}._mapping_person`
USING (person_id)
GROUP BY 1, 2
),

measurement AS (
SELECT person_id, src_hpo_id, 1 as measurement_flag FROM `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_measurement`
JOIN `{{curation_project}}.{{ehr_ops_dataset}}._mapping_measurement`
USING (measurement_id)
GROUP BY 1, 2
),

procedure AS (
SELECT person_id, src_hpo_id, 1 as procedure_flag FROM `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_procedure_occurrence`
JOIN `{{curation_project}}.{{ehr_ops_dataset}}._mapping_procedure_occurrence`
USING (procedure_occurrence_id)
GROUP BY 1, 2
),

observation AS (
SELECT person_id, src_hpo_id, 1 as observation_flag FROM `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_observation`
JOIN `{{curation_project}}.{{ehr_ops_dataset}}._mapping_observation`
USING (observation_id)
WHERE src_table_id NOT LIKE '%person%'
GROUP BY 1, 2
),

visit AS (
SELECT person_id, src_hpo_id, 1 as visit_flag FROM `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_visit_occurrence`
JOIN `{{curation_project}}.{{ehr_ops_dataset}}._mapping_visit_occurrence`
USING (visit_occurrence_id)
GROUP BY 1, 2
),

specimen AS (
SELECT person_id, src_hpo_id, 1 as specimen_flag FROM `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_specimen`
JOIN `{{curation_project}}.{{ehr_ops_dataset}}._mapping_specimen`
USING (specimen_id)
GROUP BY 1, 2
)

SELECT pl.person_id,
IFNULL(p.person_flag, 0) AS person_flag,
IFNULL(c.condition_flag, 0) AS condition_flag,
IFNULL(de.device_flag, 0) AS device_flag,
IFNULL(dea.death_flag, 0) AS death_flag,
IFNULL(dr.drug_flag, 0) AS drug_flag,
IFNULL(m.measurement_flag, 0) AS measurement_flag,
IFNULL(proc.procedure_flag, 0) AS procedure_flag,
IFNULL(o.observation_flag, 0) AS observation_flag,
IFNULL(v.visit_flag, 0) AS visit_flag,
IFNULL(s.specimen_flag, 0) AS speciment_flag

FROM person_list pl
LEFT JOIN person p
USING (person_id)
LEFT JOIN condition c
USING (person_id)
LEFT JOIN device de
USING (person_id)
LEFT JOIN death dea
USING (person_id)
LEFT JOIN drug dr
USING (person_id)
LEFT JOIN measurement m
USING (person_id)
LEFT JOIN procedure proc
USING (person_id)
LEFT JOIN observation o
USING (person_id)
LEFT JOIN visit v
USING (person_id)
LEFT JOIN specimen s
USING (person_id)