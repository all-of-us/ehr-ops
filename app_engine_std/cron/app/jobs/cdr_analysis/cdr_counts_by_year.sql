
WITH 
  condition_total_year AS (
    SELECT EXTRACT(YEAR FROM condition_start_date) as year, src_hpo_id, count(1) as condition_year_row_count
    FROM `{{curation_project}}.{{unioned_ehr_dataset}}.condition_occurrence` as t1
    INNER JOIN (
      SELECT src_hpo_id, condition_occurrence_id
      FROM `{{curation_project}}.{{unioned_ehr_dataset}}._mapping_condition_occurrence` 
    ) as t2
    ON t1.condition_occurrence_id=t2.condition_occurrence_id
    GROUP BY year, src_hpo_id
  ),
  procedure_total_year AS (
    SELECT EXTRACT(YEAR FROM procedure_date) as year, src_hpo_id, count(1) as procedure_year_row_count
    FROM `{{curation_project}}.{{unioned_ehr_dataset}}.procedure_occurrence` as t1
    INNER JOIN (
      SELECT src_hpo_id, procedure_occurrence_id
      FROM `{{curation_project}}.{{unioned_ehr_dataset}}._mapping_procedure_occurrence` 
    ) as t2
    ON t1.procedure_occurrence_id=t2.procedure_occurrence_id
    GROUP BY year, src_hpo_id
  ),

  visit_occurrence_total_year AS (
    SELECT EXTRACT(YEAR FROM visit_start_datetime) as year, src_hpo_id, count(1) as visit_year_row_count
    FROM `{{curation_project}}.{{unioned_ehr_dataset}}.visit_occurrence` as t1
    INNER JOIN (
      SELECT src_hpo_id, visit_occurrence_id
      FROM `{{curation_project}}.{{unioned_ehr_dataset}}._mapping_visit_occurrence` 
    ) as t2
    ON t1.visit_occurrence_id=t2.visit_occurrence_id
    GROUP BY year, src_hpo_id
  ),

  observation_total_year AS (
    SELECT EXTRACT(YEAR FROM observation_date) as year, src_hpo_id, count(1) as observation_row_count
    FROM `{{curation_project}}.{{unioned_ehr_dataset}}.observation` as t1
    INNER JOIN (
      SELECT src_hpo_id, observation_id
      FROM `{{curation_project}}.{{unioned_ehr_dataset}}._mapping_observation` 
    ) as t2
    ON t1.observation_id=t2.observation_id
    GROUP BY year, src_hpo_id

  ),

  drug_total_year AS (
    SELECT EXTRACT(YEAR FROM drug_exposure_start_date) as year, src_hpo_id, count(1) as drug_year_row_count
    FROM `{{curation_project}}.{{unioned_ehr_dataset}}.drug_exposure` as t1
    INNER JOIN (
      SELECT src_hpo_id, drug_exposure_id
     FROM `{{curation_project}}.{{unioned_ehr_dataset}}._mapping_drug_exposure` 
   ) as t2
   ON t1.drug_exposure_id=t2.drug_exposure_id
   GROUP BY year, src_hpo_id
 ),

  measurement_total_year AS (
    SELECT EXTRACT(YEAR FROM measurement_date) as year, src_hpo_id, count(1) as meas_year_row_count
    FROM `{{curation_project}}.{{unioned_ehr_dataset}}.measurement` as t1
    INNER JOIN (
      SELECT src_hpo_id, measurement_id 
      FROM `{{curation_project}}.{{unioned_ehr_dataset}}._mapping_measurement` 
    ) as t2
    ON t1.measurement_id=t2.measurement_id
    GROUP BY year, src_hpo_id
  ),

year_unioned_ehr AS (
  SELECT DISTINCT year, src_hpo_id FROM condition_total_year
  UNION DISTINCT
  SELECT DISTINCT year, src_hpo_id FROM procedure_total_year
  UNION DISTINCT
  SELECT DISTINCT year, src_hpo_id FROM visit_occurrence_total_year
  UNION DISTINCT
  SELECT DISTINCT year, src_hpo_id FROM observation_total_year
  UNION DISTINCT 
  SELECT DISTINCT year, src_hpo_id FROM drug_total_year
  UNION DISTINCT
  SELECT DISTINCT year, src_hpo_id FROM measurement_total_year
)

SELECT 
{{cdr_version}} as cdr_version,
yue.year, 
yue.src_hpo_id, 
COALESCE(condition_year_row_count, 0) AS condition_year_row_count,
COALESCE(procedure_year_row_count, 0) AS procedure_year_row_count,
COALESCE(visit_year_row_count, 0) AS visit_year_row_count,
COALESCE(observation_row_count, 0) AS observation_row_count,
COALESCE(drug_year_row_count, 0) AS drug_year_row_count,
COALESCE(meas_year_row_count, 0) AS meas_year_row_count
FROM year_unioned_ehr yue
LEFT JOIN condition_total_year cy ON yue.year = cy.year AND yue.src_hpo_id = cy.src_hpo_id
LEFT JOIN procedure_total_year py ON yue.year = py.year AND yue.src_hpo_id = py.src_hpo_id
LEFT JOIN visit_occurrence_total_year vy ON yue.year = vy.year AND yue.src_hpo_id = vy.src_hpo_id
LEFT JOIN observation_total_year oy ON yue.year = oy.year AND yue.src_hpo_id = oy.src_hpo_id
LEFT JOIN drug_total_year dy ON yue.year = dy.year AND yue.src_hpo_id = dy.src_hpo_id
LEFT JOIN measurement_total_year my ON yue.year = my.year AND yue.src_hpo_id = my.src_hpo_id
order by yue.year, yue.src_hpo_id


