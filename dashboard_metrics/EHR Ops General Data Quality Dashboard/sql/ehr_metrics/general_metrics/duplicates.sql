-- This query checks against the number of duplicated rows in each table

WITH sites AS (
SELECT DISTINCT src_hpo_id
FROM `{{curation_project}}.{{ehr_ops_dataset}}._mapping_person`),
    condition_agg AS (
      SELECT src_hpo_id, SUM(cnt-1) as condition_dup_cnt
      FROM
        (SELECT src_hpo_id, o.* EXCEPT (condition_occurrence_id), COUNT(*) as cnt
        FROM `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_condition_occurrence` o
        LEFT JOIN `{{curation_project}}.{{ehr_ops_dataset}}._mapping_condition_occurrence` mo
            USING (condition_occurrence_id)
        WHERE
            condition_concept_id != 0 AND condition_concept_id IS NOT NULL
            AND person_id != 0 AND person_id IS NOT NULL
            AND src_condition_occurrence_id != person_id
        GROUP BY
            1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
        HAVING cnt > 1)
        GROUP BY 1),
    procedure_agg AS (
      SELECT src_hpo_id, SUM(cnt-1) as procedure_dup_cnt
      FROM
        (SELECT src_hpo_id, o.* EXCEPT (procedure_occurrence_id), COUNT(*) as cnt
        FROM `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_procedure_occurrence` o
        LEFT JOIN `{{curation_project}}.{{ehr_ops_dataset}}._mapping_procedure_occurrence` mo
            USING (procedure_occurrence_id)
        WHERE
            procedure_concept_id != 0 AND procedure_concept_id IS NOT NULL
            AND person_id != 0 AND person_id IS NOT NULL
            AND src_procedure_occurrence_id != person_id
        GROUP BY
            1,2,3,4,5,6,7,8,9,10,11,12,13,14
        HAVING cnt > 1)
        GROUP BY 1),
    visit_agg AS (
      SELECT src_hpo_id, SUM(cnt-1) as visit_dup_cnt
      FROM
        (SELECT src_hpo_id, o.* EXCEPT (visit_occurrence_id), COUNT(*) as cnt
        FROM `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_visit_occurrence` o
        LEFT JOIN `{{curation_project}}.{{ehr_ops_dataset}}._mapping_visit_occurrence` mo
            USING (visit_occurrence_id)
        WHERE
            visit_concept_id != 0 AND visit_concept_id IS NOT NULL
            AND person_id != 0 AND person_id IS NOT NULL
            AND src_visit_occurrence_id != person_id
        GROUP BY
            1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
        HAVING cnt > 1)
        GROUP BY 1),
    observation_agg AS (
      SELECT src_hpo_id, SUM(cnt-1) as observation_dup_cnt
      FROM
        (SELECT src_hpo_id, o.* EXCEPT (observation_id), COUNT(*) as cnt
        FROM `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_observation` o
        LEFT JOIN `{{curation_project}}.{{ehr_ops_dataset}}._mapping_observation` mo
            USING (observation_id)
        WHERE
            observation_concept_id != 0 AND observation_concept_id IS NOT NULL
            AND person_id != 0 AND person_id IS NOT NULL
            AND src_observation_id != person_id
        GROUP BY
            1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
        HAVING cnt > 1)
        GROUP BY 1),
    measurement_agg AS (
      SELECT src_hpo_id, SUM(cnt-1) as measurement_dup_cnt
      FROM
        (SELECT src_hpo_id, o.* EXCEPT (measurement_id), COUNT(*) as cnt
        FROM `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_measurement` o
        LEFT JOIN `{{curation_project}}.{{ehr_ops_dataset}}._mapping_measurement` mo
            USING (measurement_id)
        WHERE
            measurement_concept_id != 0 AND measurement_concept_id IS NOT NULL
            AND person_id != 0 AND person_id IS NOT NULL
            AND src_measurement_id != person_id
        GROUP BY
            1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
        HAVING cnt > 1)
        GROUP BY 1),
    drug_agg AS (
      SELECT src_hpo_id, SUM(cnt-1) as drug_dup_cnt
      FROM
        (SELECT src_hpo_id, o.* EXCEPT (drug_exposure_id), COUNT(*) as cnt
        FROM `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_drug_exposure` o
        LEFT JOIN `{{curation_project}}.{{ehr_ops_dataset}}._mapping_drug_exposure` mo
            USING (drug_exposure_id)
        WHERE
            drug_concept_id != 0 AND drug_concept_id IS NOT NULL
            AND person_id != 0 AND person_id IS NOT NULL
            AND src_drug_exposure_id != person_id
        GROUP BY
            1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23
        HAVING cnt > 1)
        GROUP BY 1)
SELECT *,
COALESCE(condition_dup_cnt,0)+COALESCE(procedure_dup_cnt,0)+COALESCE(measurement_dup_cnt,0)+COALESCE(observation_dup_cnt,0)+COALESCE(visit_dup_cnt,0)+COALESCE(drug_dup_cnt,0) AS total_dup_cnt
FROM sites
LEFT JOIN condition_agg USING (src_hpo_id)
LEFT JOIN procedure_agg USING (src_hpo_id)
LEFT JOIN visit_agg USING (src_hpo_id)
LEFT JOIN observation_agg USING (src_hpo_id)
LEFT JOIN measurement_agg USING (src_hpo_id)
LEFT JOIN drug_agg USING (src_hpo_id)