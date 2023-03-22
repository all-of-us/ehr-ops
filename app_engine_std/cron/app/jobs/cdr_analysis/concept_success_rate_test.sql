
  SELECT
    src_hpo_id,
    COUNT( t1.condition_occurrence_id) AS total_row
  FROM
    `{{curation_project}}.{{unioned_ehr_dataset}}.condition_occurrence` AS t1
  INNER JOIN (
    SELECT
       src_hpo_id, condition_occurrence_id
    FROM
      `{{curation_project}}.{{unioned_ehr_dataset}}._mapping_condition_occurrence`) AS t2
  ON
    t1.condition_occurrence_id=t2.condition_occurrence_id
  GROUP BY
    1