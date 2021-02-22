--- This query is to check GC-2 metric for measurement specification
--- GC-2 metric is checking if the foreign keys in measurement table are valid

WITH
  ttl_counts_measurement_concept AS (
  SELECT
    src_hpo_id,
    COUNT(distinct me.measurement_id) AS total_counts
  FROM
    `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_measurement` AS me
  INNER JOIN
      (select src_hpo_id,
        measurement_id

      from `{{pdr_project}}.{{curation_dataset}}._mapping_measurement`) AS mm
  ON
    me.measurement_id=mm.measurement_id

  GROUP BY
    1 ),
  valid_counts_measurement_concept AS (
  SELECT
    mm.src_hpo_id,
    COUNT(distinct me.measurement_id) AS valid_counts
  FROM(select distinct measurement_id, measurement_concept_id from
    `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_measurement`) AS me
  INNER JOIN
    `{{pdr_project}}.{{curation_dataset}}.concept` AS c
  ON
    me.measurement_concept_id=c.concept_id
  INNER JOIN
      (select src_hpo_id,
        measurement_id

      from `{{pdr_project}}.{{curation_dataset}}._mapping_measurement`) AS mm
  ON
    me.measurement_id=mm.measurement_id
  WHERE
    me.measurement_concept_id != 0

  GROUP BY
    1 ),

  ttl_counts_measurement_person AS (
  SELECT
    src_hpo_id,
    COUNT(distinct me.measurement_id) AS total_counts
  FROM
    `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_measurement` AS me
  INNER JOIN
      (select src_hpo_id,
        measurement_id

      from `{{pdr_project}}.{{curation_dataset}}._mapping_measurement`) AS mm
  ON
    me.measurement_id=mm.measurement_id
  where person_id is not null

  GROUP BY
    1 ),
  valid_counts_measurement_person AS (
  SELECT
    mm.src_hpo_id,
    COUNT(distinct me.measurement_id) AS valid_counts
  FROM
    `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_measurement` AS me
  INNER JOIN(select distinct person_id from
    `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_person`) AS p
  ON
    me.person_id=p.person_id
  INNER JOIN
      (select src_hpo_id,
        measurement_id

      from `{{pdr_project}}.{{curation_dataset}}._mapping_measurement`) AS mm
  ON
    me.measurement_id=mm.measurement_id
  WHERE
    me.person_id != 0

  GROUP BY
    1 ),

  ttl_counts_measurement_type AS (
  SELECT
    src_hpo_id,
    COUNT(distinct me.measurement_id) AS total_counts
  FROM
    `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_measurement` AS me
  INNER JOIN
      (select src_hpo_id,
        measurement_id

      from `{{pdr_project}}.{{curation_dataset}}._mapping_measurement`) AS mm
  ON
    me.measurement_id=mm.measurement_id

  GROUP BY
    1 ),
  valid_counts_measurement_type AS (
  SELECT
    mm.src_hpo_id,
    COUNT(distinct me.measurement_id) AS valid_counts
  FROM(select distinct measurement_id, measurement_type_concept_id from
    `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_measurement`) AS me
  INNER JOIN(select distinct concept_id from
    `{{pdr_project}}.{{curation_dataset}}.concept`) AS c
  ON
    me.measurement_type_concept_id=c.concept_id
  INNER JOIN
      (select src_hpo_id,
        measurement_id

      from `{{pdr_project}}.{{curation_dataset}}._mapping_measurement`) AS mm
  ON
    me.measurement_id=mm.measurement_id
  WHERE
    (me.measurement_type_concept_id!=0)

  GROUP BY
    1 ),

  ttl_counts_measurement_operator AS (
  SELECT
    src_hpo_id,
    COUNT(distinct me.measurement_id) AS total_counts
  FROM
    `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_measurement` AS me
  INNER JOIN
      (select src_hpo_id,
        measurement_id

      from `{{pdr_project}}.{{curation_dataset}}._mapping_measurement`) AS mm
  ON
    me.measurement_id=mm.measurement_id

  GROUP BY
    1 ),
  valid_counts_measurement_operator AS (
  SELECT
    mm.src_hpo_id,
    COUNT(distinct me.measurement_id) AS valid_counts
  FROM(select distinct measurement_id, operator_concept_id from
    `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_measurement`) AS me
  INNER JOIN(select distinct concept_id from
    `{{pdr_project}}.{{curation_dataset}}.concept`) AS c
  ON
    me.operator_concept_id=c.concept_id
  INNER JOIN
      (select src_hpo_id,
        measurement_id

      from `{{pdr_project}}.{{curation_dataset}}._mapping_measurement`) AS mm
  ON
    me.measurement_id=mm.measurement_id
  WHERE
    me.operator_concept_id != 0

  GROUP BY
    1 ),

  ttl_counts_measurement_value_concept AS (
  SELECT
    src_hpo_id,
    COUNT(distinct me.measurement_id) AS total_counts
  FROM
    `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_measurement` AS me
  INNER JOIN
      (select src_hpo_id,
        measurement_id

      from `{{pdr_project}}.{{curation_dataset}}._mapping_measurement`) AS mm
  ON
    me.measurement_id=mm.measurement_id

  GROUP BY
    1 ),
  valid_counts_measurement_value_concept AS (
  SELECT
    mm.src_hpo_id,
    COUNT(distinct me.measurement_id) AS valid_counts
  FROM(select distinct measurement_id, value_as_concept_id from
    `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_measurement`) AS me
  INNER JOIN(select concept_id from
    `{{pdr_project}}.{{curation_dataset}}.concept`) AS c
  ON
    me.value_as_concept_id=c.concept_id
  INNER JOIN
      (select src_hpo_id,
        measurement_id

      from `{{pdr_project}}.{{curation_dataset}}._mapping_measurement`) AS mm
  ON
    me.measurement_id=mm.measurement_id
  WHERE
    me.value_as_concept_id != 0

  GROUP BY
    1 ),

  ttl_counts_measurement_unit AS (
  SELECT
    src_hpo_id,
    COUNT(distinct me.measurement_id) AS total_counts
  FROM
    `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_measurement` AS me
  INNER JOIN
      (select src_hpo_id,
        measurement_id

      from `{{pdr_project}}.{{curation_dataset}}._mapping_measurement`) AS mm
  ON
    me.measurement_id=mm.measurement_id

  GROUP BY
    1 ),
  valid_counts_measurement_unit AS (
  SELECT
    mm.src_hpo_id,
    COUNT(distinct me.measurement_id) AS valid_counts
  FROM(select distinct measurement_id, unit_concept_id from
    `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_measurement`) AS me
  INNER JOIN(select distinct concept_id from
    `{{pdr_project}}.{{curation_dataset}}.concept`) AS c
  ON
    me.unit_concept_id=c.concept_id
  INNER JOIN
      (select src_hpo_id,
        measurement_id

      from `{{pdr_project}}.{{curation_dataset}}._mapping_measurement`) AS mm
  ON
    me.measurement_id=mm.measurement_id
  WHERE
    me.unit_concept_id!=0

  GROUP BY
    1 ),

  ttl_counts_measurement_provider AS (
  SELECT
    src_hpo_id,
    COUNT(distinct me.measurement_id) AS total_counts
  FROM
    `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_measurement` AS me
  INNER JOIN
      (select src_hpo_id,
        measurement_id

      from `{{pdr_project}}.{{curation_dataset}}._mapping_measurement`) AS mm
  ON
    me.measurement_id=mm.measurement_id

  GROUP BY
    1 ),
  valid_counts_measurement_provider AS (
  SELECT
    mm.src_hpo_id,
    COUNT(distinct me.measurement_id) AS valid_counts
  FROM(select distinct measurement_id, provider_id from
    `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_measurement`) AS me
  INNER JOIN(
    select distinct provider_id from
    `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_provider`) AS p
  ON
    me.provider_id=p.provider_id
  INNER JOIN
      (select src_hpo_id,
        measurement_id

      from `{{pdr_project}}.{{curation_dataset}}._mapping_measurement`) AS mm
  ON
    me.measurement_id=mm.measurement_id
  WHERE
    me.provider_id != 0

  GROUP BY
    1 ),

  ttl_counts_measurement_visit AS (
  SELECT
    src_hpo_id,
    COUNT(distinct me.measurement_id) AS total_counts
  FROM
    `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_measurement` AS me
  INNER JOIN
      (select src_hpo_id,
        measurement_id

      from `{{pdr_project}}.{{curation_dataset}}._mapping_measurement`) AS mm
  ON
    me.measurement_id=mm.measurement_id

  GROUP BY
    1 ),
  valid_counts_measurement_visit AS (
  SELECT
    mm.src_hpo_id,
    COUNT(distinct me.measurement_id) AS valid_counts
  FROM(select distinct measurement_id, visit_occurrence_id from
    `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_measurement`) AS me
  INNER JOIN(select distinct visit_occurrence_id from
    `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_visit_occurrence`) AS vo
  ON
    me.visit_occurrence_id=vo.visit_occurrence_id
  INNER JOIN
      (select src_hpo_id,
        measurement_id

      from `{{pdr_project}}.{{curation_dataset}}._mapping_measurement`) AS mm
  ON
    me.measurement_id=mm.measurement_id
  WHERE
    (me.visit_occurrence_id!=0)

  GROUP BY
    1 ),

  ttl_counts_measurement_source AS (
  SELECT
    src_hpo_id,
    COUNT(distinct me.measurement_id) AS total_counts
  FROM
    `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_measurement` AS me
  INNER JOIN
      (select src_hpo_id,
        measurement_id

      from `{{pdr_project}}.{{curation_dataset}}._mapping_measurement`) AS mm
  ON
    me.measurement_id=mm.measurement_id

  GROUP BY
    1 ),
  valid_counts_measurement_source AS (
  SELECT
    mm.src_hpo_id,
    COUNT(distinct me.measurement_id) AS valid_counts
  FROM(select distinct measurement_id, measurement_source_concept_id from
    `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_measurement`) AS me
  INNER JOIN(select distinct concept_id from
    `{{pdr_project}}.{{curation_dataset}}.concept`) AS c
  ON
    me.measurement_source_concept_id=c.concept_id
  INNER JOIN
      (select src_hpo_id,
        measurement_id

      from `{{pdr_project}}.{{curation_dataset}}._mapping_measurement`) AS mm
  ON
    me.measurement_id=mm.measurement_id
  WHERE
    me.measurement_source_concept_id != 0

  GROUP BY
    1 ),

    combined_total_counts AS (
  SELECT * FROM ttl_counts_measurement_concept
  UNION ALL
  SELECT * FROM ttl_counts_measurement_type
  UNION ALL
  SELECT * FROM ttl_counts_measurement_source
  UNION ALL
  SELECT * FROM ttl_counts_measurement_visit
  UNION ALL
  SELECT * FROM ttl_counts_measurement_unit
  UNION ALL
  SELECT * FROM ttl_counts_measurement_value_concept
  UNION ALL
  SELECT * FROM ttl_counts_measurement_person
  UNION ALL
  SELECT * FROM ttl_counts_measurement_operator),

  combined_valid_counts AS (
  SELECT * FROM valid_counts_measurement_concept
  UNION ALL
  SELECT * FROM valid_counts_measurement_type
  UNION ALL
  SELECT * FROM valid_counts_measurement_source
  UNION ALL
  SELECT * FROM valid_counts_measurement_visit
  UNION ALL
  SELECT * FROM valid_counts_measurement_unit
  UNION ALL
  SELECT * FROM valid_counts_measurement_value_concept
  UNION ALL
  SELECT * FROM valid_counts_measurement_person
  UNION ALL
  SELECT * FROM valid_counts_measurement_operator)

SELECT c1.src_hpo_id,
IFNULL(c1.total_counts, 0) as total_counts,
IFNULL(c2.valid_counts, 0) as valid_counts
FROM (
  SELECT src_hpo_id, sum(total_counts) as total_counts
  FROM combined_total_counts
  GROUP BY src_hpo_id) c1

LEFT OUTER JOIN (
SELECT src_hpo_id, sum(valid_counts) as valid_counts
  FROM combined_valid_counts
  GROUP BY src_hpo_id) c2
ON c1.src_hpo_id = c2.src_hpo_id