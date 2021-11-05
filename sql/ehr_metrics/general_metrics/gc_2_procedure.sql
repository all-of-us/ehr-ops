--- This query is to check GC-2 metric for procedure_occurrence specification
--- GC-2 metric is checking if the foreign keys in procedure_occurrence table are valid

WITH
  ttl_counts_procedure_concept AS (
  SELECT
    src_hpo_id,
    COUNT(distinct t1.procedure_occurrence_id) AS total_counts
  FROM
    `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_procedure_occurrence` AS t1
  INNER JOIN
    (select src_hpo_id,
      procedure_occurrence_id,

      from `{{pdr_project}}.{{curation_dataset}}._mapping_procedure_occurrence`) AS t2
  ON
    t1.procedure_occurrence_id=t2.procedure_occurrence_id

  GROUP BY
    1 ),
  valid_counts_procedure_concept AS (
  SELECT
    t2.src_hpo_id,
    COUNT(distinct t1.procedure_occurrence_id) AS valid_counts
  FROM(select distinct procedure_occurrence_id, procedure_concept_id from
    `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_procedure_occurrence`) AS t1
  INNER JOIN(select distinct concept_id from
    `{{pdr_project}}.{{curation_dataset}}.concept`) AS c
  ON
    t1.procedure_concept_id=c.concept_id
  INNER JOIN
    (select src_hpo_id,
      procedure_occurrence_id,

      from `{{pdr_project}}.{{curation_dataset}}._mapping_procedure_occurrence`) AS t2
  ON
    t1.procedure_occurrence_id=t2.procedure_occurrence_id
  WHERE
    t1.procedure_concept_id != 0

  GROUP BY
    1 ),

  ttl_counts_procedure_person AS (
  SELECT
    src_hpo_id,
    COUNT(distinct t1.procedure_occurrence_id) AS total_counts
  FROM
    `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_procedure_occurrence` AS t1
  INNER JOIN
    (select src_hpo_id,
      procedure_occurrence_id,

      from `{{pdr_project}}.{{curation_dataset}}._mapping_procedure_occurrence`) AS t2
  ON
    t1.procedure_occurrence_id=t2.procedure_occurrence_id
  where person_id is not null

  GROUP BY
    1 ),
  valid_counts_procedure_person AS (
  SELECT
    t2.src_hpo_id,
    COUNT(distinct t1.procedure_occurrence_id) AS valid_counts
  FROM(select distinct procedure_occurrence_id, person_id from
    `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_procedure_occurrence`) AS t1
  INNER JOIN(select distinct person_id from
    `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_person` ) AS p
  ON
    t1.person_id=p.person_id
  INNER JOIN
    (select src_hpo_id,
      procedure_occurrence_id,

      from `{{pdr_project}}.{{curation_dataset}}._mapping_procedure_occurrence`) AS t2
  ON
    t1.procedure_occurrence_id=t2.procedure_occurrence_id
  WHERE
    t1.person_id != 0

  GROUP BY
    1 ),

  ttl_counts_procedure_type AS (
  SELECT
    src_hpo_id,
    COUNT(distinct t1.procedure_occurrence_id) AS total_counts
  FROM
    `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_procedure_occurrence` AS t1
  INNER JOIN
    (select src_hpo_id,
      procedure_occurrence_id,

      from `{{pdr_project}}.{{curation_dataset}}._mapping_procedure_occurrence`) AS t2
  ON
    t1.procedure_occurrence_id=t2.procedure_occurrence_id

  GROUP BY
    1 ),
  valid_counts_procedure_type AS (
  SELECT
    t2.src_hpo_id,
    COUNT(distinct t1.procedure_occurrence_id) AS valid_counts
  FROM(select distinct procedure_occurrence_id, procedure_type_concept_id from
    `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_procedure_occurrence`) AS t1
  INNER JOIN(select distinct concept_id from
    `{{pdr_project}}.{{curation_dataset}}.concept`) AS c
  ON
    t1.procedure_type_concept_id=c.concept_id
  INNER JOIN
    (select src_hpo_id,
      procedure_occurrence_id,

      from `{{pdr_project}}.{{curation_dataset}}._mapping_procedure_occurrence`) AS t2
  ON
    t1.procedure_occurrence_id=t2.procedure_occurrence_id
  WHERE
    t1.procedure_type_concept_id != 0

  GROUP BY
    1 ),

  ttl_counts_procedure_modifier AS (
  SELECT
    src_hpo_id,
    COUNT(distinct t1.procedure_occurrence_id) AS total_counts
  FROM
    `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_procedure_occurrence` AS t1
  INNER JOIN
    (select src_hpo_id,
      procedure_occurrence_id,

      from `{{pdr_project}}.{{curation_dataset}}._mapping_procedure_occurrence`) AS t2
  ON
    t1.procedure_occurrence_id=t2.procedure_occurrence_id

  GROUP BY
    1 ),
  valid_counts_procedure_modifier AS (
  SELECT
    t2.src_hpo_id,
    COUNT(distinct t1.procedure_occurrence_id) AS valid_counts
  FROM(select distinct procedure_occurrence_id, modifier_concept_id from
    `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_procedure_occurrence`) AS t1
  INNER JOIN(select distinct concept_id from
    `{{pdr_project}}.{{curation_dataset}}.concept`) AS c
  ON
    t1.modifier_concept_id=c.concept_id
  INNER JOIN
    (select src_hpo_id,
      procedure_occurrence_id,

      from `{{pdr_project}}.{{curation_dataset}}._mapping_procedure_occurrence`) AS t2
  ON
    t1.procedure_occurrence_id=t2.procedure_occurrence_id
  WHERE
    t1.modifier_concept_id != 0

  GROUP BY
    1 ),

  ttl_counts_procedure_provider AS (
  SELECT
    src_hpo_id,
    COUNT(distinct t1.procedure_occurrence_id) AS total_counts
  FROM
    `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_procedure_occurrence` AS t1
  INNER JOIN
    (select src_hpo_id,
      procedure_occurrence_id,

      from `{{pdr_project}}.{{curation_dataset}}._mapping_procedure_occurrence`) AS t2
  ON
    t1.procedure_occurrence_id=t2.procedure_occurrence_id

  GROUP BY
    1 ),
  valid_counts_procedure_provider AS (
  SELECT
    t2.src_hpo_id,
    COUNT(distinct t1.procedure_occurrence_id) AS valid_counts
  FROM(select distinct procedure_occurrence_id, provider_id from
    `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_procedure_occurrence`) AS t1
  INNER JOIN(select distinct provider_id from
    `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_provider`) AS p
  ON
    t1.provider_id=p.provider_id
  INNER JOIN
    (select src_hpo_id,
      procedure_occurrence_id,

      from `{{pdr_project}}.{{curation_dataset}}._mapping_procedure_occurrence`) AS t2
  ON
    t1.procedure_occurrence_id=t2.procedure_occurrence_id
  WHERE
    t1.provider_id != 0

  GROUP BY
    1 ),

  ttl_counts_procedure_visit AS (
  SELECT
    src_hpo_id,
    COUNT(distinct t1.procedure_occurrence_id) AS total_counts
  FROM
    `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_procedure_occurrence` AS t1
  INNER JOIN
    (select src_hpo_id,
      procedure_occurrence_id,

      from `{{pdr_project}}.{{curation_dataset}}._mapping_procedure_occurrence`) AS t2
  ON
    t1.procedure_occurrence_id=t2.procedure_occurrence_id

  GROUP BY
    1 ),
  valid_counts_procedure_visit AS (
  SELECT
    t2.src_hpo_id,
    COUNT(distinct t1.procedure_occurrence_id) AS valid_counts
  FROM(select distinct procedure_occurrence_id, visit_occurrence_id from
    `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_procedure_occurrence`) AS t1
  INNER JOIN(select distinct visit_occurrence_id from
    `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_visit_occurrence`) AS vo
  ON
    t1.visit_occurrence_id=vo.visit_occurrence_id
  INNER JOIN
    (select src_hpo_id,
      procedure_occurrence_id,

      from `{{pdr_project}}.{{curation_dataset}}._mapping_procedure_occurrence`) AS t2
  ON
    t1.procedure_occurrence_id=t2.procedure_occurrence_id
  WHERE
    t1.visit_occurrence_id != 0

  GROUP BY
    1 ),

  ttl_counts_procedure_source AS (
  SELECT
    src_hpo_id,
    COUNT(t1.procedure_occurrence_id) AS total_counts
  FROM
    `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_procedure_occurrence` AS t1
  INNER JOIN
    (select src_hpo_id,
      procedure_occurrence_id,

      from `{{pdr_project}}.{{curation_dataset}}._mapping_procedure_occurrence`) AS t2
  ON
    t1.procedure_occurrence_id=t2.procedure_occurrence_id

  GROUP BY
    1 ),
  valid_counts_procedure_source AS (
  SELECT
    t2.src_hpo_id,
    COUNT(distinct t1.procedure_occurrence_id) AS valid_counts
  FROM(select distinct procedure_occurrence_id, procedure_source_concept_id from
    `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_procedure_occurrence`) AS t1
  INNER JOIN(select distinct concept_id from
    `{{pdr_project}}.{{curation_dataset}}.concept`) AS c
  ON
    t1.procedure_source_concept_id=c.concept_id
  INNER JOIN
    (select src_hpo_id,
      procedure_occurrence_id,

      from `{{pdr_project}}.{{curation_dataset}}._mapping_procedure_occurrence`) AS t2
  ON
    t1.procedure_occurrence_id=t2.procedure_occurrence_id
  WHERE
    t1.procedure_source_concept_id != 0

  GROUP BY
    1 ),

    combined_total_counts AS (
  SELECT * FROM ttl_counts_procedure_concept
  UNION ALL
  SELECT * FROM ttl_counts_procedure_type
  UNION ALL
  SELECT * FROM ttl_counts_procedure_source
  UNION ALL
  SELECT * FROM ttl_counts_procedure_visit
  UNION ALL
  SELECT * FROM ttl_counts_procedure_modifier
  UNION ALL
  SELECT * FROM ttl_counts_procedure_person),

  combined_valid_counts AS (
  SELECT * FROM valid_counts_procedure_concept
  UNION ALL
  SELECT * FROM valid_counts_procedure_type
  UNION ALL
  SELECT * FROM valid_counts_procedure_source
  UNION ALL
  SELECT * FROM valid_counts_procedure_visit
  UNION ALL
  SELECT * FROM valid_counts_procedure_modifier
  UNION ALL
  SELECT * FROM valid_counts_procedure_person)

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