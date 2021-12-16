--- This query is to check GC-2 metric for condition specification
--- GC-2 metric is checking if the foreign keys in condition_occurrence table are valid

WITH
  ttl_counts_condition_concept AS (
  SELECT
    src_hpo_id,
    COUNT(co.condition_occurrence_id) AS total_counts
  FROM
    {{curation_ops_schema}}.unioned_ehr_condition_occurrence AS co
  INNER JOIN
  (SELECT
       src_hpo_id,
       condition_occurrence_id
    FROM
      {{curation_ops_schema}}._mapping_condition_occurrence) AS mco
  ON
    co.condition_occurrence_id = mco.condition_occurrence_id

  GROUP BY
    1 ),

  valid_counts_condition_concept AS (
  SELECT
    mco.src_hpo_id,
    count(co.condition_occurrence_id) AS valid_counts
  FROM
  (select condition_occurrence_id, condition_concept_id from
    {{curation_ops_schema}}.unioned_ehr_condition_occurrence) AS co
  inner JOIN
    (select distinct concept_id from
    {{voc_ops_schema}}.concept) AS c
  ON
    co.condition_concept_id=c.concept_id
  INNER JOIN
    (select
      condition_occurrence_id,
      src_hpo_id
    from
    {{curation_ops_schema}}._mapping_condition_occurrence) AS mco
  ON
    co.condition_occurrence_id=mco.condition_occurrence_id
  WHERE
    (co.condition_concept_id != 0)

  GROUP BY
    1),

  ttl_counts_condition_visit_occurrence AS (
  SELECT
    src_hpo_id,
    COUNT(co.condition_occurrence_id) AS total_counts
  FROM
    {{curation_ops_schema}}.unioned_ehr_condition_occurrence AS co
  INNER JOIN
    (select
      condition_occurrence_id,
      src_hpo_id

    from
    {{curation_ops_schema}}._mapping_condition_occurrence) AS mco
  ON
    co.condition_occurrence_id=mco.condition_occurrence_id

  GROUP BY
    1 ),

  valid_counts_condition_visit_occurrence AS (
  SELECT
    mco.src_hpo_id,
    COUNT(co.condition_occurrence_id) AS valid_counts
  FROM
    (select  condition_occurrence_id, visit_occurrence_id from
    {{curation_ops_schema}}.unioned_ehr_condition_occurrence) AS co
  INNER JOIN(
  select  visit_occurrence_id from
    {{curation_ops_schema}}.unioned_ehr_visit_occurrence) AS vo
  ON
    co.visit_occurrence_id=vo.visit_occurrence_id
  INNER JOIN
    (select
      condition_occurrence_id,
      src_hpo_id

    from
    {{curation_ops_schema}}._mapping_condition_occurrence) AS mco
  ON
    co.condition_occurrence_id=mco.condition_occurrence_id
  WHERE
    (co.visit_occurrence_id != 0)
  GROUP BY
    1 ),

  ttl_counts_condition_person AS (
  SELECT
    src_hpo_id,
    COUNT(co.condition_occurrence_id) AS total_counts
  FROM
    {{curation_ops_schema}}.unioned_ehr_condition_occurrence AS co
  INNER JOIN
    (select
      condition_occurrence_id,
      src_hpo_id
    from
    {{curation_ops_schema}}._mapping_condition_occurrence) AS mco
  ON
    co.condition_occurrence_id=mco.condition_occurrence_id
  where  (co.person_id IS NOT NULL)

  GROUP BY
    1 ),
  valid_counts_condition_person AS (
  SELECT
    mco.src_hpo_id,
    COUNT(co.condition_occurrence_id) AS valid_counts
  FROM
    {{curation_ops_schema}}.unioned_ehr_condition_occurrence AS co
  INNER JOIN
  (select  person_id from
    {{curation_ops_schema}}.unioned_ehr_person) AS p
  ON
    co.person_id=p.person_id
  INNER JOIN
    (select
      condition_occurrence_id,
      src_hpo_id
    from
    {{curation_ops_schema}}._mapping_condition_occurrence) AS mco
  ON
    co.condition_occurrence_id=mco.condition_occurrence_id
  WHERE
    co.person_id != 0
  GROUP BY
    1 ),

  ttl_counts_condition_type AS (
  SELECT
    src_hpo_id,
    COUNT(co.condition_occurrence_id) AS total_counts
  FROM
    {{curation_ops_schema}}.unioned_ehr_condition_occurrence AS co
  INNER JOIN
    (select
      condition_occurrence_id,
      src_hpo_id
    from
    {{curation_ops_schema}}._mapping_condition_occurrence) AS mco
  ON
    co.condition_occurrence_id=mco.condition_occurrence_id

  GROUP BY
    1 ),
  valid_counts_condition_type AS (
  SELECT
    mco.src_hpo_id,
    COUNT(co.condition_occurrence_id) AS valid_counts
  FROM
  (select  condition_occurrence_id, condition_type_concept_id from
    {{curation_ops_schema}}.unioned_ehr_condition_occurrence) AS co
  INNER JOIN
  (select distinct concept_id from
    {{voc_ops_schema}}.concept) AS c
  ON
    co.condition_type_concept_id=c.concept_id
  INNER JOIN
    (select
      condition_occurrence_id,
      src_hpo_id
    from
    {{curation_ops_schema}}._mapping_condition_occurrence) AS mco
  ON
    co.condition_occurrence_id=mco.condition_occurrence_id
  WHERE
    co.condition_type_concept_id!= 0

  GROUP BY
    1 ),

  ttl_counts_condition_source AS (
  SELECT
    src_hpo_id,
    COUNT(co.condition_occurrence_id) AS total_counts
  FROM
    {{curation_ops_schema}}.unioned_ehr_condition_occurrence AS co
  INNER JOIN
    (select
      condition_occurrence_id,
      src_hpo_id

    from
    {{curation_ops_schema}}._mapping_condition_occurrence) AS mco
  ON
    co.condition_occurrence_id=mco.condition_occurrence_id

  GROUP BY
    1 ),
  valid_counts_condition_source AS (
  SELECT
    mco.src_hpo_id,
    COUNT(co.condition_occurrence_id) AS valid_counts
  FROM(
  select  condition_occurrence_id, condition_source_concept_id from
    {{curation_ops_schema}}.unioned_ehr_condition_occurrence) AS co
  INNER JOIN(
  select distinct concept_id from
    {{voc_ops_schema}}.concept) AS c
  ON
    co.condition_source_concept_id=c.concept_id
  INNER JOIN
    (select
      condition_occurrence_id,
      src_hpo_id

    from
    {{curation_ops_schema}}._mapping_condition_occurrence) AS mco
  ON
    co.condition_occurrence_id=mco.condition_occurrence_id
  WHERE
    co.condition_source_concept_id != 0

  GROUP BY
    1 ),

  ttl_counts_condition_status AS (
  SELECT
    src_hpo_id,
    COUNT(co.condition_occurrence_id) AS total_counts
  FROM
    {{curation_ops_schema}}.unioned_ehr_condition_occurrence AS co
  INNER JOIN
    (select
      condition_occurrence_id,
      src_hpo_id
    from
    {{curation_ops_schema}}._mapping_condition_occurrence) AS mco
  ON
    co.condition_occurrence_id=mco.condition_occurrence_id

  GROUP BY
    1 ),
  valid_counts_condition_status AS (
  SELECT
    mco.src_hpo_id,
    COUNT(co.condition_occurrence_id) AS valid_counts
  FROM(
  select  condition_occurrence_id, condition_status_concept_id from
    {{curation_ops_schema}}.unioned_ehr_condition_occurrence) AS co
  INNER JOIN(
  select distinct concept_id from
    {{voc_ops_schema}}.concept) AS c
  ON
    co.condition_status_concept_id=c.concept_id
  INNER JOIN
    (select
      condition_occurrence_id,
      src_hpo_id
    from
    {{curation_ops_schema}}._mapping_condition_occurrence) AS mco
  ON
    co.condition_occurrence_id=mco.condition_occurrence_id
  WHERE
    co.condition_status_concept_id != 0
  GROUP BY
    1 ),

    combined_total_counts AS (
  SELECT * FROM ttl_counts_condition_concept
  UNION ALL
  SELECT * FROM ttl_counts_condition_type
  UNION ALL
  SELECT * FROM ttl_counts_condition_source
  UNION ALL
  SELECT * FROM ttl_counts_condition_status
  UNION ALL
  SELECT * FROM ttl_counts_condition_visit_occurrence
  UNION ALL
  SELECT * FROM ttl_counts_condition_person),

  combined_valid_counts AS (
  SELECT * FROM valid_counts_condition_concept
  UNION ALL
  SELECT * FROM valid_counts_condition_type
  UNION ALL
  SELECT * FROM valid_counts_condition_source
  UNION ALL
  SELECT * FROM valid_counts_condition_status
  UNION ALL
  SELECT * FROM valid_counts_condition_visit_occurrence
  UNION ALL
  SELECT * FROM valid_counts_condition_person)

SELECT c1.src_hpo_id,
COALESCE(c1.total_counts, 0) as total_counts,
COALESCE(c2.valid_counts, 0) as valid_counts
FROM (
  SELECT src_hpo_id, sum(total_counts) as total_counts
  FROM combined_total_counts
  GROUP BY src_hpo_id) c1

LEFT OUTER JOIN (
SELECT src_hpo_id, sum(valid_counts) as valid_counts
  FROM combined_valid_counts
  GROUP BY src_hpo_id) c2
ON c1.src_hpo_id = c2.src_hpo_id