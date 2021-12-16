--- This query is to check GC-2 metric for device_exposure specification
--- GC-2 metric is checking if the foreign keys in device_exposure table are valid

WITH
  ttl_counts_device_concept AS (
  SELECT
    src_hpo_id,
    COUNT(co.device_exposure_id) AS total_counts
  FROM
    {{curation_ops_schema}}.unioned_ehr_device_exposure AS co
  INNER JOIN
    (select
      src_hpo_id,
      device_exposure_id
    from
      {{curation_ops_schema}}._mapping_device_exposure) AS mco
  ON
    co.device_exposure_id=mco.device_exposure_id

  GROUP BY
    1 ),
  valid_counts_device_concept AS (
  SELECT
    mco.src_hpo_id,
    COUNT(co.device_exposure_id) AS valid_counts
  FROM
    {{curation_ops_schema}}.unioned_ehr_device_exposure AS co
  INNER JOIN
    {{voc_ops_schema}}.concept AS c
  ON
    co.device_concept_id=c.concept_id
  INNER JOIN
    (select
      src_hpo_id,
      device_exposure_id
    from
      {{curation_ops_schema}}._mapping_device_exposure) AS mco
  ON
    co.device_exposure_id=mco.device_exposure_id
  WHERE
    (co.device_concept_id!=0)

  GROUP BY
    1 ),

  ttl_counts_device_visit_occurrence AS (
  SELECT
    src_hpo_id,
    COUNT(co.device_exposure_id) AS total_counts
  FROM
    {{curation_ops_schema}}.unioned_ehr_device_exposure AS co
  INNER JOIN
    (select
      src_hpo_id,
      device_exposure_id
    from
      {{curation_ops_schema}}._mapping_device_exposure) AS mco
  ON
    co.device_exposure_id=mco.device_exposure_id

  GROUP BY
    1 ),
  valid_counts_device_visit_occurrence AS (
  SELECT
    mco.src_hpo_id,
    COUNT(co.device_exposure_id) AS valid_counts
  FROM
    {{curation_ops_schema}}.unioned_ehr_device_exposure AS co
  INNER JOIN(select distinct visit_occurrence_id from
    {{curation_ops_schema}}.unioned_ehr_visit_occurrence) AS vo
  ON
    co.visit_occurrence_id=vo.visit_occurrence_id
  INNER JOIN
    (select
      src_hpo_id,
      device_exposure_id
    from
      {{curation_ops_schema}}._mapping_device_exposure) AS mco
  ON
    co.device_exposure_id=mco.device_exposure_id
  WHERE
    (co.visit_occurrence_id != 0)

  GROUP BY
    1 ),

  ttl_counts_device_person AS (
  SELECT
    src_hpo_id,
    COUNT(co.device_exposure_id) AS total_counts
  FROM
    {{curation_ops_schema}}.unioned_ehr_device_exposure AS co
  INNER JOIN
    (select
      src_hpo_id,
      device_exposure_id
    from
      {{curation_ops_schema}}._mapping_device_exposure) AS mco
  ON
    co.device_exposure_id=mco.device_exposure_id
  where person_id is not null

  GROUP BY
    1 ),
  valid_counts_device_person AS (
  SELECT
    mco.src_hpo_id,
    COUNT(co.device_exposure_id) AS valid_counts
  FROM
    {{curation_ops_schema}}.unioned_ehr_device_exposure AS co
  INNER JOIN(select distinct person_id from
    {{curation_ops_schema}}.unioned_ehr_person) AS p
  ON
    co.person_id=p.person_id
  INNER JOIN
    (select
      src_hpo_id,
      device_exposure_id
    from
      {{curation_ops_schema}}._mapping_device_exposure) AS mco
  ON
    co.device_exposure_id=mco.device_exposure_id
  WHERE
    co.person_id != 0

  GROUP BY
    1 ),

  ttl_counts_device_type AS (
  SELECT
    src_hpo_id,
    COUNT(co.device_exposure_id) AS total_counts
  FROM
    {{curation_ops_schema}}.unioned_ehr_device_exposure AS co
  INNER JOIN
    (select
      src_hpo_id,
      device_exposure_id
    from
      {{curation_ops_schema}}._mapping_device_exposure) AS mco
  ON
    co.device_exposure_id=mco.device_exposure_id

  GROUP BY
    1 ),
  valid_counts_device_type AS (
  SELECT
    mco.src_hpo_id,
    COUNT(co.device_exposure_id) AS valid_counts
  FROM(select device_exposure_id, device_type_concept_id from
    {{curation_ops_schema}}.unioned_ehr_device_exposure) AS co
  INNER JOIN
    {{voc_ops_schema}}.concept AS c
  ON
    co.device_type_concept_id=c.concept_id
  INNER JOIN
    (select
      src_hpo_id,
      device_exposure_id
    from
      {{curation_ops_schema}}._mapping_device_exposure) AS mco
  ON
    co.device_exposure_id=mco.device_exposure_id
  WHERE
    co.device_type_concept_id!= 0

  GROUP BY
    1 ),

  ttl_counts_device_source AS (
  SELECT
    src_hpo_id,
    COUNT(co.device_exposure_id) AS total_counts
  FROM
    {{curation_ops_schema}}.unioned_ehr_device_exposure AS co
  INNER JOIN
    (select
      src_hpo_id,
      device_exposure_id
    from
      {{curation_ops_schema}}._mapping_device_exposure) AS mco
  ON
    co.device_exposure_id=mco.device_exposure_id

  GROUP BY
    1 ),
  valid_counts_device_source AS (
  SELECT
    mco.src_hpo_id,
    COUNT(co.device_exposure_id) AS valid_counts
  FROM(select device_exposure_id, device_source_concept_id from
    {{curation_ops_schema}}.unioned_ehr_device_exposure) AS co
  INNER JOIN
    {{voc_ops_schema}}.concept AS c
  ON
    co.device_source_concept_id=c.concept_id
  INNER JOIN
    (select
      src_hpo_id,
      device_exposure_id
    from
      {{curation_ops_schema}}._mapping_device_exposure) AS mco
  ON
    co.device_exposure_id=mco.device_exposure_id
  WHERE
    co.device_source_concept_id != 0

  GROUP BY
    1 ),

    combined_total_counts AS (
  SELECT * FROM ttl_counts_device_concept
  UNION ALL
  SELECT * FROM ttl_counts_device_type
  UNION ALL
  SELECT * FROM ttl_counts_device_source
  UNION ALL
  SELECT * FROM ttl_counts_device_visit_occurrence
  UNION ALL
  SELECT * FROM ttl_counts_device_person),

  combined_valid_counts AS (
  SELECT * FROM valid_counts_device_concept
  UNION ALL
  SELECT * FROM valid_counts_device_type
  UNION ALL
  SELECT * FROM valid_counts_device_source
  UNION ALL
  SELECT * FROM valid_counts_device_visit_occurrence
  UNION ALL
  SELECT * FROM valid_counts_device_person)

SELECT c1.src_hpo_id,
COALESCE(c1.total_counts, 0) AS total_counts,
COALESCE(c2.valid_counts, 0) AS valid_counts
FROM (
  SELECT src_hpo_id, sum(total_counts) as total_counts
  FROM combined_total_counts
  GROUP BY src_hpo_id) c1

LEFT OUTER JOIN (
SELECT src_hpo_id, sum(valid_counts) as valid_counts
  FROM combined_valid_counts
  GROUP BY src_hpo_id) c2
ON c1.src_hpo_id = c2.src_hpo_id