--- This query is to check GC-2 metric for visit_occurrence specification
--- GC-2 metric is checking if the foreign keys in visit_occurrence table are valid
WITH
  ttl_counts_visit_visit_concept AS (
  SELECT
    src_hpo_id,
    COUNT(vo.visit_occurrence_id) AS total_counts
  FROM
    {{curation_ops_schema}}.unioned_ehr_visit_occurrence AS vo
  INNER JOIN
    (select src_hpo_id,
      visit_occurrence_id
      from {{curation_ops_schema}}._mapping_visit_occurrence) AS mvo
  ON
    vo.visit_occurrence_id=mvo.visit_occurrence_id
  WHERE
    (vo.visit_concept_id IS NOT NULL)

  GROUP BY
    1 ),
  valid_counts_visit_visit_concept AS (
  SELECT
    mvo.src_hpo_id,
    COUNT(vo.visit_occurrence_id) AS valid_counts
  FROM(select visit_occurrence_id, visit_concept_id from
    {{curation_ops_schema}}.unioned_ehr_visit_occurrence) AS vo
  INNER JOIN(select distinct concept_id from
    {{vocab_schema}}.concept) AS c
  ON
    vo.visit_concept_id=c.concept_id
  INNER JOIN
    (select src_hpo_id,
      visit_occurrence_id
      from {{curation_ops_schema}}._mapping_visit_occurrence) AS mvo
  ON
    vo.visit_occurrence_id=mvo.visit_occurrence_id
  WHERE
    (vo.visit_concept_id!=0)

  GROUP BY
    1 ),

  ttl_counts_visit_person AS (
  SELECT
    src_hpo_id,
    COUNT(vo.visit_occurrence_id) AS total_counts
  FROM
    {{curation_ops_schema}}.unioned_ehr_visit_occurrence AS vo
  INNER JOIN
    (select src_hpo_id,
      visit_occurrence_id
      from {{curation_ops_schema}}._mapping_visit_occurrence) AS mvo
  ON
    vo.visit_occurrence_id=mvo.visit_occurrence_id
  WHERE
    (vo.person_id IS NOT NULL)

  GROUP BY
    1 ),
  valid_counts_visit_person AS (
  SELECT
    mvo.src_hpo_id,
    COUNT(vo.visit_occurrence_id) AS valid_counts
  FROM(select visit_occurrence_id, person_id from
    {{curation_ops_schema}}.unioned_ehr_visit_occurrence) AS vo
  INNER JOIN(select distinct person_id from
    {{curation_ops_schema}}.unioned_ehr_person) AS p
  ON
    vo.person_id=p.person_id
  INNER JOIN
    (select src_hpo_id,
      visit_occurrence_id
      from {{curation_ops_schema}}._mapping_visit_occurrence) AS mvo
  ON
    vo.visit_occurrence_id=mvo.visit_occurrence_id
  WHERE
    (vo.person_id != 0)

  GROUP BY
    1 ),


  ttl_counts_visit_visit_type AS (
  SELECT
    src_hpo_id,
    COUNT(vo.visit_occurrence_id) AS total_counts
  FROM
    {{curation_ops_schema}}.unioned_ehr_visit_occurrence AS vo
  INNER JOIN
    (select src_hpo_id,
      visit_occurrence_id
      from {{curation_ops_schema}}._mapping_visit_occurrence) AS mvo
  ON
    vo.visit_occurrence_id=mvo.visit_occurrence_id

  GROUP BY
    1 ),
  valid_counts_visit_visit_type AS (
  SELECT
    mvo.src_hpo_id,
    COUNT(vo.visit_occurrence_id) AS valid_counts
  FROM(select visit_occurrence_id, visit_type_concept_id from
    {{curation_ops_schema}}.unioned_ehr_visit_occurrence) AS vo
  INNER JOIN(select distinct concept_id from
    {{vocab_schema}}.concept) AS c
  ON
    vo.visit_type_concept_id=c.concept_id
  INNER JOIN
    (select src_hpo_id,
      visit_occurrence_id
      from {{curation_ops_schema}}._mapping_visit_occurrence) AS mvo
  ON
    vo.visit_occurrence_id=mvo.visit_occurrence_id
  WHERE
    (vo.visit_type_concept_id!=0)

  GROUP BY
    1 ),

  ttl_counts_visit_visit_source_concept AS (
  SELECT
    src_hpo_id,
    COUNT(vo.visit_occurrence_id) AS total_counts
  FROM
    {{curation_ops_schema}}.unioned_ehr_visit_occurrence AS vo
  INNER JOIN
    (select src_hpo_id,
      visit_occurrence_id
      from {{curation_ops_schema}}._mapping_visit_occurrence) AS mvo
  ON
    vo.visit_occurrence_id=mvo.visit_occurrence_id

  GROUP BY
    1 ),
  valid_counts_visit_visit_source_concept AS (
  SELECT
    mvo.src_hpo_id,
    COUNT(vo.visit_occurrence_id) AS valid_counts
  FROM(select visit_occurrence_id, visit_source_concept_id from
    {{curation_ops_schema}}.unioned_ehr_visit_occurrence) AS vo
  INNER JOIN(select distinct concept_id from
    {{vocab_schema}}.concept) AS c
  ON
    vo.visit_source_concept_id=c.concept_id
  INNER JOIN
    (select src_hpo_id,
      visit_occurrence_id
      from {{curation_ops_schema}}._mapping_visit_occurrence) AS mvo
  ON
    vo.visit_occurrence_id=mvo.visit_occurrence_id
  WHERE
    (vo.visit_source_concept_id!=0)

  GROUP BY
    1 ),

  ttl_counts_visit_admitting AS (
  SELECT
    src_hpo_id,
    COUNT(vo.visit_occurrence_id) AS total_counts
  FROM
    {{curation_ops_schema}}.unioned_ehr_visit_occurrence AS vo
  INNER JOIN
    (select src_hpo_id,
      visit_occurrence_id
      from {{curation_ops_schema}}._mapping_visit_occurrence) AS mvo
  ON
    vo.visit_occurrence_id=mvo.visit_occurrence_id

  GROUP BY
    1 ),
  valid_counts_visit_admitting AS (
  SELECT
    mvo.src_hpo_id,
    COUNT(vo.visit_occurrence_id) AS valid_counts
  FROM(select visit_occurrence_id, admitting_source_concept_id from
    {{curation_ops_schema}}.unioned_ehr_visit_occurrence) AS vo
  INNER JOIN(select concept_id from
    {{vocab_schema}}.concept) AS c
  ON
    vo.admitting_source_concept_id=c.concept_id
  INNER JOIN
    (select src_hpo_id,
      visit_occurrence_id
      from {{curation_ops_schema}}._mapping_visit_occurrence) AS mvo
  ON
    vo.visit_occurrence_id=mvo.visit_occurrence_id
  WHERE
    (vo.admitting_source_concept_id!=0)

  GROUP BY
    1 ),

  ttl_counts_visit_discharge_concept AS (
  SELECT
    src_hpo_id,
    COUNT(vo.visit_occurrence_id) AS total_counts
  FROM
    {{curation_ops_schema}}.unioned_ehr_visit_occurrence AS vo
  INNER JOIN
    (select src_hpo_id,
      visit_occurrence_id
      from {{curation_ops_schema}}._mapping_visit_occurrence) AS mvo
  ON
    vo.visit_occurrence_id=mvo.visit_occurrence_id

  GROUP BY
    1 ),
  valid_counts_visit_discharge_concept AS (
  SELECT
    mvo.src_hpo_id,
    COUNT(vo.visit_occurrence_id) AS valid_counts
  FROM(select visit_occurrence_id, discharge_to_concept_id from
    {{curation_ops_schema}}.unioned_ehr_visit_occurrence) AS vo
  INNER JOIN(select distinct concept_id from
    {{vocab_schema}}.concept) AS c
  ON
    vo.discharge_to_concept_id=c.concept_id
  INNER JOIN
    (select src_hpo_id,
      visit_occurrence_id
      from {{curation_ops_schema}}._mapping_visit_occurrence) AS mvo
  ON
    vo.visit_occurrence_id=mvo.visit_occurrence_id
  WHERE
    (vo.discharge_to_concept_id!=0)

  GROUP BY
    1 ),

  ttl_counts_visit_preceding AS (
  SELECT
    src_hpo_id,
    COUNT(vo.visit_occurrence_id) AS total_counts
  FROM
    {{curation_ops_schema}}.unioned_ehr_visit_occurrence AS vo
  INNER JOIN
    (select src_hpo_id,
      visit_occurrence_id
      from {{curation_ops_schema}}._mapping_visit_occurrence) AS mvo
  ON
    vo.visit_occurrence_id=mvo.visit_occurrence_id

  GROUP BY
    1 ),
  valid_counts_visit_preceding AS (
  SELECT
    mvo.src_hpo_id,
    COUNT(vo.visit_occurrence_id) AS valid_counts
  FROM(select visit_occurrence_id, preceding_visit_occurrence_id from
    {{curation_ops_schema}}.unioned_ehr_visit_occurrence) AS vo
  INNER JOIN
    (select src_hpo_id,
      visit_occurrence_id
      from {{curation_ops_schema}}._mapping_visit_occurrence) AS mvo
  ON
    vo.visit_occurrence_id=mvo.visit_occurrence_id
  WHERE
    (vo.preceding_visit_occurrence_id!=0)

  GROUP BY
    1 ),

  combined_total_counts AS (
  SELECT * FROM ttl_counts_visit_visit_concept
  UNION ALL
  SELECT * FROM ttl_counts_visit_visit_type
  UNION ALL
  SELECT * FROM ttl_counts_visit_visit_source_concept
  UNION ALL
  SELECT * FROM ttl_counts_visit_admitting
  UNION ALL
  SELECT * FROM ttl_counts_visit_discharge_concept
  UNION ALL
  SELECT * FROM ttl_counts_visit_preceding
  UNION ALL
  SELECT * FROM ttl_counts_visit_person),

  combined_valid_counts AS (
  SELECT * FROM valid_counts_visit_visit_concept
  UNION ALL
  SELECT * FROM valid_counts_visit_visit_type
  UNION ALL
  SELECT * FROM valid_counts_visit_visit_source_concept
  UNION ALL
  SELECT * FROM valid_counts_visit_admitting
  UNION ALL
  SELECT * FROM valid_counts_visit_discharge_concept
  UNION ALL
  SELECT * FROM valid_counts_visit_preceding
  UNION ALL
  SELECT * FROM valid_counts_visit_person)

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