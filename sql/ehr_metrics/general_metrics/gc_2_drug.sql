--- This query is to check GC-2 metric for drug_exposure specification
--- GC-2 metric is checking if the foreign keys in drug_exposure table are valid

WITH
  ttl_counts_drug_concept AS (
  SELECT
    src_hpo_id,
    COUNT(de.drug_exposure_id) AS total_counts
  FROM
    `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_drug_exposure` AS de
  INNER JOIN
      (select src_hpo_id,
        drug_exposure_id

        from `{{pdr_project}}.{{curation_dataset}}._mapping_drug_exposure`) AS md
  ON
    de.drug_exposure_id=md.drug_exposure_id

  GROUP BY
    1 ),
  valid_counts_drug_concept AS (
  SELECT
    md.src_hpo_id,
    COUNT(de.drug_exposure_id) AS valid_counts
  FROM(select drug_exposure_id, drug_concept_id from
    `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_drug_exposure`) AS de
  INNER JOIN
    `{{pdr_project}}.{{curation_dataset}}.concept` AS c
  ON
    de.drug_concept_id=c.concept_id
  INNER JOIN
      (select src_hpo_id,
        drug_exposure_id

        from `{{pdr_project}}.{{curation_dataset}}._mapping_drug_exposure`) AS md
  ON
    de.drug_exposure_id=md.drug_exposure_id
  WHERE
    de.drug_concept_id != 0

  GROUP BY
    1 ),

  ttl_counts_drug_person AS (
  SELECT
    src_hpo_id,
    COUNT(de.drug_exposure_id) AS total_counts
  FROM
    `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_drug_exposure` AS de
  INNER JOIN
      (select src_hpo_id,
        drug_exposure_id

        from `{{pdr_project}}.{{curation_dataset}}._mapping_drug_exposure`) AS md
  ON
    de.drug_exposure_id=md.drug_exposure_id
  where person_id is not null

  GROUP BY
    1 ),
  valid_counts_drug_person AS (
  SELECT
    md.src_hpo_id,
    COUNT(de.drug_exposure_id) AS valid_counts
  FROM
    `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_drug_exposure` AS de
  INNER JOIN(select distinct person_id from
    `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_person`) AS p
  ON
    de.person_id=p.person_id
  INNER JOIN
      (select src_hpo_id,
        drug_exposure_id

        from `{{pdr_project}}.{{curation_dataset}}._mapping_drug_exposure`) AS md
  ON
    de.drug_exposure_id=md.drug_exposure_id
  WHERE
    de.person_id != 0

  GROUP BY
    1 ),

  ttl_counts_drug_type AS (
  SELECT
    src_hpo_id,
    COUNT(de.drug_exposure_id) AS total_counts
  FROM
    `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_drug_exposure` AS de
  INNER JOIN
      (select src_hpo_id,
        drug_exposure_id

        from `{{pdr_project}}.{{curation_dataset}}._mapping_drug_exposure`) AS md
  ON
    de.drug_exposure_id=md.drug_exposure_id

  GROUP BY
    1 ),
  valid_counts_drug_type AS (
  SELECT
    md.src_hpo_id,
    COUNT(de.drug_exposure_id) AS valid_counts
  FROM(select drug_exposure_id, drug_type_concept_id from
    `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_drug_exposure`) AS de
  INNER JOIN(select distinct concept_id from
    `{{pdr_project}}.{{curation_dataset}}.concept`) AS c
  ON
    de.drug_type_concept_id=c.concept_id
  INNER JOIN
      (select src_hpo_id,
        drug_exposure_id

        from `{{pdr_project}}.{{curation_dataset}}._mapping_drug_exposure`) AS md
  ON
    de.drug_exposure_id=md.drug_exposure_id
  WHERE
    de.drug_type_concept_id!=0 and de.drug_type_concept_id is not null

  GROUP BY
    1 ),

  ttl_counts_drug_route AS (
  SELECT
    src_hpo_id,
    COUNT(de.drug_exposure_id) AS total_counts
  FROM
    `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_drug_exposure` AS de
  INNER JOIN
      (select src_hpo_id,
        drug_exposure_id

        from `{{pdr_project}}.{{curation_dataset}}._mapping_drug_exposure`) AS md
  ON
    de.drug_exposure_id=md.drug_exposure_id

  GROUP BY
    1 ),
  valid_counts_drug_route AS (
  SELECT
    md.src_hpo_id,
    COUNT(de.drug_exposure_id) AS valid_counts
  FROM(select drug_exposure_id, route_concept_id from
    `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_drug_exposure`) AS de
  INNER JOIN(select distinct concept_id from
    `{{pdr_project}}.{{curation_dataset}}.concept`) AS c
  ON
    de.route_concept_id=c.concept_id
  INNER JOIN
      (select src_hpo_id,
        drug_exposure_id

        from `{{pdr_project}}.{{curation_dataset}}._mapping_drug_exposure`) AS md
  ON
    de.drug_exposure_id=md.drug_exposure_id
  WHERE
    de.route_concept_id != 0 and de.route_concept_id is not null

  GROUP BY
    1 ),

  ttl_counts_drug_provider AS (
  SELECT
    src_hpo_id,
    COUNT( de.drug_exposure_id) AS total_counts
  FROM
    `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_drug_exposure` AS de
  INNER JOIN
      (select src_hpo_id,
        drug_exposure_id

        from `{{pdr_project}}.{{curation_dataset}}._mapping_drug_exposure`) AS md
  ON
    de.drug_exposure_id=md.drug_exposure_id

  GROUP BY
    1 ),
  valid_counts_drug_provider AS (
  SELECT
    md.src_hpo_id,
    COUNT( de.drug_exposure_id) AS valid_counts
  FROM(select  drug_exposure_id, provider_id from
    `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_drug_exposure`) AS de
  INNER JOIN(
    select distinct provider_id from
    `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_provider`) AS p
  ON
    de.provider_id=p.provider_id
  INNER JOIN
      (select src_hpo_id,
        drug_exposure_id

        from `{{pdr_project}}.{{curation_dataset}}._mapping_drug_exposure`) AS md
  ON
    de.drug_exposure_id=md.drug_exposure_id
  WHERE
    de.provider_id != 0 and de.provider_id is not null

  GROUP BY
    1 ),

  ttl_counts_drug_visit AS (
  SELECT
    src_hpo_id,
    COUNT( de.drug_exposure_id) AS total_counts
  FROM
    `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_drug_exposure` AS de
  INNER JOIN
      (select src_hpo_id,
        drug_exposure_id

        from `{{pdr_project}}.{{curation_dataset}}._mapping_drug_exposure`) AS md
  ON
    de.drug_exposure_id=md.drug_exposure_id

  GROUP BY
    1 ),
  valid_counts_drug_visit AS (
  SELECT
    md.src_hpo_id,
    COUNT( de.drug_exposure_id) AS valid_counts
  FROM(select  drug_exposure_id, visit_occurrence_id from
    `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_drug_exposure`) AS de
  INNER JOIN(select distinct visit_occurrence_id from
    `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_visit_occurrence`) AS vo
  ON
    de.visit_occurrence_id=vo.visit_occurrence_id
  INNER JOIN
      (select src_hpo_id,
        drug_exposure_id

        from `{{pdr_project}}.{{curation_dataset}}._mapping_drug_exposure`) AS md
  ON
    de.drug_exposure_id=md.drug_exposure_id
  WHERE
    de.visit_occurrence_id!=0 and de.visit_occurrence_id is not null

  GROUP BY
    1 ),

  ttl_counts_drug_source AS (
  SELECT
    src_hpo_id,
    COUNT(de.drug_exposure_id) AS total_counts
  FROM
    `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_drug_exposure` AS de
  INNER JOIN
      (select src_hpo_id,
        drug_exposure_id

        from `{{pdr_project}}.{{curation_dataset}}._mapping_drug_exposure`) AS md
  ON
    de.drug_exposure_id=md.drug_exposure_id

  GROUP BY
    1 ),
  valid_counts_drug_source AS (
  SELECT
    md.src_hpo_id,
    COUNT(de.drug_exposure_id) AS valid_counts
  FROM(select drug_exposure_id, drug_source_concept_id from
    `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_drug_exposure`) AS de
  INNER JOIN(select distinct concept_id from
    `{{pdr_project}}.{{curation_dataset}}.concept`) AS c
  ON
    de.drug_source_concept_id=c.concept_id
  INNER JOIN
      (select src_hpo_id,
        drug_exposure_id

        from `{{pdr_project}}.{{curation_dataset}}._mapping_drug_exposure`) AS md
  ON
    de.drug_exposure_id=md.drug_exposure_id
  WHERE
    de.drug_source_concept_id != 0 and de.drug_source_concept_id is not null

  GROUP BY
    1 ),

    combined_total_counts AS (
  SELECT * FROM ttl_counts_drug_concept
  UNION ALL
  SELECT * FROM ttl_counts_drug_type
  UNION ALL
  SELECT * FROM ttl_counts_drug_source
  UNION ALL
  SELECT * FROM ttl_counts_drug_visit
  UNION ALL
  SELECT * FROM ttl_counts_drug_route
  UNION ALL
  SELECT * FROM ttl_counts_drug_person),

  combined_valid_counts AS (
  SELECT * FROM valid_counts_drug_concept
  UNION ALL
  SELECT * FROM valid_counts_drug_type
  UNION ALL
  SELECT * FROM valid_counts_drug_source
  UNION ALL
  SELECT * FROM valid_counts_drug_visit
  UNION ALL
  SELECT * FROM valid_counts_drug_route
  UNION ALL
  SELECT * FROM valid_counts_drug_person)

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