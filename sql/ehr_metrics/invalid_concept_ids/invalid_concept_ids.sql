SELECT
  'visit_occurrence' AS table_name,
  'visit_concept_id' AS column_name,
  m.src_hpo_id AS src_hpo_id,
  visit_concept_id AS concept_id,
  CASE
    WHEN c.concept_id IS NULL THEN 'concept_not_exist'
    WHEN c.concept_id IS NOT NULL
  AND c.standard_concept != 'S' THEN 'concept_not_standard'
END
  AS invalid_reason,
  COUNT(1) AS row_count
FROM
  {{curation_ops_schema}}.unioned_ehr_visit_occurrence t
JOIN
  {{curation_ops_schema}}._mapping_visit_occurrence m
USING
  (visit_occurrence_id)
LEFT JOIN
  {{vocab_schema}}.concept c
ON
  visit_concept_id = c.concept_id
WHERE
  (c.concept_id IS NULL
    OR c.standard_concept != 'S')
  AND visit_concept_id != 0
GROUP BY
  1,
  2,
  3,
  4,
  5
UNION ALL
SELECT
  'visit_occurrence' AS table_name,
  'visit_type_concept_id' AS column_name,
  m.src_hpo_id AS src_hpo_id,
  visit_type_concept_id AS concept_id,
  CASE
    WHEN c.concept_id IS NULL THEN 'concept_not_exist'
    WHEN c.concept_id IS NOT NULL
  AND c.standard_concept != 'S' THEN 'concept_not_standard'
END
  AS invalid_reason,
  COUNT(1) AS row_count
FROM
  {{curation_ops_schema}}.unioned_ehr_visit_occurrence t
JOIN
  {{curation_ops_schema}}._mapping_visit_occurrence m
USING
  (visit_occurrence_id)
LEFT JOIN
  {{vocab_schema}}.concept c
ON
  visit_type_concept_id = c.concept_id
WHERE
  (c.concept_id IS NULL
    OR c.standard_concept != 'S')
  AND visit_type_concept_id != 0
GROUP BY
  1,
  2,
  3,
  4,
  5
UNION ALL
SELECT
  'visit_occurrence' AS table_name,
  'discharge_to_concept_id' AS column_name,
  m.src_hpo_id AS src_hpo_id,
  discharge_to_concept_id AS concept_id,
  CASE
    WHEN c.concept_id IS NULL THEN 'concept_not_exist'
    WHEN c.concept_id IS NOT NULL
  AND c.standard_concept != 'S' THEN 'concept_not_standard'
END
  AS invalid_reason,
  COUNT(1) AS row_count
FROM
  {{curation_ops_schema}}.unioned_ehr_visit_occurrence t
JOIN
  {{curation_ops_schema}}._mapping_visit_occurrence m
USING
  (visit_occurrence_id)
LEFT JOIN
  {{vocab_schema}}.concept c
ON
  discharge_to_concept_id = c.concept_id
WHERE
  (c.concept_id IS NULL
    OR c.standard_concept != 'S')
  AND discharge_to_concept_id != 0
GROUP BY
  1,
  2,
  3,
  4,
  5
UNION ALL
SELECT
  'person' AS table_name,
  'gender_concept_id' AS column_name,
  m.src_hpo_id AS src_hpo_id,
  gender_concept_id AS concept_id,
  CASE
    WHEN c.concept_id IS NULL THEN 'concept_not_exist'
    WHEN c.concept_id IS NOT NULL
  AND c.standard_concept != 'S' THEN 'concept_not_standard'
END
  AS invalid_reason,
  COUNT(1) AS row_count
FROM
  {{curation_ops_schema}}.unioned_ehr_person t
JOIN
  {{curation_ops_schema}}._mapping_person m
USING
  (person_id)
LEFT JOIN
  {{vocab_schema}}.concept c
ON
  gender_concept_id = c.concept_id
WHERE
  (c.concept_id IS NULL
    OR c.standard_concept != 'S')
  AND gender_concept_id != 0
GROUP BY
  1,
  2,
  3,
  4,
  5
UNION ALL
SELECT
  'person' AS table_name,
  'race_concept_id' AS column_name,
  m.src_hpo_id AS src_hpo_id,
  race_concept_id AS concept_id,
  CASE
    WHEN c.concept_id IS NULL THEN 'concept_not_exist'
    WHEN c.concept_id IS NOT NULL
  AND c.standard_concept != 'S' THEN 'concept_not_standard'
END
  AS invalid_reason,
  COUNT(1) AS row_count
FROM
  {{curation_ops_schema}}.unioned_ehr_person t
JOIN
  {{curation_ops_schema}}._mapping_person m
USING
  (person_id)
LEFT JOIN
  {{vocab_schema}}.concept c
ON
  race_concept_id = c.concept_id
WHERE
  (c.concept_id IS NULL
    OR c.standard_concept != 'S')
  AND race_concept_id != 0
GROUP BY
  1,
  2,
  3,
  4,
  5
UNION ALL
SELECT
  'person' AS table_name,
  'ethnicity_concept_id' AS column_name,
  m.src_hpo_id AS src_hpo_id,
  ethnicity_concept_id AS concept_id,
  CASE
    WHEN c.concept_id IS NULL THEN 'concept_not_exist'
    WHEN c.concept_id IS NOT NULL
  AND c.standard_concept != 'S' THEN 'concept_not_standard'
END
  AS invalid_reason,
  COUNT(1) AS row_count
FROM
  {{curation_ops_schema}}.unioned_ehr_person t
JOIN
  {{curation_ops_schema}}._mapping_person m
USING
  (person_id)
LEFT JOIN
  {{vocab_schema}}.concept c
ON
  ethnicity_concept_id = c.concept_id
WHERE
  (c.concept_id IS NULL
    OR c.standard_concept != 'S')
  AND ethnicity_concept_id != 0
GROUP BY
  1,
  2,
  3,
  4,
  5
UNION ALL
SELECT
  'device_exposure' AS table_name,
  'device_concept_id' AS column_name,
  m.src_hpo_id AS src_hpo_id,
  device_concept_id AS concept_id,
  CASE
    WHEN c.concept_id IS NULL THEN 'concept_not_exist'
    WHEN c.concept_id IS NOT NULL
  AND c.standard_concept != 'S' THEN 'concept_not_standard'
END
  AS invalid_reason,
  COUNT(1) AS row_count
FROM
  {{curation_ops_schema}}.unioned_ehr_device_exposure t
JOIN
  {{curation_ops_schema}}._mapping_device_exposure m
USING
  (device_exposure_id)
LEFT JOIN
  {{vocab_schema}}.concept c
ON
  device_concept_id = c.concept_id
WHERE
  (c.concept_id IS NULL
    OR c.standard_concept != 'S')
  AND device_concept_id != 0
GROUP BY
  1,
  2,
  3,
  4,
  5
UNION ALL
SELECT
  'device_exposure' AS table_name,
  'device_type_concept_id' AS column_name,
  m.src_hpo_id AS src_hpo_id,
  device_type_concept_id AS concept_id,
  CASE
    WHEN c.concept_id IS NULL THEN 'concept_not_exist'
    WHEN c.concept_id IS NOT NULL
  AND c.standard_concept != 'S' THEN 'concept_not_standard'
END
  AS invalid_reason,
  COUNT(1) AS row_count
FROM
  {{curation_ops_schema}}.unioned_ehr_device_exposure t
JOIN
  {{curation_ops_schema}}._mapping_device_exposure m
USING
  (device_exposure_id)
LEFT JOIN
  {{vocab_schema}}.concept c
ON
  device_type_concept_id = c.concept_id
WHERE
  (c.concept_id IS NULL
    OR c.standard_concept != 'S')
  AND device_type_concept_id != 0
GROUP BY
  1,
  2,
  3,
  4,
  5
UNION ALL
SELECT
  'specimen' AS table_name,
  'specimen_concept_id' AS column_name,
  m.src_hpo_id AS src_hpo_id,
  specimen_concept_id AS concept_id,
  CASE
    WHEN c.concept_id IS NULL THEN 'concept_not_exist'
    WHEN c.concept_id IS NOT NULL
  AND c.standard_concept != 'S' THEN 'concept_not_standard'
END
  AS invalid_reason,
  COUNT(1) AS row_count
FROM
  {{curation_ops_schema}}.unioned_ehr_specimen t
JOIN
  {{curation_ops_schema}}._mapping_specimen m
USING
  (specimen_id)
LEFT JOIN
  {{vocab_schema}}.concept c
ON
  specimen_concept_id = c.concept_id
WHERE
  (c.concept_id IS NULL
    OR c.standard_concept != 'S')
  AND specimen_concept_id != 0
GROUP BY
  1,
  2,
  3,
  4,
  5
UNION ALL
SELECT
  'specimen' AS table_name,
  'specimen_type_concept_id' AS column_name,
  m.src_hpo_id AS src_hpo_id,
  specimen_type_concept_id AS concept_id,
  CASE
    WHEN c.concept_id IS NULL THEN 'concept_not_exist'
    WHEN c.concept_id IS NOT NULL
  AND c.standard_concept != 'S' THEN 'concept_not_standard'
END
  AS invalid_reason,
  COUNT(1) AS row_count
FROM
  {{curation_ops_schema}}.unioned_ehr_specimen t
JOIN
  {{curation_ops_schema}}._mapping_specimen m
USING
  (specimen_id)
LEFT JOIN
  {{vocab_schema}}.concept c
ON
  specimen_type_concept_id = c.concept_id
WHERE
  (c.concept_id IS NULL
    OR c.standard_concept != 'S')
  AND specimen_type_concept_id != 0
GROUP BY
  1,
  2,
  3,
  4,
  5
UNION ALL
SELECT
  'specimen' AS table_name,
  'unit_concept_id' AS column_name,
  m.src_hpo_id AS src_hpo_id,
  unit_concept_id AS concept_id,
  CASE
    WHEN c.concept_id IS NULL THEN 'concept_not_exist'
    WHEN c.concept_id IS NOT NULL
  AND c.standard_concept != 'S' THEN 'concept_not_standard'
END
  AS invalid_reason,
  COUNT(1) AS row_count
FROM
  {{curation_ops_schema}}.unioned_ehr_specimen t
JOIN
  {{curation_ops_schema}}._mapping_specimen m
USING
  (specimen_id)
LEFT JOIN
  {{vocab_schema}}.concept c
ON
  unit_concept_id = c.concept_id
WHERE
  (c.concept_id IS NULL
    OR c.standard_concept != 'S')
  AND unit_concept_id != 0
GROUP BY
  1,
  2,
  3,
  4,
  5
UNION ALL
SELECT
  'specimen' AS table_name,
  'anatomic_site_concept_id' AS column_name,
  m.src_hpo_id AS src_hpo_id,
  anatomic_site_concept_id AS concept_id,
  CASE
    WHEN c.concept_id IS NULL THEN 'concept_not_exist'
    WHEN c.concept_id IS NOT NULL
  AND c.standard_concept != 'S' THEN 'concept_not_standard'
END
  AS invalid_reason,
  COUNT(1) AS row_count
FROM
  {{curation_ops_schema}}.unioned_ehr_specimen t
JOIN
  {{curation_ops_schema}}._mapping_specimen m
USING
  (specimen_id)
LEFT JOIN
  {{vocab_schema}}.concept c
ON
  anatomic_site_concept_id = c.concept_id
WHERE
  (c.concept_id IS NULL
    OR c.standard_concept != 'S')
  AND anatomic_site_concept_id != 0
GROUP BY
  1,
  2,
  3,
  4,
  5
UNION ALL
SELECT
  'specimen' AS table_name,
  'disease_status_concept_id' AS column_name,
  m.src_hpo_id AS src_hpo_id,
  disease_status_concept_id AS concept_id,
  CASE
    WHEN c.concept_id IS NULL THEN 'concept_not_exist'
    WHEN c.concept_id IS NOT NULL
  AND c.standard_concept != 'S' THEN 'concept_not_standard'
END
  AS invalid_reason,
  COUNT(1) AS row_count
FROM
  {{curation_ops_schema}}.unioned_ehr_specimen t
JOIN
  {{curation_ops_schema}}._mapping_specimen m
USING
  (specimen_id)
LEFT JOIN
  {{vocab_schema}}.concept c
ON
  disease_status_concept_id = c.concept_id
WHERE
  (c.concept_id IS NULL
    OR c.standard_concept != 'S')
  AND disease_status_concept_id != 0
GROUP BY
  1,
  2,
  3,
  4,
  5
UNION ALL
SELECT
  'procedure_occurrence' AS table_name,
  'procedure_concept_id' AS column_name,
  m.src_hpo_id AS src_hpo_id,
  procedure_concept_id AS concept_id,
  CASE
    WHEN c.concept_id IS NULL THEN 'concept_not_exist'
    WHEN c.concept_id IS NOT NULL
  AND c.standard_concept != 'S' THEN 'concept_not_standard'
END
  AS invalid_reason,
  COUNT(1) AS row_count
FROM
  {{curation_ops_schema}}.unioned_ehr_procedure_occurrence t
JOIN
  {{curation_ops_schema}}._mapping_procedure_occurrence m
USING
  (procedure_occurrence_id)
LEFT JOIN
  {{vocab_schema}}.concept c
ON
  procedure_concept_id = c.concept_id
WHERE
  (c.concept_id IS NULL
    OR c.standard_concept != 'S')
  AND procedure_concept_id != 0
GROUP BY
  1,
  2,
  3,
  4,
  5
UNION ALL
SELECT
  'procedure_occurrence' AS table_name,
  'procedure_type_concept_id' AS column_name,
  m.src_hpo_id AS src_hpo_id,
  procedure_type_concept_id AS concept_id,
  CASE
    WHEN c.concept_id IS NULL THEN 'concept_not_exist'
    WHEN c.concept_id IS NOT NULL
  AND c.standard_concept != 'S' THEN 'concept_not_standard'
END
  AS invalid_reason,
  COUNT(1) AS row_count
FROM
  {{curation_ops_schema}}.unioned_ehr_procedure_occurrence t
JOIN
  {{curation_ops_schema}}._mapping_procedure_occurrence m
USING
  (procedure_occurrence_id)
LEFT JOIN
  {{vocab_schema}}.concept c
ON
  procedure_type_concept_id = c.concept_id
WHERE
  (c.concept_id IS NULL
    OR c.standard_concept != 'S')
  AND procedure_type_concept_id != 0
GROUP BY
  1,
  2,
  3,
  4,
  5
UNION ALL
SELECT
  'procedure_occurrence' AS table_name,
  'modifier_concept_id' AS column_name,
  m.src_hpo_id AS src_hpo_id,
  modifier_concept_id AS concept_id,
  CASE
    WHEN c.concept_id IS NULL THEN 'concept_not_exist'
    WHEN c.concept_id IS NOT NULL
  AND c.standard_concept != 'S' THEN 'concept_not_standard'
END
  AS invalid_reason,
  COUNT(1) AS row_count
FROM
  {{curation_ops_schema}}.unioned_ehr_procedure_occurrence t
JOIN
  {{curation_ops_schema}}._mapping_procedure_occurrence m
USING
  (procedure_occurrence_id)
LEFT JOIN
  {{vocab_schema}}.concept c
ON
  modifier_concept_id = c.concept_id
WHERE
  (c.concept_id IS NULL
    OR c.standard_concept != 'S')
  AND modifier_concept_id != 0
GROUP BY
  1,
  2,
  3,
  4,
  5
UNION ALL
SELECT
  'drug_exposure' AS table_name,
  'drug_concept_id' AS column_name,
  m.src_hpo_id AS src_hpo_id,
  drug_concept_id AS concept_id,
  CASE
    WHEN c.concept_id IS NULL THEN 'concept_not_exist'
    WHEN c.concept_id IS NOT NULL
  AND c.standard_concept != 'S' THEN 'concept_not_standard'
END
  AS invalid_reason,
  COUNT(1) AS row_count
FROM
  {{curation_ops_schema}}.unioned_ehr_drug_exposure t
JOIN
  {{curation_ops_schema}}._mapping_drug_exposure m
USING
  (drug_exposure_id)
LEFT JOIN
  {{vocab_schema}}.concept c
ON
  drug_concept_id = c.concept_id
WHERE
  (c.concept_id IS NULL
    OR c.standard_concept != 'S')
  AND drug_concept_id != 0
GROUP BY
  1,
  2,
  3,
  4,
  5
UNION ALL
SELECT
  'drug_exposure' AS table_name,
  'drug_type_concept_id' AS column_name,
  m.src_hpo_id AS src_hpo_id,
  drug_type_concept_id AS concept_id,
  CASE
    WHEN c.concept_id IS NULL THEN 'concept_not_exist'
    WHEN c.concept_id IS NOT NULL
  AND c.standard_concept != 'S' THEN 'concept_not_standard'
END
  AS invalid_reason,
  COUNT(1) AS row_count
FROM
  {{curation_ops_schema}}.unioned_ehr_drug_exposure t
JOIN
  {{curation_ops_schema}}._mapping_drug_exposure m
USING
  (drug_exposure_id)
LEFT JOIN
  {{vocab_schema}}.concept c
ON
  drug_type_concept_id = c.concept_id
WHERE
  (c.concept_id IS NULL
    OR c.standard_concept != 'S')
  AND drug_type_concept_id != 0
GROUP BY
  1,
  2,
  3,
  4,
  5
UNION ALL
SELECT
  'drug_exposure' AS table_name,
  'route_concept_id' AS column_name,
  m.src_hpo_id AS src_hpo_id,
  route_concept_id AS concept_id,
  CASE
    WHEN c.concept_id IS NULL THEN 'concept_not_exist'
    WHEN c.concept_id IS NOT NULL
  AND c.standard_concept != 'S' THEN 'concept_not_standard'
END
  AS invalid_reason,
  COUNT(1) AS row_count
FROM
  {{curation_ops_schema}}.unioned_ehr_drug_exposure t
JOIN
  {{curation_ops_schema}}._mapping_drug_exposure m
USING
  (drug_exposure_id)
LEFT JOIN
  {{vocab_schema}}.concept c
ON
  route_concept_id = c.concept_id
WHERE
  (c.concept_id IS NULL
    OR c.standard_concept != 'S')
  AND route_concept_id != 0
GROUP BY
  1,
  2,
  3,
  4,
  5
UNION ALL
SELECT
  'observation' AS table_name,
  'observation_concept_id' AS column_name,
  m.src_hpo_id AS src_hpo_id,
  observation_concept_id AS concept_id,
  CASE
    WHEN c.concept_id IS NULL THEN 'concept_not_exist'
    WHEN c.concept_id IS NOT NULL
  AND c.standard_concept != 'S' THEN 'concept_not_standard'
END
  AS invalid_reason,
  COUNT(1) AS row_count
FROM
  {{curation_ops_schema}}.unioned_ehr_observation t
JOIN
  {{curation_ops_schema}}._mapping_observation m
USING
  (observation_id)
LEFT JOIN
  {{vocab_schema}}.concept c
ON
  observation_concept_id = c.concept_id
WHERE
  (c.concept_id IS NULL
    OR c.standard_concept != 'S')
  AND observation_concept_id != 0
GROUP BY
  1,
  2,
  3,
  4,
  5
UNION ALL
SELECT
  'observation' AS table_name,
  'observation_type_concept' AS column_name,
  m.src_hpo_id AS src_hpo_id,
  observation_type_concept_id AS concept_id,
  CASE
    WHEN c.concept_id IS NULL THEN 'concept_not_exist'
    WHEN c.concept_id IS NOT NULL
  AND c.standard_concept != 'S' THEN 'concept_not_standard'
END
  AS invalid_reason,
  COUNT(1) AS row_count
FROM
  {{curation_ops_schema}}.unioned_ehr_observation t
JOIN
  {{curation_ops_schema}}._mapping_observation m
USING
  (observation_id)
LEFT JOIN
  {{vocab_schema}}.concept c
ON
  observation_type_concept_id = c.concept_id
WHERE
  (c.concept_id IS NULL
    OR c.standard_concept != 'S')
  AND observation_type_concept_id != 0
GROUP BY
  1,
  2,
  3,
  4,
  5
UNION ALL
SELECT
  'observation' AS table_name,
  'value_as_concept_id' AS column_name,
  m.src_hpo_id AS src_hpo_id,
  value_as_concept_id AS concept_id,
  CASE
    WHEN c.concept_id IS NULL THEN 'concept_not_exist'
    WHEN c.concept_id IS NOT NULL
  AND c.standard_concept != 'S' THEN 'concept_not_standard'
END
  AS invalid_reason,
  COUNT(1) AS row_count
FROM
  {{curation_ops_schema}}.unioned_ehr_observation t
JOIN
  {{curation_ops_schema}}._mapping_observation m
USING
  (observation_id)
LEFT JOIN
  {{vocab_schema}}.concept c
ON
  value_as_concept_id = c.concept_id
WHERE
  (c.concept_id IS NULL
    OR c.standard_concept != 'S')
  AND value_as_concept_id != 0
GROUP BY
  1,
  2,
  3,
  4,
  5
UNION ALL
SELECT
  'observation' AS table_name,
  'qualifier_concept_id' AS column_name,
  m.src_hpo_id AS src_hpo_id,
  qualifier_concept_id AS concept_id,
  CASE
    WHEN c.concept_id IS NULL THEN 'concept_not_exist'
    WHEN c.concept_id IS NOT NULL
  AND c.standard_concept != 'S' THEN 'concept_not_standard'
END
  AS invalid_reason,
  COUNT(1) AS row_count
FROM
  {{curation_ops_schema}}.unioned_ehr_observation t
JOIN
  {{curation_ops_schema}}._mapping_observation m
USING
  (observation_id)
LEFT JOIN
  {{vocab_schema}}.concept c
ON
  qualifier_concept_id = c.concept_id
WHERE
  (c.concept_id IS NULL
    OR c.standard_concept != 'S')
  AND qualifier_concept_id != 0
GROUP BY
  1,
  2,
  3,
  4,
  5
UNION ALL
SELECT
  'observation' AS table_name,
  'unit_concept_id' AS column_name,
  m.src_hpo_id AS src_hpo_id,
  unit_concept_id AS concept_id,
  CASE
    WHEN c.concept_id IS NULL THEN 'concept_not_exist'
    WHEN c.concept_id IS NOT NULL
  AND c.standard_concept != 'S' THEN 'concept_not_standard'
END
  AS invalid_reason,
  COUNT(1) AS row_count
FROM
  {{curation_ops_schema}}.unioned_ehr_observation t
JOIN
  {{curation_ops_schema}}._mapping_observation m
USING
  (observation_id)
LEFT JOIN
  {{vocab_schema}}.concept c
ON
  unit_concept_id = c.concept_id
WHERE
  (c.concept_id IS NULL
    OR c.standard_concept != 'S')
  AND unit_concept_id != 0
GROUP BY
  1,
  2,
  3,
  4,
  5
UNION ALL
SELECT
  'measurement' AS table_name,
  'measurement_concept_id' AS column_name,
  m.src_hpo_id AS src_hpo_id,
  measurement_concept_id AS concept_id,
  CASE
    WHEN c.concept_id IS NULL THEN 'concept_not_exist'
    WHEN c.concept_id IS NOT NULL
  AND c.standard_concept != 'S' THEN 'concept_not_standard'
END
  AS invalid_reason,
  COUNT(1) AS row_count
FROM
  {{curation_ops_schema}}.unioned_ehr_measurement t
JOIN
  {{curation_ops_schema}}._mapping_measurement m
USING
  (measurement_id)
LEFT JOIN
  {{vocab_schema}}.concept c
ON
  measurement_concept_id = c.concept_id
WHERE
  (c.concept_id IS NULL
    OR c.standard_concept != 'S')
  AND measurement_concept_id != 0
GROUP BY
  1,
  2,
  3,
  4,
  5
UNION ALL
SELECT
  'measurement' AS table_name,
  'measurement_type_concept_id' AS column_name,
  m.src_hpo_id AS src_hpo_id,
  measurement_type_concept_id AS concept_id,
  CASE
    WHEN c.concept_id IS NULL THEN 'concept_not_exist'
    WHEN c.concept_id IS NOT NULL
  AND c.standard_concept != 'S' THEN 'concept_not_standard'
END
  AS invalid_reason,
  COUNT(1) AS row_count
FROM
  {{curation_ops_schema}}.unioned_ehr_measurement t
JOIN
  {{curation_ops_schema}}._mapping_measurement m
USING
  (measurement_id)
LEFT JOIN
  {{vocab_schema}}.concept c
ON
  measurement_type_concept_id = c.concept_id
WHERE
  (c.concept_id IS NULL
    OR c.standard_concept != 'S')
  AND measurement_type_concept_id != 0
GROUP BY
  1,
  2,
  3,
  4,
  5
UNION ALL
SELECT
  'measurement' AS table_name,
  'operator_concept_id' AS column_name,
  m.src_hpo_id AS src_hpo_id,
  operator_concept_id AS concept_id,
  CASE
    WHEN c.concept_id IS NULL THEN 'concept_not_exist'
    WHEN c.concept_id IS NOT NULL
  AND c.standard_concept != 'S' THEN 'concept_not_standard'
END
  AS invalid_reason,
  COUNT(1) AS row_count
FROM
  {{curation_ops_schema}}.unioned_ehr_measurement t
JOIN
  {{curation_ops_schema}}._mapping_measurement m
USING
  (measurement_id)
LEFT JOIN
  {{vocab_schema}}.concept c
ON
  operator_concept_id = c.concept_id
WHERE
  (c.concept_id IS NULL
    OR c.standard_concept != 'S')
  AND operator_concept_id != 0
GROUP BY
  1,
  2,
  3,
  4,
  5
UNION ALL
SELECT
  'measurement' AS table_name,
  'value_as_concept_id' AS column_name,
  m.src_hpo_id AS src_hpo_id,
  value_as_concept_id AS concept_id,
  CASE
    WHEN c.concept_id IS NULL THEN 'concept_not_exist'
    WHEN c.concept_id IS NOT NULL
  AND c.standard_concept != 'S' THEN 'concept_not_standard'
END
  AS invalid_reason,
  COUNT(1) AS row_count
FROM
  {{curation_ops_schema}}.unioned_ehr_measurement t
JOIN
  {{curation_ops_schema}}._mapping_measurement m
USING
  (measurement_id)
LEFT JOIN
  {{vocab_schema}}.concept c
ON
  value_as_concept_id = c.concept_id
WHERE
  (c.concept_id IS NULL
    OR c.standard_concept != 'S')
  AND value_as_concept_id != 0
GROUP BY
  1,
  2,
  3,
  4,
  5
UNION ALL
SELECT
  'measurement' AS table_name,
  'unit_concept_id' AS column_name,
  m.src_hpo_id AS src_hpo_id,
  unit_concept_id AS concept_id,
  CASE
    WHEN c.concept_id IS NULL THEN 'concept_not_exist'
    WHEN c.concept_id IS NOT NULL
  AND c.standard_concept != 'S' THEN 'concept_not_standard'
END
  AS invalid_reason,
  COUNT(1) AS row_count
FROM
  {{curation_ops_schema}}.unioned_ehr_measurement t
JOIN
  {{curation_ops_schema}}._mapping_measurement m
USING
  (measurement_id)
LEFT JOIN
  {{vocab_schema}}.concept c
ON
  unit_concept_id = c.concept_id
WHERE
  (c.concept_id IS NULL
    OR c.standard_concept != 'S')
  AND unit_concept_id != 0
GROUP BY
  1,
  2,
  3,
  4,
  5
UNION ALL
SELECT
  'condition_occurrence' AS table_name,
  'condition_concept_id' AS column_name,
  m.src_hpo_id AS src_hpo_id,
  condition_concept_id AS concept_id,
  CASE
    WHEN c.concept_id IS NULL THEN 'concept_not_exist'
    WHEN c.concept_id IS NOT NULL
  AND c.standard_concept != 'S' THEN 'concept_not_standard'
END
  AS invalid_reason,
  COUNT(1) AS row_count
FROM
  {{curation_ops_schema}}.unioned_ehr_condition_occurrence t
JOIN
  {{curation_ops_schema}}._mapping_condition_occurrence m
USING
  (condition_occurrence_id)
LEFT JOIN
  {{vocab_schema}}.concept c
ON
  condition_concept_id = c.concept_id
WHERE
  (c.concept_id IS NULL
    OR c.standard_concept != 'S')
  AND condition_concept_id != 0
GROUP BY
  1,
  2,
  3,
  4,
  5
UNION ALL
SELECT
  'condition_occurrence' AS table_name,
  'condition_type_concept_id' AS column_name,
  m.src_hpo_id AS src_hpo_id,
  condition_type_concept_id AS concept_id,
  CASE
    WHEN c.concept_id IS NULL THEN 'concept_not_exist'
    WHEN c.concept_id IS NOT NULL
  AND c.standard_concept != 'S' THEN 'concept_not_standard'
END
  AS invalid_reason,
  COUNT(1) AS row_count
FROM
  {{curation_ops_schema}}.unioned_ehr_condition_occurrence t
JOIN
  {{curation_ops_schema}}._mapping_condition_occurrence m
USING
  (condition_occurrence_id)
LEFT JOIN
  {{vocab_schema}}.concept c
ON
  condition_type_concept_id = c.concept_id
WHERE
  (c.concept_id IS NULL
    OR c.standard_concept != 'S')
  AND condition_type_concept_id != 0
GROUP BY
  1,
  2,
  3,
  4,
  5
UNION ALL
SELECT
  'condition_occurrence' AS table_name,
  'condition_status_concept_id' AS column_name,
  m.src_hpo_id AS src_hpo_id,
  condition_status_concept_id AS concept_id,
  CASE
    WHEN c.concept_id IS NULL THEN 'concept_not_exist'
    WHEN c.concept_id IS NOT NULL
  AND c.standard_concept != 'S' THEN 'concept_not_standard'
END
  AS invalid_reason,
  COUNT(1) AS row_count
FROM
  {{curation_ops_schema}}.unioned_ehr_condition_occurrence t
JOIN
  {{curation_ops_schema}}._mapping_condition_occurrence m
USING
  (condition_occurrence_id)
LEFT JOIN
  {{vocab_schema}}.concept c
ON
  condition_status_concept_id = c.concept_id
WHERE
  (c.concept_id IS NULL
    OR c.standard_concept != 'S')
  AND condition_status_concept_id != 0
GROUP BY
  1,
  2,
  3,
  4,
  5;