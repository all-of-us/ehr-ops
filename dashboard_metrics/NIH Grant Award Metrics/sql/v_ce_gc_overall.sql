WITH
  ce_person AS (
    SELECT DISTINCT person_id FROM `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_person` p
    JOIN `{{curation_project}}.{{ehr_ops_dataset}}._mapping_person` mp
    USING(person_id)
    WHERE mp.src_hpo_id = 'care_evolution_omop_dv'
  ),
  condition_total_row AS (
  SELECT
    t1.person_id,
    COUNT( t1.condition_occurrence_id) AS total_row
  FROM
    `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_condition_occurrence` AS t1
  JOIN
    `{{curation_project}}.{{ehr_ops_dataset}}._mapping_condition_occurrence` AS mt1
  USING
    (condition_occurrence_id)
  WHERE
    src_hpo_id = 'care_evolution_omop_dv'
  GROUP BY
    1 ),
  condition_well_defined_row AS (
  SELECT
    t1.person_id,
    COUNT( t1.condition_occurrence_id) AS well_defined_row
  FROM
    `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_condition_occurrence` AS t1
  JOIN
    `{{curation_project}}.{{ehr_ops_dataset}}._mapping_condition_occurrence` AS mt1
  USING
    (condition_occurrence_id)
  INNER JOIN
    `{{curation_project}}.{{ehr_ops_dataset}}.concept` AS t3
  ON
    t3.concept_id = t1.condition_concept_id
  WHERE
    src_hpo_id = 'care_evolution_omop_dv'
    AND t3.standard_concept="S"
    AND t3.domain_id="Condition"
  GROUP BY
    1 ),
  condition_total_zero_missing AS (
  SELECT
    t1.person_id,
    COUNT( t1.condition_occurrence_id) AS total_zero_missing
  FROM
    `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_condition_occurrence` AS t1
  JOIN
    `{{curation_project}}.{{ehr_ops_dataset}}._mapping_condition_occurrence` AS mt1
  USING
    (condition_occurrence_id)
  WHERE
    src_hpo_id = 'care_evolution_omop_dv'
    AND (t1.condition_concept_id=0
      OR t1.condition_concept_id IS NULL)
  GROUP BY
    1 ),
  condition_total_missing AS (
  SELECT
    t1.person_id,
    COUNT( t1.condition_occurrence_id) AS total_missing
  FROM
    `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_condition_occurrence` AS t1
  JOIN
    `{{curation_project}}.{{ehr_ops_dataset}}._mapping_condition_occurrence` AS mt1
  USING
    (condition_occurrence_id)
  WHERE
    src_hpo_id = 'care_evolution_omop_dv'
    AND t1.condition_concept_id IS NULL
  GROUP BY
    1 ),
  procedure_total_row AS (
  SELECT
    t1.person_id,
    COUNT( t1.procedure_occurrence_id) AS total_row
  FROM
    `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_procedure_occurrence` AS t1
  JOIN
    `{{curation_project}}.{{ehr_ops_dataset}}._mapping_procedure_occurrence` AS mt1
  USING
    (procedure_occurrence_id)
  WHERE
    src_hpo_id = 'care_evolution_omop_dv'
  GROUP BY
    1 ),
  procedure_well_defined_row AS (
  SELECT
    t1.person_id,
    COUNT( t1.procedure_occurrence_id) AS well_defined_row
  FROM
    `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_procedure_occurrence` AS t1
  JOIN
    `{{curation_project}}.{{ehr_ops_dataset}}._mapping_procedure_occurrence` AS mt1
  USING
    (procedure_occurrence_id)
  INNER JOIN
    `{{curation_project}}.{{ehr_ops_dataset}}.concept` AS t3
  ON
    t3.concept_id = t1.procedure_concept_id
  WHERE
    src_hpo_id = 'care_evolution_omop_dv'
    AND t3.standard_concept="S"
    AND t3.domain_id="Procedure"
  GROUP BY
    1 ),
  procedure_total_zero_missing AS (
  SELECT
    t1.person_id,
    COUNT( t1.procedure_occurrence_id) AS total_zero_missing
  FROM
    `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_procedure_occurrence` AS t1
  JOIN
    `{{curation_project}}.{{ehr_ops_dataset}}._mapping_procedure_occurrence` AS mt1
  USING
    (procedure_occurrence_id)
  WHERE
    src_hpo_id = 'care_evolution_omop_dv'
    AND (t1.procedure_concept_id=0
      OR t1.procedure_concept_id IS NULL)
  GROUP BY
    1 ),
  procedure_total_missing AS (
  SELECT
    t1.person_id,
    COUNT( t1.procedure_occurrence_id) AS total_missing
  FROM
    `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_procedure_occurrence` AS t1
  JOIN
    `{{curation_project}}.{{ehr_ops_dataset}}._mapping_procedure_occurrence` AS mt1
  USING
    (procedure_occurrence_id)
  WHERE
    src_hpo_id = 'care_evolution_omop_dv'
    AND t1.procedure_concept_id IS NULL
  GROUP BY
    1 ),
  drug_total_row AS (
  SELECT
    t1.person_id,
    COUNT( t1.drug_exposure_id) AS total_row
  FROM
    `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_drug_exposure` AS t1
  JOIN
    `{{curation_project}}.{{ehr_ops_dataset}}._mapping_drug_exposure` AS mt1
  USING
    (drug_exposure_id)
  WHERE
    src_hpo_id = 'care_evolution_omop_dv'
  GROUP BY
    1 ),
  drug_well_defined_row AS (
  SELECT
    t1.person_id,
    COUNT( t1.drug_exposure_id) AS well_defined_row
  FROM
    `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_drug_exposure` AS t1
  JOIN
    `{{curation_project}}.{{ehr_ops_dataset}}._mapping_drug_exposure` AS mt1
  USING
    (drug_exposure_id)
  INNER JOIN
    `{{curation_project}}.{{ehr_ops_dataset}}.concept` AS t3
  ON
    t3.concept_id = t1.drug_concept_id
  WHERE
    src_hpo_id = 'care_evolution_omop_dv'
    AND t3.standard_concept="S"
    AND t3.domain_id="Drug"
  GROUP BY
    1 ),
  drug_total_zero_missing AS (
  SELECT
    t1.person_id,
    COUNT( t1.drug_exposure_id) AS total_zero_missing
  FROM
    `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_drug_exposure` AS t1
  JOIN
    `{{curation_project}}.{{ehr_ops_dataset}}._mapping_drug_exposure` AS mt1
  USING
    (drug_exposure_id)
  WHERE
    src_hpo_id = 'care_evolution_omop_dv'
    AND (t1.drug_concept_id=0
      OR t1.drug_concept_id IS NULL)
  GROUP BY
    1 ),
  drug_total_missing AS (
  SELECT
    t1.person_id,
    COUNT( t1.drug_exposure_id) AS drug_total_missing
  FROM
    `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_drug_exposure` AS t1
  JOIN
    `{{curation_project}}.{{ehr_ops_dataset}}._mapping_drug_exposure` AS mt1
  USING
    (drug_exposure_id)
  WHERE
    src_hpo_id = 'care_evolution_omop_dv'
    AND t1.drug_concept_id IS NULL
  GROUP BY
    1 ),
  observation_total_row AS (
  SELECT
    t1.person_id,
    COUNT( t1.observation_id) AS total_row
  FROM
    `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_observation` AS t1
  JOIN
    `{{curation_project}}.{{ehr_ops_dataset}}._mapping_observation` AS mt1
  USING
    (observation_id)
  WHERE
    src_hpo_id = 'care_evolution_omop_dv'
  GROUP BY
    1 ),
  observation_well_defined_row AS (
  SELECT
    t1.person_id,
    COUNT( t1.observation_id) AS well_defined_row
  FROM
    `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_observation` AS t1
  INNER JOIN
    `{{curation_project}}.{{ehr_ops_dataset}}.concept` AS t3
  ON
    t3.concept_id = t1.observation_concept_id
  JOIN
    `{{curation_project}}.{{ehr_ops_dataset}}._mapping_observation` AS mt1
  USING
    (observation_id)
  WHERE
    src_hpo_id = 'care_evolution_omop_dv'
    AND t3.standard_concept="S"
  GROUP BY
    1 ),
  observation_total_zero_missing AS (
  SELECT
    t1.person_id,
    COUNT( t1.observation_id) AS total_zero_missing
  FROM
    `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_observation` AS t1
  JOIN
    `{{curation_project}}.{{ehr_ops_dataset}}._mapping_observation` AS mt1
  USING
    (observation_id)
  WHERE
    src_hpo_id = 'care_evolution_omop_dv'
    AND (t1.observation_concept_id=0
      OR t1.observation_concept_id IS NULL)
  GROUP BY
    1 ),
  observation_total_missing AS (
  SELECT
    t1.person_id,
    COUNT( t1.observation_id) AS total_missing
  FROM
    `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_observation` AS t1
  JOIN
    `{{curation_project}}.{{ehr_ops_dataset}}._mapping_observation` AS mt1
  USING
    (observation_id)
  WHERE
    src_hpo_id = 'care_evolution_omop_dv'
    AND t1.observation_concept_id IS NULL
  GROUP BY
    1 ),
  measurement_total_row AS (
  SELECT
    t1.person_id,
    COUNT( t1.measurement_id) AS total_row
  FROM
    `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_measurement` AS t1
  JOIN
    `{{curation_project}}.{{ehr_ops_dataset}}._mapping_measurement` AS mt1
  USING
    (measurement_id)
  WHERE
    src_hpo_id = 'care_evolution_omop_dv'
  GROUP BY
    1 ),
  measurement_well_defined_row AS (
  SELECT
    t1.person_id,
    COUNT( t1.measurement_id) AS well_defined_row
  FROM
    `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_measurement` AS t1
  INNER JOIN
    `{{curation_project}}.{{ehr_ops_dataset}}.concept` AS t3
  ON
    t3.concept_id = t1.measurement_concept_id
  JOIN
    `{{curation_project}}.{{ehr_ops_dataset}}._mapping_measurement` AS mt1
  USING
    (measurement_id)
  WHERE
    src_hpo_id = 'care_evolution_omop_dv'
    AND t3.standard_concept="S"
    AND t3.domain_id="Measurement"
  GROUP BY
    1 ),
  measurement_total_zero_missing AS (
  SELECT
    t1.person_id,
    COUNT( t1.measurement_id) AS total_zero_missing
  FROM
    `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_measurement` AS t1
  JOIN
    `{{curation_project}}.{{ehr_ops_dataset}}._mapping_measurement` AS mt1
  USING
    (measurement_id)
  WHERE
    src_hpo_id = 'care_evolution_omop_dv'
    AND (t1.measurement_concept_id=0
      OR t1.measurement_concept_id IS NULL)
  GROUP BY
    1 ),
  measurement_total_missing AS (
  SELECT
    t1.person_id,
    COUNT( t1.measurement_id) AS total_missing
  FROM
    `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_measurement` AS t1
  JOIN
    `{{curation_project}}.{{ehr_ops_dataset}}._mapping_measurement` AS mt1
  USING
    (measurement_id)
  WHERE
    src_hpo_id = 'care_evolution_omop_dv'
    AND t1.measurement_concept_id IS NULL
  GROUP BY
    1 ),
  visit_total_row AS (
  SELECT
    t1.person_id,
    COUNT( t1.visit_occurrence_id) AS total_row
  FROM
    `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_visit_occurrence` AS t1
  JOIN
    `{{curation_project}}.{{ehr_ops_dataset}}._mapping_visit_occurrence` AS mt1
  USING
    (visit_occurrence_id)
  WHERE
    src_hpo_id = 'care_evolution_omop_dv'
  GROUP BY
    1 ),
  visit_well_defined_row AS (
  SELECT
    t1.person_id,
    COUNT( t1.visit_occurrence_id) AS well_defined_row
  FROM
    `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_visit_occurrence` AS t1
  INNER JOIN
    `{{curation_project}}.{{ehr_ops_dataset}}.concept` AS t3
  ON
    t3.concept_id = t1.visit_concept_id
  JOIN
    `{{curation_project}}.{{ehr_ops_dataset}}._mapping_visit_occurrence` AS mt1
  USING
    (visit_occurrence_id)
  WHERE
    src_hpo_id = 'care_evolution_omop_dv'
    AND t3.standard_concept="S"
    AND t3.domain_id="Visit"
  GROUP BY
    1 ),
  visit_total_zero_missing AS (
  SELECT
    t1.person_id,
    COUNT( t1.visit_occurrence_id) AS total_zero_missing
  FROM
    `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_visit_occurrence` AS t1
  JOIN
    `{{curation_project}}.{{ehr_ops_dataset}}._mapping_visit_occurrence` AS mt1
  USING
    (visit_occurrence_id)
  WHERE
    src_hpo_id = 'care_evolution_omop_dv'
    AND (t1.visit_concept_id=0
      OR t1.visit_concept_id IS NULL)
  GROUP BY
    1 ),
  visit_total_missing AS (
  SELECT
    t1.person_id,
    COUNT( t1.visit_occurrence_id) AS total_missing
  FROM
    `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_visit_occurrence` AS t1
  JOIN
    `{{curation_project}}.{{ehr_ops_dataset}}._mapping_visit_occurrence` AS mt1
  USING
    (visit_occurrence_id)
  WHERE
    src_hpo_id = 'care_evolution_omop_dv'
    AND t1.visit_concept_id IS NULL
  GROUP BY
    1 )
SELECT DISTINCT
  participant_id,
  ORGANIZATION,
  hpo,
  hpo_display_name,
  org_display_name,
  'CARE_EVOLUTION_OMOP_DV' AS external_id,
  IFNULL(ct.total_row, 0) AS condition_total_row,
  IFNULL(cwd.well_defined_row, 0) AS condition_well_defined_row,
  IFNULL(ctzm.total_zero_missing, 0) AS condition_total_zero_missing,
  IFNULL(ctm.total_missing, 0) AS condition_total_missing,
  IFNULL(dt.total_row, 0) AS drug_total_row,
  IFNULL(dwd.well_defined_row, 0) AS drug_well_defined_row,
  IFNULL(dtzm.total_zero_missing, 0) AS drug_total_zero_missing,
  IFNULL(dtm.drug_total_missing, 0) AS drug_total_missing,
  IFNULL(mt.total_row, 0) AS measurement_total_row,
  IFNULL(mwd.well_defined_row, 0) AS measurement_well_defined_row,
  IFNULL(mtzm.total_zero_missing, 0) AS measurement_total_zero_missing,
  IFNULL(mtm.total_missing, 0) AS measurement_total_missing,
  IFNULL(ot.total_row, 0) AS observation_total_row,
  IFNULL(owd.well_defined_row, 0) AS observation_well_defined_row,
  IFNULL(otzm.total_zero_missing, 0) AS observation_total_zero_missing,
  IFNULL(otm.total_missing, 0) AS observation_total_missing,
  IFNULL(pt.total_row, 0) AS procedure_total_row,
  IFNULL(pwd.well_defined_row, 0) AS procedure_well_defined_row,
  IFNULL(ptzm.total_zero_missing, 0) AS procedure_total_zero_missing,
  IFNULL(ptm.total_missing, 0) AS procedure_total_missing,
  IFNULL(vt.total_row, 0) AS visit_total_row,
  IFNULL(vwd.well_defined_row, 0) AS visit_well_defined_row,
  IFNULL(vtzm.total_zero_missing, 0) AS visit_total_zero_missing,
  IFNULL(vtm.total_missing, 0) AS visit_total_missing
FROM
  `{{ehr_ops_project}}.{{ehr_ops_resources_dataset}}.v_ehr_rdr_participant` fin
JOIN
  ce_person
ON fin.participant_id = ce_person.person_id
LEFT JOIN
  visit_total_row vt
ON
  fin.participant_id = vt.person_id
LEFT JOIN
  drug_total_row dt
ON
  fin.participant_id = dt.person_id
LEFT JOIN
  measurement_total_row mt
ON
  fin.participant_id = mt.person_id
LEFT JOIN
  observation_total_row ot
ON
  fin.participant_id = ot.person_id
LEFT JOIN
  procedure_total_row pt
ON
  fin.participant_id = pt.person_id
LEFT JOIN
  condition_total_row ct
ON
  fin.participant_id = ct.person_id
LEFT JOIN
  condition_well_defined_row cwd
ON
  fin.participant_id = cwd.person_id
LEFT JOIN
  drug_well_defined_row dwd
ON
  fin.participant_id = dwd.person_id
LEFT JOIN
  measurement_well_defined_row mwd
ON
  fin.participant_id = mwd.person_id
LEFT JOIN
  observation_well_defined_row owd
ON
  fin.participant_id = owd.person_id
LEFT JOIN
  procedure_well_defined_row pwd
ON
  fin.participant_id = pwd.person_id
LEFT JOIN
  visit_well_defined_row vwd
ON
  fin.participant_id = vwd.person_id
LEFT JOIN
  condition_total_zero_missing ctzm
ON
  fin.participant_id = ctzm.person_id
LEFT JOIN
  drug_total_zero_missing dtzm
ON
  fin.participant_id = dtzm.person_id
LEFT JOIN
  measurement_total_zero_missing mtzm
ON
  fin.participant_id = mtzm.person_id
LEFT JOIN
  observation_total_zero_missing otzm
ON
  fin.participant_id = otzm.person_id
LEFT JOIN
  procedure_total_zero_missing ptzm
ON
  fin.participant_id = ptzm.person_id
LEFT JOIN
  visit_total_zero_missing vtzm
ON
  fin.participant_id = vtzm.person_id
LEFT JOIN
  condition_total_missing ctm
ON
  fin.participant_id = ctm.person_id
LEFT JOIN
  drug_total_missing dtm
ON
  fin.participant_id = dtm.person_id
LEFT JOIN
  measurement_total_missing mtm
ON
  fin.participant_id = mtm.person_id
LEFT JOIN
  observation_total_missing otm
ON
  fin.participant_id = otm.person_id
LEFT JOIN
  procedure_total_missing ptm
ON
  fin.participant_id = ptm.person_id
LEFT JOIN
  visit_total_missing vtm
ON
  fin.participant_id = vtm.person_id
WHERE fin.ORG_ID = 'CARE_EVOLUTION_OMOP_DV'