WITH
  condition_total_row AS (
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
    1 ),
  condition_well_defined_row AS (
  SELECT
    src_hpo_id,
    COUNT( t1.condition_occurrence_id) AS well_defined_row
  FROM
    `{{curation_project}}.{{unioned_ehr_dataset}}.condition_occurrence` AS t1
  INNER JOIN (
    SELECT
       src_hpo_id, condition_occurrence_id
    FROM
      `{{curation_project}}.{{unioned_ehr_dataset}}._mapping_condition_occurrence`) AS t2
  ON
    t1.condition_occurrence_id=t2.condition_occurrence_id
  INNER JOIN
    `{{curation_project}}.{{unioned_ehr_dataset}}.concept` AS t3
  ON
    t3.concept_id = t1.condition_concept_id
  WHERE
    t3.standard_concept="S"
    AND t3.domain_id="Condition"

  GROUP BY
    1 ),
  condition_total_zero_missing AS (
  SELECT
    src_hpo_id,
    COUNT( t1.condition_occurrence_id) AS total_zero_missing
  FROM
    `{{curation_project}}.{{unioned_ehr_dataset}}.condition_occurrence` AS t1
  INNER JOIN (
    SELECT
       src_hpo_id, condition_occurrence_id
    FROM
      `{{curation_project}}.{{unioned_ehr_dataset}}._mapping_condition_occurrence`) AS t2
  ON
    t1.condition_occurrence_id=t2.condition_occurrence_id
  WHERE
    (t1.condition_concept_id=0
      OR t1.condition_concept_id IS NULL)

  GROUP BY
    1 ),
  condition_total_missing AS (
  SELECT
    src_hpo_id,
    COUNT( t1.condition_occurrence_id) AS total_missing
  FROM
    `{{curation_project}}.{{unioned_ehr_dataset}}.condition_occurrence` AS t1
  INNER JOIN (
    SELECT
       src_hpo_id, condition_occurrence_id
    FROM
      `{{curation_project}}.{{unioned_ehr_dataset}}._mapping_condition_occurrence`) AS t2
  ON
    t1.condition_occurrence_id=t2.condition_occurrence_id
  WHERE
    t1.condition_concept_id IS NULL

  GROUP BY
    1 ),
  procedure_total_row AS (
  SELECT
    src_hpo_id,
    COUNT( t1.procedure_occurrence_id) AS total_row
  FROM
    `{{curation_project}}.{{unioned_ehr_dataset}}.procedure_occurrence` AS t1
  INNER JOIN (
    SELECT
       src_hpo_id, procedure_occurrence_id
    FROM
      `{{curation_project}}.{{unioned_ehr_dataset}}._mapping_procedure_occurrence`) AS t2
  ON
    t1.procedure_occurrence_id=t2.procedure_occurrence_id

  GROUP BY
    1 ),
  procedure_well_defined_row AS (
  SELECT
    src_hpo_id,
    COUNT( t1.procedure_occurrence_id) AS well_defined_row
  FROM
    `{{curation_project}}.{{unioned_ehr_dataset}}.procedure_occurrence` AS t1
  INNER JOIN (
    SELECT
       src_hpo_id, procedure_occurrence_id
    FROM
      `{{curation_project}}.{{unioned_ehr_dataset}}._mapping_procedure_occurrence`) AS t2
  ON
    t1.procedure_occurrence_id=t2.procedure_occurrence_id
  INNER JOIN
    `{{curation_project}}.{{unioned_ehr_dataset}}.concept` AS t3
  ON
    t3.concept_id = t1.procedure_concept_id
  WHERE
    t3.standard_concept="S"
    AND t3.domain_id="Procedure"

  GROUP BY
    1 ),
  procedure_total_zero_missing AS (
  SELECT
    src_hpo_id,
    COUNT( t1.procedure_occurrence_id) AS total_zero_missing
  FROM
    `{{curation_project}}.{{unioned_ehr_dataset}}.procedure_occurrence` AS t1
  INNER JOIN (
    SELECT
       src_hpo_id, procedure_occurrence_id
    FROM
      `{{curation_project}}.{{unioned_ehr_dataset}}._mapping_procedure_occurrence`) AS t2
  ON
    t1.procedure_occurrence_id=t2.procedure_occurrence_id
  WHERE
    (t1.procedure_concept_id=0
      OR t1.procedure_concept_id IS NULL)

  GROUP BY
    1 ),
  procedure_total_missing AS (
  SELECT
    src_hpo_id,
    COUNT( t1.procedure_occurrence_id) AS total_missing
  FROM
    `{{curation_project}}.{{unioned_ehr_dataset}}.procedure_occurrence` AS t1
  INNER JOIN (
    SELECT
       src_hpo_id, procedure_occurrence_id
    FROM
      `{{curation_project}}.{{unioned_ehr_dataset}}._mapping_procedure_occurrence`) AS t2
  ON
    t1.procedure_occurrence_id=t2.procedure_occurrence_id
  WHERE
    t1.procedure_concept_id IS NULL

  GROUP BY
    1 ),
 drug_total_row AS (
  SELECT
    src_hpo_id,
    COUNT( t1.drug_exposure_id) AS total_row
  FROM
    `{{curation_project}}.{{unioned_ehr_dataset}}.drug_exposure` AS t1
  INNER JOIN (
    SELECT
       src_hpo_id, drug_exposure_id
    FROM
      `{{curation_project}}.{{unioned_ehr_dataset}}._mapping_drug_exposure`) AS t2
  ON
    t1.drug_exposure_id=t2.drug_exposure_id

  GROUP BY
    1 ),
  drug_well_defined_row AS (
  SELECT
    src_hpo_id,
    COUNT( t1.drug_exposure_id) AS well_defined_row
  FROM
    `{{curation_project}}.{{unioned_ehr_dataset}}.drug_exposure` AS t1
  INNER JOIN (
    SELECT
       src_hpo_id, drug_exposure_id
    FROM
      `{{curation_project}}.{{unioned_ehr_dataset}}._mapping_drug_exposure`) AS t2
  ON
    t1.drug_exposure_id=t2.drug_exposure_id
  INNER JOIN
    `{{curation_project}}.{{unioned_ehr_dataset}}.concept` AS t3
  ON
    t3.concept_id = t1.drug_concept_id
  WHERE
    t3.standard_concept="S"
    AND t3.domain_id="Drug"

  GROUP BY
    1 ),
  drug_total_zero_missing AS (
  SELECT
    src_hpo_id,
    COUNT( t1.drug_exposure_id) AS total_zero_missing
  FROM
    `{{curation_project}}.{{unioned_ehr_dataset}}.drug_exposure` AS t1
  INNER JOIN (
    SELECT
       src_hpo_id, drug_exposure_id
    FROM
      `{{curation_project}}.{{unioned_ehr_dataset}}._mapping_drug_exposure`) AS t2
  ON
    t1.drug_exposure_id=t2.drug_exposure_id
  WHERE
    (t1.drug_concept_id=0
      OR t1.drug_concept_id IS NULL)

  GROUP BY
    1 ),
  drug_total_missing AS (
  SELECT
    src_hpo_id,
    COUNT( t1.drug_exposure_id) AS drug_total_missing
  FROM
    `{{curation_project}}.{{unioned_ehr_dataset}}.drug_exposure` AS t1
  INNER JOIN (
    SELECT
       src_hpo_id, drug_exposure_id
    FROM
      `{{curation_project}}.{{unioned_ehr_dataset}}._mapping_drug_exposure`) AS t2
  ON
    t1.drug_exposure_id=t2.drug_exposure_id
  WHERE
    t1.drug_concept_id IS NULL

  GROUP BY
    1 ),

  observation_total_row AS (
  SELECT
    src_hpo_id,
    COUNT( t1.observation_id) AS total_row
  FROM
    `{{curation_project}}.{{unioned_ehr_dataset}}.observation` AS t1
  INNER JOIN (
    SELECT
       src_hpo_id, observation_id
    FROM
      `{{curation_project}}.{{unioned_ehr_dataset}}._mapping_observation`
    WHERE src_table_id not like '%person%') AS t2
  ON
    t1.observation_id=t2.observation_id

  GROUP BY
    1 ),
  observation_well_defined_row AS (
  SELECT
    src_hpo_id,
    COUNT( t1.observation_id) AS well_defined_row
  FROM
    `{{curation_project}}.{{unioned_ehr_dataset}}.observation` AS t1
  INNER JOIN (
    SELECT
       src_hpo_id, observation_id
    FROM
      `{{curation_project}}.{{unioned_ehr_dataset}}._mapping_observation`
    WHERE src_table_id not like '%person') AS t2
  ON
    t1.observation_id=t2.observation_id
  INNER JOIN
    `{{curation_project}}.{{unioned_ehr_dataset}}.concept` AS t3
  ON
    t3.concept_id = t1.observation_concept_id
  WHERE
    t3.standard_concept="S"

  GROUP BY
    1 ),
  observation_total_zero_missing AS (
  SELECT
    src_hpo_id,
    COUNT( t1.observation_id) AS total_zero_missing
  FROM
    `{{curation_project}}.{{unioned_ehr_dataset}}.observation` AS t1
  INNER JOIN (
    SELECT
       src_hpo_id, observation_id
    FROM
      `{{curation_project}}.{{unioned_ehr_dataset}}._mapping_observation`
    WHERE src_table_id not like '%person%') AS t2
  ON
    t1.observation_id=t2.observation_id
  WHERE
    (t1.observation_concept_id=0
      OR t1.observation_concept_id IS NULL)

  GROUP BY
    1 ),
  observation_total_missing AS (
  SELECT
    src_hpo_id,
    COUNT( t1.observation_id) AS total_missing
  FROM
    `{{curation_project}}.{{unioned_ehr_dataset}}.observation` AS t1
  INNER JOIN (
    SELECT
       src_hpo_id, observation_id
    FROM
      `{{curation_project}}.{{unioned_ehr_dataset}}._mapping_observation`
    WHERE src_table_id not like '%person%') AS t2
  ON
    t1.observation_id=t2.observation_id
  WHERE
    t1.observation_concept_id IS NULL

  GROUP BY
    1 ),
 measurement_total_row AS (
  SELECT
    src_hpo_id,
    COUNT( t1.measurement_id) AS total_row
  FROM
    `{{curation_project}}.{{unioned_ehr_dataset}}.measurement` AS t1
  INNER JOIN (
    SELECT
       src_hpo_id, measurement_id
    FROM
      `{{curation_project}}.{{unioned_ehr_dataset}}._mapping_measurement`) AS t2
  ON
    t1.measurement_id=t2.measurement_id

  GROUP BY
    1 ),
  measurement_well_defined_row AS (
  SELECT
    src_hpo_id,
    COUNT( t1.measurement_id) AS well_defined_row
  FROM
    `{{curation_project}}.{{unioned_ehr_dataset}}.measurement` AS t1
  INNER JOIN (
    SELECT
       src_hpo_id, measurement_id
    FROM
      `{{curation_project}}.{{unioned_ehr_dataset}}._mapping_measurement`) AS t2
  ON
    t1.measurement_id=t2.measurement_id
  INNER JOIN
    `{{curation_project}}.{{unioned_ehr_dataset}}.concept` AS t3
  ON
    t3.concept_id = t1.measurement_concept_id
  WHERE
    t3.standard_concept="S"
    AND t3.domain_id="Measurement"

  GROUP BY
    1 ),
  measurement_total_zero_missing AS (
  SELECT
    src_hpo_id,
    COUNT( t1.measurement_id) AS total_zero_missing
  FROM
    `{{curation_project}}.{{unioned_ehr_dataset}}.measurement` AS t1
  INNER JOIN (
    SELECT
       src_hpo_id, measurement_id
    FROM
      `{{curation_project}}.{{unioned_ehr_dataset}}._mapping_measurement`) AS t2
  ON
    t1.measurement_id=t2.measurement_id
  WHERE
    (t1.measurement_concept_id=0
      OR t1.measurement_concept_id IS NULL)

  GROUP BY
    1 ),
  measurement_total_missing AS (
  SELECT
    src_hpo_id,
    COUNT( t1.measurement_id) AS total_missing
  FROM
    `{{curation_project}}.{{unioned_ehr_dataset}}.measurement` AS t1
  INNER JOIN (
    SELECT
       src_hpo_id, measurement_id
    FROM
      `{{curation_project}}.{{unioned_ehr_dataset}}._mapping_measurement`) AS t2
  ON
    t1.measurement_id=t2.measurement_id
  WHERE
    t1.measurement_concept_id IS NULL

  GROUP BY
    1 ),
  visit_total_row AS (
  SELECT
    src_hpo_id,
    COUNT( t1.visit_occurrence_id) AS total_row
  FROM
    `{{curation_project}}.{{unioned_ehr_dataset}}.visit_occurrence` AS t1
  INNER JOIN (
    SELECT
       src_hpo_id, visit_occurrence_id
    FROM
      `{{curation_project}}.{{unioned_ehr_dataset}}._mapping_visit_occurrence`) AS t2
  ON
    t1.visit_occurrence_id=t2.visit_occurrence_id

  GROUP BY
    1 ),
  visit_well_defined_row AS (
  SELECT
    src_hpo_id,
    COUNT( t1.visit_occurrence_id) AS well_defined_row
  FROM
    `{{curation_project}}.{{unioned_ehr_dataset}}.visit_occurrence` AS t1
  INNER JOIN (
    SELECT
       src_hpo_id, visit_occurrence_id
    FROM
      `{{curation_project}}.{{unioned_ehr_dataset}}._mapping_visit_occurrence`) AS t2
  ON
    t1.visit_occurrence_id=t2.visit_occurrence_id
  INNER JOIN
    `{{curation_project}}.{{unioned_ehr_dataset}}.concept` AS t3
  ON
    t3.concept_id = t1.visit_concept_id
  WHERE
    t3.standard_concept="S"
    AND t3.domain_id="Visit"

  GROUP BY
    1 ),
  visit_total_zero_missing AS (
  SELECT
    src_hpo_id,
    COUNT( t1.visit_occurrence_id) AS total_zero_missing
  FROM
    `{{curation_project}}.{{unioned_ehr_dataset}}.visit_occurrence` AS t1
  INNER JOIN (
    SELECT
       src_hpo_id, visit_occurrence_id
    FROM
      `{{curation_project}}.{{unioned_ehr_dataset}}._mapping_visit_occurrence`) AS t2
  ON
    t1.visit_occurrence_id=t2.visit_occurrence_id
  WHERE
    (t1.visit_concept_id=0
      OR t1.visit_concept_id IS NULL)

  GROUP BY
    1 ),
  visit_total_missing AS (
  SELECT
    src_hpo_id,
    COUNT( t1.visit_occurrence_id) AS total_missing
  FROM
    `{{curation_project}}.{{unioned_ehr_dataset}}.visit_occurrence` AS t1
  INNER JOIN (
    SELECT
       src_hpo_id, visit_occurrence_id
    FROM
      `{{curation_project}}.{{unioned_ehr_dataset}}._mapping_visit_occurrence`) AS t2
  ON
    t1.visit_occurrence_id=t2.visit_occurrence_id
  WHERE
    t1.visit_concept_id IS NULL

  GROUP BY
    1 )


SELECT 
{{cdr_version}} as cdr_version,
src_hpo_id,
condition_total_row, 
drug_total_row, 
measurement_total_row,
observation_total_row,
procedure_total_row,
visit_total_row,
safe_divide(condition_well_defined_row,condition_total_row) as condition_gc_1,
safe_divide(drug_well_defined_row,drug_total_row) as drug_gc_1,
safe_divide(measurement_well_defined_row,measurement_total_row) as measurement_gc_1,
safe_divide(observation_well_defined_row,observation_total_row) as observation_gc_1,
safe_divide(procedure_well_defined_row,procedure_total_row) as procedure_gc_1,
safe_divide(visit_well_defined_row,visit_total_row) as visit_gc_1
FROM
(
SELECT vt.src_hpo_id,
{{cdr_version}} as cdr_version,
IFNULL(ct.total_row, 0) as condition_total_row,
IFNULL(cwd.well_defined_row, 0) as condition_well_defined_row,
IFNULL(ctzm.total_zero_missing, 0) as condition_total_zero_missing,
IFNULL(ctm.total_missing, 0) as condition_total_missing,
IFNULL(dt.total_row, 0) as drug_total_row,
IFNULL(dwd.well_defined_row, 0) as drug_well_defined_row,
IFNULL(dtzm.total_zero_missing, 0) as drug_total_zero_missing,
IFNULL(dtm.drug_total_missing, 0) as drug_total_missing,
IFNULL(mt.total_row, 0) as measurement_total_row,
IFNULL(mwd.well_defined_row, 0) as measurement_well_defined_row,
IFNULL(mtzm.total_zero_missing, 0) as measurement_total_zero_missing,
IFNULL(mtm.total_missing, 0) as measurement_total_missing,
IFNULL(ot.total_row, 0) as observation_total_row,
IFNULL(owd.well_defined_row, 0) as observation_well_defined_row,
IFNULL(otzm.total_zero_missing, 0) as observation_total_zero_missing,
IFNULL(otm.total_missing, 0) as observation_total_missing,
IFNULL(pt.total_row, 0) as procedure_total_row,
IFNULL(pwd.well_defined_row, 0) as procedure_well_defined_row,
IFNULL(ptzm.total_zero_missing, 0) as procedure_total_zero_missing,
IFNULL(ptm.total_missing, 0) as procedure_total_missing,
IFNULL(vt.total_row, 0) as visit_total_row,
IFNULL(vwd.well_defined_row, 0) as visit_well_defined_row,
IFNULL(vtzm.total_zero_missing, 0) as visit_total_zero_missing,
IFNULL(vtm.total_missing, 0) as visit_total_missing,


FROM visit_total_row vt
LEFT JOIN drug_total_row dt on vt.src_hpo_id = dt.src_hpo_id
LEFT JOIN measurement_total_row mt on vt.src_hpo_id = mt.src_hpo_id
LEFT JOIN observation_total_row ot on vt.src_hpo_id = ot.src_hpo_id
LEFT JOIN procedure_total_row pt on vt.src_hpo_id = pt.src_hpo_id
LEFT JOIN condition_total_row ct on vt.src_hpo_id = ct.src_hpo_id
LEFT JOIN condition_well_defined_row cwd on vt.src_hpo_id = cwd.src_hpo_id
LEFT JOIN drug_well_defined_row dwd on vt.src_hpo_id = dwd.src_hpo_id
LEFT JOIN measurement_well_defined_row mwd on vt.src_hpo_id = mwd.src_hpo_id
LEFT JOIN observation_well_defined_row owd on vt.src_hpo_id = owd.src_hpo_id
LEFT JOIN procedure_well_defined_row pwd on vt.src_hpo_id = pwd.src_hpo_id
LEFT JOIN visit_well_defined_row vwd on vt.src_hpo_id = vwd.src_hpo_id
LEFT JOIN condition_total_zero_missing ctzm on vt.src_hpo_id = ctzm.src_hpo_id
LEFT JOIN drug_total_zero_missing dtzm on vt.src_hpo_id = dtzm.src_hpo_id
LEFT JOIN measurement_total_zero_missing mtzm on vt.src_hpo_id = mtzm.src_hpo_id
LEFT JOIN observation_total_zero_missing otzm on vt.src_hpo_id = otzm.src_hpo_id
LEFT JOIN procedure_total_zero_missing ptzm on vt.src_hpo_id = ptzm.src_hpo_id
LEFT JOIN visit_total_zero_missing vtzm on vt.src_hpo_id = vtzm.src_hpo_id
LEFT JOIN condition_total_missing ctm on vt.src_hpo_id = ctm.src_hpo_id
LEFT JOIN drug_total_missing dtm on vt.src_hpo_id = dtm.src_hpo_id
LEFT JOIN measurement_total_missing mtm on vt.src_hpo_id = mtm.src_hpo_id
LEFT JOIN observation_total_missing otm on vt.src_hpo_id = otm.src_hpo_id
LEFT JOIN procedure_total_missing ptm on vt.src_hpo_id = ptm.src_hpo_id
LEFT JOIN visit_total_missing vtm on vt.src_hpo_id = vtm.src_hpo_id
)
GROUP BY 2,3,4,5,6,7,8,9,10,11,12,13,14