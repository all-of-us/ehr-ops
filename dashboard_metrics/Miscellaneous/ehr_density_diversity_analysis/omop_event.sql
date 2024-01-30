SELECT DISTINCT
    person_id,
    condition_occurrence_id AS event_id,
    condition_start_datetime AS event_datetime,
    'condition' AS event_type
FROM `{CURATION_PROJECT_ID}.{EHR_OPS_DATASET_ID}.unioned_ehr_condition_occurrence`
WHERE condition_start_datetime IS NOT NULL
UNION ALL
SELECT DISTINCT
    person_id,
    drug_exposure_id AS event_id,
    drug_exposure_start_datetime AS event_datetime,
    'drug' AS event_type
FROM `{CURATION_PROJECT_ID}.{EHR_OPS_DATASET_ID}.unioned_ehr_drug_exposure`
WHERE drug_exposure_start_datetime IS NOT NULL
UNION ALL
SELECT DISTINCT
    person_id,
    measurement_id AS event_id,
    measurement_datetime AS event_datetime,
    'measurement' AS event_type
FROM `{CURATION_PROJECT_ID}.{EHR_OPS_DATASET_ID}.unioned_ehr_measurement`
WHERE measurement_datetime IS NOT NULL
UNION ALL
SELECT DISTINCT
    person_id,
    procedure_occurrence_id AS event_id,
    procedure_datetime AS event_datetime,
    'procedure' AS event_type
FROM `{CURATION_PROJECT_ID}.{EHR_OPS_DATASET_ID}.unioned_ehr_procedure_occurrence`
WHERE procedure_datetime IS NOT NULL
UNION ALL
SELECT DISTINCT
    person_id,
    observation_id AS event_id,
    observation_datetime AS event_datetime,
    'observation' AS event_type
FROM `{CURATION_PROJECT_ID}.{EHR_OPS_DATASET_ID}.unioned_ehr_observation`
WHERE observation_datetime IS NOT NULL
UNION ALL
SELECT DISTINCT
    person_id,
    visit_occurrence_id AS event_id,
    visit_start_datetime AS event_datetime,
    'visit' AS event_type
FROM `{CURATION_PROJECT_ID}.{EHR_OPS_DATASET_ID}.unioned_ehr_visit_occurrence`
WHERE visit_start_datetime IS NOT NULL
UNION ALL
SELECT DISTINCT
    person_id,
    device_exposure_id AS event_id,
    device_exposure_start_datetime AS event_datetime,
    'device' AS event_type
FROM `{CURATION_PROJECT_ID}.{EHR_OPS_DATASET_ID}.unioned_ehr_device_exposure`
WHERE device_exposure_start_datetime IS NOT NULL
