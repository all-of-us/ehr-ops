WITH unioned_event AS
(
  SELECT
    person_id,
    event_id,
    event_datetime,
    event_type
  FROM
    `{EHR_OPS_PROJECT_ID}.{EHR_DENSITY_DATASET_ID}.condition_event`
  UNION ALL
  SELECT
    person_id,
    event_id,
    event_datetime,
    event_type
  FROM
    `{EHR_OPS_PROJECT_ID}.{EHR_DENSITY_DATASET_ID}.drug_event`
  UNION ALL
  SELECT
    person_id,
    event_id,
    event_datetime,
    event_type
  FROM
    `{EHR_OPS_PROJECT_ID}.{EHR_DENSITY_DATASET_ID}.measurement_event`
  UNION ALL
  SELECT
    person_id,
    event_id,
    event_datetime,
    event_type
  FROM
    `{EHR_OPS_PROJECT_ID}.{EHR_DENSITY_DATASET_ID}.procedure_event`
  UNION ALL
  SELECT
    person_id,
    event_id,
    event_datetime,
    event_type
  FROM
    `{EHR_OPS_PROJECT_ID}.{EHR_DENSITY_DATASET_ID}.observation_event`
  UNION ALL
  SELECT
    person_id,
    event_id,
    event_datetime,
    event_type
  FROM
    `{EHR_OPS_PROJECT_ID}.{EHR_DENSITY_DATASET_ID}.visit_event`
  UNION ALL
  SELECT
    person_id,
    event_id,
    event_datetime,
    event_type
  FROM
    `{EHR_OPS_PROJECT_ID}.{EHR_DENSITY_DATASET_ID}.device_event`)
SELECT
  person_id,
  event_datetime,
  event_type,
  event_id,
  COALESCE(LAG(event_datetime) OVER (PARTITION BY person_id ORDER BY event_datetime ASC, event_id ASC), event_datetime) AS preceding_event_datetime,
  COALESCE(TIMESTAMP_DIFF(
    event_datetime,
    LAG(event_datetime) OVER (PARTITION BY person_id ORDER BY event_datetime ASC, event_id ASC),
    SECOND
  ), 0) AS datetime_difference_seconds
FROM unioned_event
