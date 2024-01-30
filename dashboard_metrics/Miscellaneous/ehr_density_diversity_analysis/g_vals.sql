WITH person_time_diff AS
(
SELECT DISTINCT person_id,
 COALESCE(TIMESTAMP_DIFF(MAX(event_datetime),MIN(event_datetime), SECOND), 0) AS total_time_diff
FROM
`{EHR_OPS_PROJECT_ID}.{EHR_DENSITY_DATASET_ID}.unioned_event`
GROUP BY 1
)
SELECT
    ue.person_id,
    ue.event_type,
    ue.event_id,
    ue.event_datetime,
    ue.preceding_event_datetime,
    ue.datetime_difference_seconds,
    ptd.total_time_diff,
    SAFE_DIVIDE(ue.datetime_difference_seconds, ptd.total_time_diff) as g_val
FROM `{EHR_OPS_PROJECT_ID}.{EHR_DENSITY_DATASET_ID}.unioned_event` ue
JOIN person_time_diff ptd
USING(person_id)