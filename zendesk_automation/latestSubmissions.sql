SELECT DISTINCT
hpo_id
FROM `{{curation_project}}.{{operations_analytics_dataset}}.table_counts_with_upload_timestamp_for_hpo_sites`
WHERE TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), person_upload_time, HOUR) <= 24