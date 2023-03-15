SELECT DISTINCT
hpo_id
FROM `aou-res-curation-prod.operations_analytics..table_counts_with_upload_timestamp_for_hpo_sites`
WHERE TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), person_upload_time, HOUR) <= 24