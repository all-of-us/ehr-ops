SELECT DISTINCT
hpo_id
FROM `aou-ehr-ops-curation-prod.ehr_ops_metrics_staging.mv_table_counts_with_upload_timestamp_for_hpo_sites`
WHERE TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), person_upload_time, HOUR) <= 24