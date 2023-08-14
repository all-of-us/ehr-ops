SELECT DISTINCT
hpo_id
FROM `{{ehr_ops_project}}.{{ehr_ops_staging_dataset}}.mv_table_counts_with_upload_timestamp_for_hpo_sites`
WHERE TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), person_upload_time, HOUR) <= 24