INSERT INTO {{ehr_ops_project}}.{{ehr_ops_resources_dataset}}.snapshot_table_counts_with_upload_timestamp
(select *, current_timestamp() as snapshot_ts from {{ehr_ops_project}}.{{ehr_ops_resources_dataset}}.table_counts_with_upload_timestamp)
