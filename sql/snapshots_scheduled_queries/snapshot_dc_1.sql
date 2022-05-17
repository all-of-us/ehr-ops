INSERT INTO {{ehr_ops_project}}.{{ehr_ops_resources_dataset}}.snapshot_dc_1
(select *, current_timestamp() as snapshot_ts from {{ehr_ops_project}}.{{ehr_ops_resources_dataset}}.mv_dc_1)
