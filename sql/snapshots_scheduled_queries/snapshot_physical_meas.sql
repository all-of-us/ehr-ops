INSERT INTO {{ehr_ops_project}}.{{ehr_ops_resources_dataset}}.snapshot_physical_meas
(select *, current_timestamp() as snapshot_ts from {{ehr_ops_project}}.{{ehr_ops_resources_dataset}}.mv_physical_meas)