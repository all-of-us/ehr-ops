INSERT INTO {{ehr_ops_project}}.{{ehr_ops_resources_dataset}}.snapshot_visit_id_failure
(select *, current_timestamp() as snapshot_ts from {{ehr_ops_project}}.{{ehr_ops_resources_dataset}}.mv_visit_id_failure)
