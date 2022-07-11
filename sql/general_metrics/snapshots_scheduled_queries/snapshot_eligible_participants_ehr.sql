INSERT INTO {{ehr_ops_project}}.{{ehr_ops_resources_dataset}}.snapshot_eligible_participants_ehr
(select *, current_timestamp() as snapshot_ts from {{ehr_ops_project}}.{{ehr_ops_resources_dataset}}.mv_eligible_participants_ehr)
