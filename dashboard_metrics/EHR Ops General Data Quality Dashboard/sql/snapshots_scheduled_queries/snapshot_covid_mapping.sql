INSERT INTO {{ehr_ops_project}}.{{ehr_ops_resources_dataset}}.snapshot_covid_mapping
(select *, current_timestamp() as snapshot_ts from {{ehr_ops_project}}.{{ehr_ops_resources_dataset}}.mv_covid_mapping)
