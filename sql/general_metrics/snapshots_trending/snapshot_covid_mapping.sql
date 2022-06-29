SELECT distinct src_hpo_id, cast(snapshot_ts as date)AS snapshot_ts_dt, covid_mapping_issue_count, total_covid_measurements
FROM `{{curation_project}}.{{ehr_ops_dataset}}.snapshot_covid_mapping`