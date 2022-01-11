SELECT distinct src_hpo_id, cast(snapshot_ts as date)AS snapshot_ts_dt, covid_mapping_issue_count, total_covid_measurements
FROM {{curation_ops_schema}}.snapshot_covid_mapping