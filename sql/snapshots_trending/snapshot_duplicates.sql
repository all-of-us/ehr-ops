SELECT distinct src_hpo_id, cast(snapshot_ts as date) AS snapshot_ts_dt, 
    condition_dup_cnt, procedure_dup_cnt, visit_dup_cnt, observation_dup_cnt, 
    measurement_dup_cnt, drug_dup_cnt, total_dup_cnt
FROM {{curation_ops_schema}}.snapshot_duplicates