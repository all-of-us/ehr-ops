SELECT distinct src_hpo_id, cast(snapshot_ts as date) AS snapshot_ts_dt, 
  condition_rows_w_no_valid_vo, procedure_rows_w_no_valid_vo
FROM {{curation_ops_schema}}.snapshot_visit_id_failure