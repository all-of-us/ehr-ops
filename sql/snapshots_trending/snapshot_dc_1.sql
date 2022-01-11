SELECT distinct src_hpo_id, cast(snapshot_ts as date)AS snapshot_ts_dt, 
 visit_total_rows, visit_wrong_date_rows, condition_total_rows, condition_wrong_date_rows,
 drug_total_rows, drug_wrong_date_rows, device_total_rows, device_wrong_date_rows
FROM {{curation_ops_schema}}.snapshot_dc_1