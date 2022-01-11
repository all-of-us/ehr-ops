SELECT distinct src_hpo_id, cast(snapshot_ts as date) AS snapshot_ts_dt, 
  number_successful_units, rows_w_units, number_valid_routes
FROM {{curation_ops_schema}}.snapshot_unit_route_failure