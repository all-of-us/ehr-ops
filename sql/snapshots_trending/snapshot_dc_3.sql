SELECT distinct src_hpo_id, cast(snapshot_ts as date)AS snapshot_ts_dt, 
    visit_start_total_rows, visit_start_wrong_date_rows, visit_end_total_rows, visit_end_wrong_date_rows,
    condition_start_total_rows, condition_start_wrong_date_rows, condition_end_total_rows, condition_end_wrong_date_rows,
    drug_start_total_rows, drug_start_wrong_date_rows, drug_end_total_rows, drug_end_wrong_date_rows,
    measurement_total_rows, measurement_wrong_date_rows,
    observation_total_rows, observation_wrong_date_rows, 
    procedure_total_rows, procedure_wrong_date_rows,
    device_start_total_rows, device_start_wrong_date_rows, device_end_total_rows, device_end_wrong_date_rows
FROM `{{curation_project}}.{{ehr_ops_dataset}}.snapshot_dc_3`