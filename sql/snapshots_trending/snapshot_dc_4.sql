SELECT distinct src_hpo_id, cast(snapshot_ts as date)AS snapshot_ts_dt, 
    visit_start_total_rows, visit_start_not_match, visit_end_total_rows, visit_end_not_match,
    condition_start_total_rows, condition_start_not_match, condition_end_total_rows, condition_end_not_match,
    drug_start_total_rows, drug_start_not_match, drug_end_total_rows, drug_end_not_match, 
    measurement_total_rows, measurement_not_match, 
    observation_total_rows, observation_not_match, 
    procedure_total_rows, procedure_not_match, 
    device_start_total_rows, device_start_not_match, device_end_total_rows, device_end_not_match	
FROM `{{pdr_project}}.{{curation_dataset}}.snapshot_dc_4`