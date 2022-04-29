SELECT distinct src_hpo_id, cast(snapshot_ts as date)AS snapshot_ts_dt, 
 visit_total_rows, wrong_visit_start_death_date, wrong_visit_end_death_date,
 condition_total_rows, wrong_condition_start_death_date, wrong_condition_end_death_date, 
 drug_total_rows, wrong_drug_start_death_date, wrong_drug_end_death_date, 
 device_total_rows, wrong_device_start_death_date, wrong_device_end_death_date,
 measurement_total_rows, wrong_measurement_death_date, 
 observation_total_rows, wrong_observation_death_date, 
 procedure_total_rows, wrong_procedure_death_date	
FROM `{{pdr_project}}.{{curation_dataset}}.snapshot_dc_2`