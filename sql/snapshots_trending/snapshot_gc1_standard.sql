SELECT distinct src_hpo_id, cast(snapshot_ts as date) AS snapshot_ts_dt, 
  condition_total_row, condition_well_defined_row, condition_total_zero_missing, condition_total_missing,
  drug_total_row, drug_well_defined_row, drug_total_zero_missing, drug_total_missing,
  measurement_total_row, measurement_well_defined_row, measurement_total_zero_missing, measurement_total_missing,
  observation_total_row, observation_well_defined_row, observation_total_zero_missing, observation_total_missing,
  procedure_total_row, procedure_well_defined_row, procedure_total_zero_missing, procedure_total_missing,
  visit_total_row, visit_well_defined_row, visit_total_zero_missing, visit_total_missing 
FROM `{{pdr_project}}.{{curation_dataset}}.snapshot_gc_1_standard`