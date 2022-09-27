SELECT
  gc.ORGANIZATION,
  gc.hpo_display_name,
  gc.org_display_name,
  SUM(condition_total_row) condition_total_row,
  SUM(condition_well_defined_row) condition_well_defined_row,
  SUM(condition_total_zero_missing) condition_total_zero_missing,
  SUM(condition_total_missing) condition_total_missing,
  SUM(drug_total_row) drug_total_row,
  SUM(drug_well_defined_row) drug_well_defined_row,
  SUM(drug_total_zero_missing) drug_total_zero_missing,
  SUM(drug_total_missing) drug_total_missing,
  SUM(measurement_total_row) measurement_total_row,
  SUM(measurement_well_defined_row) measurement_well_defined_row,
  SUM(measurement_total_zero_missing) measurement_total_zero_missing,
  SUM(measurement_total_missing) measurement_total_missing,
  SUM(observation_total_row) observation_total_row,
  SUM(observation_well_defined_row) observation_well_defined_row,
  SUM(observation_total_zero_missing) observation_total_zero_missing,
  SUM(procedure_total_row) procedure_total_row,
  SUM(procedure_well_defined_row) procedure_well_defined_row,
  SUM(procedure_total_zero_missing) procedure_total_zero_missing,
  SUM(procedure_total_missing) procedure_total_missing,
  SUM(visit_total_row) visit_total_row,
  SUM(visit_well_defined_row) visit_well_defined_row,
  SUM(visit_total_zero_missing) visit_total_zero_missing,
  SUM(visit_total_missing) visit_total_missing
FROM
  `{{ehr_ops_project}}.{{ehr_ops_staging_dataset}}.mv_ehr_gc_overall` gc
GROUP BY
  1,2,3