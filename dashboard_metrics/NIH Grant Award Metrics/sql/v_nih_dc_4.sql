SELECT
  ORGANIZATION,
  hpo_display_name,
  org_display_name,
  SUM(visit_start_total_rows_4) AS visit_start_total_rows,
  SUM(visit_start_not_match_4) AS visit_start_not_match,
  SUM(visit_end_total_rows_4) AS visit_end_total_rows,
  SUM(visit_end_not_match_4) AS visit_end_not_match,

  SUM(condition_start_total_rows_4) AS condition_start_total_rows,
  SUM(condition_start_not_match_4) AS condition_start_not_match,
  SUM(condition_end_total_rows_4) AS condition_end_total_rows,
  SUM(condition_end_not_match_4) AS condition_end_not_match,

  SUM(drug_start_total_rows_4) AS drug_start_total_rows,
  SUM(drug_start_not_match_4) AS drug_start_not_match,
  SUM(drug_end_total_rows_4) AS drug_end_total_rows,
  SUM(drug_end_not_match_4) AS drug_end_not_match,

  SUM(measurement_total_rows_4) AS measurement_total_rows,
  SUM(measurement_not_match_4) AS measurement_not_match,

  SUM(observation_total_rows_4) AS observation_total_rows,
  SUM(observation_not_match_4) AS observation_not_match,

  SUM(procedure_total_rows_4) AS procedure_total_rows,
  SUM(procedure_not_match_4) AS procedure_not_match,

  SUM(device_start_total_rows) AS device_start_total_rows,
  SUM(device_start_not_match) AS device_start_not_match,
  SUM(device_end_total_rows) AS device_end_total_rows,
  SUM(device_end_not_match) AS device_end_not_match

FROM
  `{{ehr_ops_project}}.{{ehr_ops_staging_dataset}}.mv_ehr_dc_overall`
GROUP BY 1,2,3