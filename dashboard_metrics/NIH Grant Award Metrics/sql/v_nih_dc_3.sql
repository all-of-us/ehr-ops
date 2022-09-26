SELECT
  ORGANIZATION,
  hpo_display_name,
  org_display_name,
  SUM(visit_start_total_rows_3) AS visit_start_total_rows,
  SUM(visit_start_wrong_date_rows_3) AS visit_start_wrong_date_rows,
  SUM(visit_end_total_rows_3) AS visit_end_total_rows,
  SUM(visit_end_wrong_date_rows_3) AS visit_end_wrong_date_rows,

  SUM(condition_start_total_rows_3) AS condition_start_total_rows,
  SUM(condition_start_wrong_date_rows_3) AS condition_start_wrong_date_rows,
  SUM(condition_end_total_rows_3) AS condition_end_total_rows,
  SUM(condition_end_wrong_date_rows_3) AS condition_end_wrong_date_rows,

  SUM(drug_start_total_rows_3) AS drug_start_total_rows,
  SUM(drug_start_wrong_date_rows_3) AS drug_start_wrong_date_rows,
  SUM(drug_end_total_rows_3) AS drug_end_total_rows,
  SUM(drug_end_wrong_date_rows_3) AS drug_end_wrong_date_rows,

  SUM(measurement_total_rows_3) AS measurement_total_rows,
  SUM(measurement_wrong_date_rows_3) AS measurement_wrong_date_rows,

  SUM(observation_total_rows_3) AS observation_total_rows,
  SUM(observation_wrong_date_rows_3) AS observation_wrong_date_rows,

  SUM(procedure_total_rows_3) AS procedure_total_rows,
  SUM(procedure_wrong_date_rows_3) AS procedure_wrong_date_rows,

  SUM(device_start_total_rows_3) AS device_start_total_rows,
  SUM(device_start_wrong_date_rows_3) AS device_start_wrong_date_rows,
  SUM(device_end_total_rows_3) AS device_end_total_rows,
  SUM(device_end_wrong_date_rows_3) AS device_end_wrong_date_rows,

  CURRENT_TIMESTAMP()  AS snapshot_ts
FROM
  `{{ehr_ops_project}}.{{ehr_ops_staging_dataset}}.mv_ehr_dc_overall`
GROUP BY 1,2,3