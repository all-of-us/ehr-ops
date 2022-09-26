SELECT
  ORGANIZATION,
  hpo_display_name,
  org_display_name,
  SUM(visit_total_rows_2) AS visit_total_rows,
  SUM(wrong_visit_start_death_date_2) AS wrong_visit_start_death_date,
  SUM(wrong_visit_end_death_date_2) AS wrong_visit_end_death_date,

  SUM(condition_total_rows_2) AS condition_total_rows,
  SUM(wrong_condition_start_death_date_2) AS wrong_condition_start_death_date,
  SUM(wrong_condition_end_death_date_2) AS wrong_condition_end_death_date,

  SUM(drug_total_rows_2) AS drug_total_rows,
  SUM(wrong_drug_start_death_date_2) AS wrong_drug_start_death_date,
  SUM(wrong_drug_end_death_date_2) AS wrong_drug_end_death_date,

  SUM(device_total_rows_2) AS device_total_rows,
  SUM(wrong_device_start_death_date_2) AS wrong_device_start_death_date,
  SUM(wrong_device_end_death_date_2) AS wrong_device_end_death_date,

  SUM(measurement_total_rows_2) AS measurement_total_rows,
  SUM(wrong_measurement_death_date_2) AS wrong_measurement_death_date,

  SUM(observation_total_rows_2) AS observation_total_rows,
  SUM(wrong_observation_death_date_2) AS wrong_observation_death_date,

  SUM(procedure_total_rows_2) AS procedure_total_rows,
  SUM(wrong_procedure_death_date_2) AS wrong_procedure_death_date,

  CURRENT_TIMESTAMP()  AS snapshot_ts
FROM
  `{{ehr_ops_project}}.{{ehr_ops_staging_dataset}}.mv_ehr_dc_overall`
GROUP BY 1,2,3