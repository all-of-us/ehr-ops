SELECT
  ORGANIZATION,
  hpo_display_name,
  org_display_name,
  SUM(visit_total_rows) AS visit_total_rows,
  SUM(visit_wrong_date_rows) AS visit_wrong_date_rows,
  SUM(condition_total_rows) AS condition_total_rows,
  SUM(condition_wrong_date_rows) AS condition_wrong_date_rows,
  SUM(drug_total_rows) AS drug_total_rows,
  SUM(drug_wrong_date_rows) AS drug_wrong_date_rows,
  SUM(device_total_rows) AS device_total_rows,
  SUM(device_wrong_date_rows) AS device_wrong_date_rows
FROM
  `{{ehr_ops_project}}.{{ehr_ops_staging_dataset}}.mv_ehr_dc_overall`
GROUP BY 1,2,3