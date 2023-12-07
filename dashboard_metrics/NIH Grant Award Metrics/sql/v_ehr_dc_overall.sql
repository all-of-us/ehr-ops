SELECT * FROM `{{ehr_ops_project}}.{{ehr_ops_staging_dataset}}.mv_ce_dc_overall`
UNION ALL
SELECT * FROM `{{ehr_ops_project}}.{{ehr_ops_staging_dataset}}.mv_hpo_dc_overall`