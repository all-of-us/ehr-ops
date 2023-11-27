SELECT
*
FROM
  `{{ehr_ops_project}}.{{ehr_ops_staging_dataset}}.mv_ce_gc_overall` gc
UNION ALL
SELECT
*
FROM
  `{{ehr_ops_project}}.{{ehr_ops_staging_dataset}}.mv_hpo_gc_overall` gc