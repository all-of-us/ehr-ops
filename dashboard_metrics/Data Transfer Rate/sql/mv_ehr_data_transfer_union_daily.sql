create materialized view if not exists {{curation_dm_schema}}.mv_ehr_data_transfer_union_daily as
WITH current_table AS (
    SELECT DISTINCT report_run_time,
                    hpo_id,
                    org_id,
                    site_name,
                    person
    FROM {{curation_ops_schema}}.snapshot_table_counts_with_upload_timestamp_for_hpo_sites
)
SELECT a.org_id                                    AS external_id,
       org.display_name                            AS org_name,
       hpo.display_name                            AS awardee_name,
       date(a.report_run_time)                     AS date,
       date_part('year'::text, a.report_run_time)  AS year,
       date_part('month'::text, a.report_run_time) AS month,
       date_part('day'::text, a.report_run_time)   AS day,
       a.person                                    AS person_count,
       a.person_change                             AS diff
FROM (SELECT site_name,
             report_run_time,
             org_id,
             person,
             person - lag(person)
                                    OVER (PARTITION BY site_name ORDER BY report_run_time) AS person_change
      FROM current_table
      ORDER BY site_name, report_run_time) a
         LEFT JOIN pdr.mv_organization org ON a.org_id::text = org.external_id::text
         LEFT JOIN pdr.mv_hpo hpo ON hpo.hpo_id = org.hpo_id
WHERE a.person_change IS NOT NULL
UNION
SELECT external_id,
       org_name,
       awardee_name,
       date,
       year,
       month,
       day,
       person_count,
       diff
FROM {{curation_dm_schema}}.ehr_data_transfer_historical
WHERE ehr_data_transfer_historical.date <= '2021-06-08'::date
ORDER BY 2, 4;
