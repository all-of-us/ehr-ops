WITH ep AS (
SELECT distinct src_hpo_id, cast(snapshot_ts as date) AS snapshot_ts_dt, Participants_With_EHR_Data
FROM {{curation_ops_schema}}.snapshot_eligible_participants_ehr),

org_hpo AS (
SELECT
    external_id,
    "Org_ID",
    lower(HPO_ID) AS src_hpo_id,
    "Display_Order",
    "On_Boarding_Time"
FROM {{curation_dm_schema}}.v_org_hpo_mapping_temp)

SELECT DISTINCT
    ep.*,
    case when display_name like 'VA%' then 'Veteran Affairs'
        else display_name end as site_name
FROM ep
JOIN org_hpo
ON ep.src_hpo_id = org_hpo.src_hpo_id
JOIN {{pdr_schema}}.mv_organization o
    ON o.external_id = org_hpo."Org_ID"