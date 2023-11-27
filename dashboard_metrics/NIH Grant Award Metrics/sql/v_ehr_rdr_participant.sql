--TODO: Create a separate table to host CE data only
WITH most_recent_submission as (
  SELECT *
  FROM
  `{{curation_project}}.{{operations_analytics_dataset}}.v_org_hpo_mapping` org
  LEFT JOIN `{{curation_project}}.{{operations_analytics_dataset}}.table_counts_with_upload_timestamp_for_hpo_sites` tc
  ON tc.org_id = org.external_id
  AND tc.hpo_id IS NOT NULL
  AND tc.hpo_id != 'COOK_COUNTY'
)

SELECT
  DISTINCT ppa.participant_id,
  ppa.ORGANIZATION,
  ppa.hpo,
  h.display_name AS hpo_display_name,
  o.display_name AS org_display_name,
  o.external_id,
  ppa.withdrawal_status,
  pss.patient_status,
  pm.pm_status,
  bio.biosp_baseline_tests_confirmed as onsite_collections,
  ppa.withdrawal_time,
  CASE WHEN ppa.withdrawal_time IS NOT NULL THEN 1 ELSE 0 END AS withdrawal_flag,
  ehr_consent_yes_flag,
  ehr_expired_consent_flag,
  ef.*,
  ms.person_upload_time
FROM
  `{{pdr_project}}.{{rdr_ops_dataset}}.v_pdr_participant_all` ppa
LEFT JOIN
(SELECT DISTINCT
    participant_id,
    patient_status,
    rank() over (partition by ps.participant_id order by patient_status_modified desc) ps_order
FROM
  `{{pdr_project}}.{{rdr_ops_dataset}}.v_pdr_participant_patient_status` ps) pss
ON
  ppa.participant_id = pss.participant_id AND ps_order = 1
LEFT JOIN
`{{pdr_project}}.{{rdr_ops_dataset}}.v_pdr_participant_pm` pm
ON ppa.participant_id = pm.participant_id
LEFT JOIN
`{{pdr_project}}.{{rdr_ops_dataset}}.v_pdr_biospec` bio
ON ppa.participant_id = bio.participant_id
LEFT JOIN
(SELECT
   pdr_cons.participant_id,
   CASE WHEN consent_value = 'ConsentPermission_Yes' THEN 1 ELSE 0 END AS ehr_consent_yes_flag,
   CASE WHEN consent_expired IS NOT NULL THEN 1 ELSE 0 END AS ehr_expired_consent_flag,
   rank() over (partition by pdr_cons.participant_id order by consent_module_authored desc) d_order
   FROM `{{pdr_project}}.{{rdr_ops_dataset}}.v_pdr_participant_consent` pdr_cons
   WHERE consent_module = 'EHRConsentPII'
) consent
ON ppa.participant_id = consent.participant_id AND d_order = 1
LEFT JOIN
`{{ehr_ops_project}}.{{ehr_ops_resources_dataset}}.v_ehr_flag` ef
ON ppa.participant_id = ef.person_id
LEFT JOIN
  `{{pdr_project}}.{{rdr_ops_dataset}}.v_hpo` h
ON
  ppa.hpo_id = h.hpo_id
LEFT JOIN
  `{{pdr_project}}.{{rdr_ops_dataset}}.v_organization` o
ON
  ppa.organization_id = o.organization_id
LEFT JOIN
  most_recent_submission ms ON o.external_id = ms.external_id
WHERE ppa.is_ghost_id = 0 and suspension_status_id = 1
AND ORGANIZATION IS NOT NULL AND hpo != 'TEST'