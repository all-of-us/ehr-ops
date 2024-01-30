WITH
  most_recent_submission AS (
  SELECT
    *
  FROM
    `{{ehr_ops_project}}.{{ehr_ops_staging_dataset}}.mv_org_hpo_mapping` org
  LEFT JOIN
    `{{curation_project}}.{{operations_analytics_dataset}}.table_counts_with_upload_timestamp_for_hpo_sites` tc
  ON
    tc.org_id = org.external_id
    AND tc.hpo_id IS NOT NULL
    AND tc.hpo_id != 'COOK_COUNTY' ),
  care_evolution_person_list AS (
  SELECT
    DISTINCT person_id
  FROM
    `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_person`
  JOIN
    `{{curation_project}}.{{ehr_ops_dataset}}._mapping_person`
  USING
    (person_id)
  WHERE
    src_hpo_id = 'care_evolution_omop_dv'),
  participant_with_org AS (
  SELECT
    participant_id,
    ORGANIZATION,
    organization_id,
    hpo,
    hpo_id,
    withdrawal_status,
    withdrawal_time,
    CASE
      WHEN withdrawal_time IS NOT NULL THEN 1
    ELSE
    0
  END
    AS withdrawal_flag
  FROM
    `{{pdr_project}}.{{rdr_ops_dataset}}.v_pdr_participant_all`
  WHERE
    is_ghost_id = 0
    --AND suspension_status_id = 1
    AND ORGANIZATION IS NOT NULL
    AND hpo != 'TEST'),
  participant_care_evolution AS (
  SELECT
    participant_id,
    'CARE_EVOLUTION_OMOP_DV' AS ORGANIZATION,
    organization_id,
    hpo,
    hpo_id,
    withdrawal_status,
    withdrawal_time,
    CASE
      WHEN withdrawal_time IS NOT NULL THEN 1
    ELSE
    0
  END
    AS withdrawal_flag
  FROM
    `{{pdr_project}}.{{rdr_ops_dataset}}.v_pdr_participant_all` ppa
  JOIN
    care_evolution_person_list ce
  ON
    ppa.participant_id = ce.person_id
  WHERE
    is_ghost_id = 0
    --AND suspension_status_id = 1
    AND hpo != 'TEST' )

SELECT
  DISTINCT ppa.participant_id,
  ppa.ORGANIZATION,
  CASE
    WHEN ppa.hpo = 'UNSET' AND ppa.ORGANIZATION = 'CARE_EVOLUTION_OMOP_DV' THEN 'CARE_EVOLUTION_OMOP_DV'
  ELSE
  ppa.hpo END AS hpo,
  CASE
    WHEN ppa.hpo = 'UNSET' AND ppa.ORGANIZATION = 'CARE_EVOLUTION_OMOP_DV' THEN 'Care Evolution'
  ELSE
  h.display_name END AS hpo_display_name,
  CASE
    WHEN o.display_name IS NULL AND ppa.ORGANIZATION = 'CARE_EVOLUTION_OMOP_DV' THEN 'Care Evolution'
  ELSE
  o.display_name
END
  AS org_display_name,
  CASE
    WHEN o.external_id IS NULL AND ppa.ORGANIZATION = 'CARE_EVOLUTION_OMOP_DV' THEN 'CARE_EVOLUTION_OMOP_DV'
  ELSE
  o.external_id END AS external_id,
  ppa.withdrawal_status,
  pss.patient_status,
  pm.pm_status,
  bio.bbo_collection_method,
  ppa.withdrawal_time,
  CASE
    WHEN ppa.withdrawal_time IS NOT NULL THEN 1
  ELSE
  0
END
  AS withdrawal_flag,
  ehr_consent_yes_flag,
  ehr_expired_consent_flag,
  ef.*,
  ms.person_upload_time
FROM (
  SELECT
    *
  FROM
    participant_with_org
  UNION DISTINCT
  SELECT
    *
  FROM
    participant_care_evolution) ppa
LEFT JOIN (
  SELECT
    DISTINCT participant_id,
    patient_status,
    RANK() OVER (PARTITION BY ps.participant_id ORDER BY patient_status_modified DESC) ps_order
  FROM
    `{{pdr_project}}.{{rdr_ops_dataset}}.v_pdr_participant_patient_status` ps) pss
ON
  ppa.participant_id = pss.participant_id
  AND ps_order = 1
LEFT JOIN
    `{{pdr_project}}.{{rdr_ops_dataset}}.v_pdr_participant_pm` pm
ON
  ppa.participant_id = pm.participant_id
  AND pm.pm_status = 'COMPLETED'
LEFT JOIN
-- find the biobank collection method
     (SELECT participant_id,
             bbo_collection_method
      FROM `{{pdr_project}}.{{rdr_ops_dataset}}.v_pdr_participant_biobank_order`
      ) bio
ON
  ppa.participant_id = bio.participant_id
LEFT JOIN (
  SELECT
    pdr_cons.participant_id,
    CASE
      WHEN mod_consent_value = 'ConsentPermission_Yes' THEN 1
    ELSE
    0
  END
    AS ehr_consent_yes_flag,
    CASE
      WHEN mod_consent_expired IS NOT NULL THEN 1
    ELSE
    0
  END
    AS ehr_expired_consent_flag,
    RANK() OVER (PARTITION BY pdr_cons.participant_id ORDER BY mod_authored DESC) d_order
  FROM
    `{{pdr_project}}.{{rdr_ops_dataset}}.v_pdr_participant_module` pdr_cons
  WHERE
    mod_module = 'EHRConsentPII' ) consent
ON
  ppa.participant_id = consent.participant_id
  AND d_order = 1
LEFT JOIN
  `{{ehr_ops_project}}.{{ehr_ops_resources_dataset}}.v_ehr_flag` ef
ON
  ppa.participant_id = ef.person_id
  AND ((ppa.ORGANIZATION != 'CARE_EVOLUTION_OMOP_DV' AND ef.ORG_ID != 'CARE_EVOLUTION_OMOP_DV')
  OR (ppa.ORGANIZATION = 'CARE_EVOLUTION_OMOP_DV' AND ppa.ORGANIZATION = ef.ORG_ID))
LEFT JOIN
  `{{pdr_project}}.{{rdr_ops_dataset}}.v_hpo` h
ON
  ppa.hpo_id = h.hpo_id
LEFT JOIN
  `{{pdr_project}}.{{rdr_ops_dataset}}.v_organization` o
ON
  ppa.organization_id = o.organization_id
LEFT JOIN
  most_recent_submission ms
ON
  o.external_id = ms.external_id