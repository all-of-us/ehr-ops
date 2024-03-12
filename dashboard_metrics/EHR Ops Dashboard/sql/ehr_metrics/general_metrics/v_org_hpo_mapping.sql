-- This query returns the org_id linkage between CDR and PDR
WITH
  bucket_create AS (
  -- SELECT
  --   resource.labels.bucket_name,
  --   MAX(timestamp) AS bucket_created_timestamp
  -- FROM
  --   `{{rdr_project}}.GcsBucketLogging.cloudaudit_googleapis_com_activity_20*`
  -- WHERE
  --   resource.type = 'gcs_bucket'
  --   AND protopayload_auditlog.methodName = 'storage.buckets.create'
  --   AND resource.labels.bucket_name LIKE 'aou%'
  -- GROUP BY
  --   1
  SELECT * FROM `{{ehr_ops_project}}.{{ehr_ops_resources_dataset}}.hpo_onboarding`),

updated_hpo_site_id_mappings AS
(
SELECT
  CASE WHEN Org_ID = 'COSSMA_PR' THEN 'COSSMA_63K3' ---build the linkage between RDR and EHR for COSSMA organization
  ELSE Org_ID
  END AS Org_ID,
  HPO_ID,
  Display_Order
FROM
  `{{curation_project}}.{{lookup_dataset}}.hpo_site_id_mappings`
 )

SELECT coalesce(o.external_id, m.Org_ID) as external_id,
m.Org_ID,
m.HPO_ID,
m.Display_Order,
b.bucket_created_timestamp AS On_Boarding_Time
FROM updated_hpo_site_id_mappings m
LEFT JOIN
  `{{pdr_project}}.{{rdr_ops_dataset}}.v_organization` o
ON
  o.external_id = m.Org_ID
LEFT JOIN
  `{{curation_project}}.{{lookup_dataset}}.hpo_id_bucket_name` h
ON
  m.HPO_ID = h.hpo_id
LEFT JOIN
  bucket_create b
USING
  (bucket_name)
WHERE m.Org_ID NOT LIKE '%FHIR%'
AND b.bucket_created_timestamp IS NOT NULL