-- this view has been built in curation dataset and copied as a table in PDR
WITH
  bucket_create AS (
  SELECT
    resource.labels.bucket_name,
    MAX(timestamp) AS bucket_created_timestamp
  FROM
    `{{rdr_project}}.{{gcs_logging}}.cloudaudit_googleapis_com_activity_20*`
  WHERE
    resource.type = 'gcs_bucket'
    AND protopayload_auditlog.methodName = 'storage.buckets.create'
    AND resource.labels.bucket_name LIKE 'aou%'
  GROUP BY
    1)
SELECT
  external_id,
  m.Org_ID,
  m.HPO_ID,
  m.Display_Order,
  b.bucket_created_timestamp AS On_Boarding_Time
FROM
  `{{pdr_prject}}.{{rdr_ops_dataset}}.v_organization` o
JOIN
  `{{curation_prject}}.{{lookup_dataset}}.hpo_site_id_mappings` m
ON
  o.external_id = m.Org_ID
JOIN
  `{{curation_prject}}.{{lookup_dataset}}.hpo_id_bucket_name` h
ON
  m.HPO_ID = h.hpo_id
JOIN
  bucket_create b
USING
  (bucket_name)