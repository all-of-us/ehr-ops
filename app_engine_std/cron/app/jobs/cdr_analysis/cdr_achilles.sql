SELECT {{cdr_version}} as cdr_version,
analysis_id, 
regexp_extract(achilles_heel_warning, r'^(ERROR|WARNING|NOTIFICATION)') as analysis_type,
IFNULL(record_count, 0) as count,
regexp_extract(achilles_heel_warning, r'-(.*)$') AS description
FROM `{{curation_project}}.{{unioned_ehr_dataset}}.achilles_heel_results`
where analysis_id is not null
group by 2,3,4,5