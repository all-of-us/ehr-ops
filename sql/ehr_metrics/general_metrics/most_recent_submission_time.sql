--- This query is to return the most data recent submission time of each site
select distinct
lower(hpo_id) as src_hpo_id,
display_order,
person_upload_time
from `{{pdr_project}}.{{curation_dataset}}.table_counts_with_upload_timestamp_for_hpo_sites`