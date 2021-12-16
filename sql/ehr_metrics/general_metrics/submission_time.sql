select distinct 
lower(hpo_id) as src_hpo_id, 
display_order, 
person_upload_time 
from {{curation_ops_schema}}.table_counts_with_upload_timestamp_for_hpo_sites