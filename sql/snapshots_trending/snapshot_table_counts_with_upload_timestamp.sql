SELECT distinct lower(hpo_id) as src_hpo_id, cast(snapshot_ts as date)AS snapshot_ts_dt, 
    cast(person_upload_time as date) as person_upload_dt,
    person, condition_occurrence, procedure_occurrence, drug_exposure, visit_occurrencence as visit_occurrence,
    measurement, observation, device_exposure, death, provider, specimen, location, care_site, note, 
    pii_address, pii_email, pii_mrn, pii_name, pii_phone, participant_match
FROM {{curation_ops_schema}}.snapshot_table_counts_with_upload_timestamp_for_hpo_sites