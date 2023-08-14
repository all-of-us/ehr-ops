Select participant_id, ORGANIZATION, withdrawal_flag, ehr_consent_yes_flag, LOWER(mapping.HPO_ID) as HPO_ID 
from `{{ehr_ops_project}}.{{ehr_ops_staging_dataset}}.mv_ehr_rdr_participant` mv_ehr_rdr_participant 
join `{{ehr_ops_project}}.{{ehr_ops_staging_dataset}}.mv_org_hpo_mapping` mapping
ON mapping.Org_ID = mv_ehr_rdr_participant.ORGANIZATION
where (ehr_consent_yes_flag !=1 or TIMESTAMP(withdrawal_time) < mv_ehr_rdr_participant.person_upload_time) 
and (person_flag = 1 or condition_flag = 1 or device_flag = 1 or death_flag = 1 or drug_flag = 1 or measurement_flag = 1 or procedure_flag = 1 or observation_flag = 1 or visit_flag = 1 or speciment_flag = 1)
and LOWER(mapping.HPO_ID) in UNNEST(@included_hpos)
