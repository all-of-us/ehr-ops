Select participant_id, ORGANIZATION, withdrawal_flag, ehr_consent_yes_flag   
from `aou-ehr-ops-curation-prod.ehr_ops_metrics_staging.mv_ehr_rdr_participant` mv_ehr_rdr_participant 
join `aou-ehr-ops-curation-prod.ehr_ops_metrics_staging.`
where (ehr_consent_yes_flag !=1 or TIMESTAMP(withdrawal_time) > mv_ehr_rdr_participant.person_upload_time) 
and (person_flag = 1 or condition_flag = 1 or device_flag = 1 or death_flag = 1 or drug_flag = 1 or measurement_flag = 1 or procedure_flag = 1 or observation_flag = 1 or visit_flag = 1 or speciment_flag = 1)