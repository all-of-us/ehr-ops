
SELECT 
src_hpo_id,
Count(distinct(person_id)) as Missing_EHR_Consent_Count,
sum(withdrawn_before_submission) as withdrawn_before_sub,
sum(withdrawn_after_submission) as withdrawn_after_sub,
sum(no_consent_but_submitted) as consent_no_but_submitted,
sum(removed_consent_since) as consent_removed_since_last_submission
FROM (
    SELECT 
    person_id,
    src_hpo_id,
    consent_module,
    consent_value,
    consent_module_authored,
    withdrawal_time,
    person_upload_time,
    CASE WHEN withdrawal_time IS NOT NULL AND (DATE(withdrawal_time) < DATE(person_upload_time)) THEN 1 ELSE 0
    END AS withdrawn_before_submission,
    CASE WHEN withdrawal_time IS NOT NULL AND (DATE(withdrawal_time) > DATE(person_upload_time)) THEN 1 ELSE 0
    END AS withdrawn_after_submission,
    CASE WHEN withdrawal_time is NULL and consent_value != 'ConsentPermission_Yes' AND (DATE(consent_module_authored) < DATE(person_upload_time)) THEN 1 ELSE 0
    END AS no_consent_but_submitted,
    CASE WHEN withdrawal_time is NULL and consent_value != 'ConsentPermission_Yes' AND (DATE(consent_module_authored) > DATE(person_upload_time)) THEN 1 ELSE 0
    END AS removed_consent_since,
    rank() over (partition by person_id order by consent_module_authored desc) d_order
    FROM {{curation_ops_schema}}._mapping_person as ehr
    INNER JOIN {{curation_ops_schema}}.table_counts_with_upload_timestamp_for_hpo_sites rsubmission
        ON ehr.src_hpo_id = lower(rsubmission.hpo_id)
    INNER JOIN {{pdr_schema}}.mv_participant_all pdr_all
        ON ehr.person_id = pdr_all.participant_id
    LEFT JOIN {{pdr_schema}}.mv_participant_consent pdr_cons
        ON person_id = pdr_cons.participant_id AND consent_module = 'EHRConsentPII')
WHERE d_order = 1 and (consent_value != 'ConsentPermission_Yes' or withdrawal_time is not null) a
Group by 1