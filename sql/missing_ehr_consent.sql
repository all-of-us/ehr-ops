SELECT
src_hpo_id,
Count(distinct(person_id)) as Missing_EHR_Consent_Count,
sum(withdrawn) as withdrawn_status,
sum(no_value_consent) as wrong_consent
FROM (
    SELECT
    person_id,
    src_hpo_id,
    consent_module,
    consent_value,
    consent_module_authored,
    withdrawal_time,
    CASE WHEN withdrawal_time IS NOT NULL THEN 1 ELSE 0
        END AS withdrawn,
    CASE WHEN withdrawal_time is NULL and consent_value != 'ConsentPermission_Yes' THEN 1 ELSE 0
        END AS no_value_consent,
    rank() over (partition by person_id order by consent_module_authored desc) d_order
        FROM {{pdr_project}}.{{rdr_ops_dataset}}._mapping_person as ehr
        INNER JOIN `{{pdr_project}}.{{rdr_ops_dataset}}.v_pdr_participant_all` pdr_all
        ON ehr.person_id = pdr_all.participant_id
        LEFT JOIN `{{pdr_project}}.{{rdr_ops_dataset}}.v_pdr_participant_consent` pdr_cons
        ON person_id = pdr_cons.participant_id
        WHERE consent_module ='EHRConsentPII')
WHERE d_order = 1 and consent_value != 'ConsentPermission_Yes'
GROUP BY 1