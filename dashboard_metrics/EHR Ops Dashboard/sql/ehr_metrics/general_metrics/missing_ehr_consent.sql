SELECT
    src_hpo_id,
    Count(distinct(person_id)) as Missing_EHR_Consent_Count,
    sum(withdrawn_before_submission) as withdrawn_before_sub,
    sum(withdrawn_after_submission) as withdrawn_after_sub,
    sum(no_consent_but_submitted) as consent_no_but_submitted,
    sum(removed_consent_since) as consent_removed_since_last_submission
FROM
    (
        SELECT
            person_id,
            src_hpo_id,
            mod_module,
            mod_consent_value,
            mod_authored,
            withdrawal_time,
            person_upload_time,
            CASE
                WHEN withdrawal_time IS NOT NULL
                AND (DATE(withdrawal_time) < DATE(person_upload_time)) THEN 1
                ELSE 0
            END AS withdrawn_before_submission,
            CASE
                WHEN withdrawal_time IS NOT NULL
                AND (DATE(withdrawal_time) > DATE(person_upload_time)) THEN 1
                ELSE 0
            END AS withdrawn_after_submission,
            CASE
                WHEN withdrawal_time is NULL
                and mod_consent_value != 'ConsentPermission_Yes'
                AND (
                    DATE(mod_authored) < DATE(person_upload_time)
                ) THEN 1
                ELSE 0
            END AS no_consent_but_submitted,
            CASE
                WHEN withdrawal_time is NULL
                and mod_consent_value != 'ConsentPermission_Yes'
                AND (
                    DATE(mod_authored) > DATE(person_upload_time)
                ) THEN 1
                ELSE 0
            END AS removed_consent_since,
            rank() over (
                partition by person_id
                order by
                    mod_authored desc
            ) d_order
        FROM
            {{pdr_project}}.{{rdr_ops_dataset}}._mapping_person as ehr
            INNER JOIN `{{pdr_project}}.{{rdr_ops_dataset}}.table_counts_with_upload_timestamp_for_hpo_sites` rsubmission
                ON ehr.src_hpo_id = lower(rsubmission.hpo_id)
            INNER JOIN `{{pdr_project}}.{{rdr_ops_dataset}}.v_pdr_participant_all` pdr_all
                ON ehr.person_id = pdr_all.participant_id
            LEFT JOIN `{{pdr_project}}.{{rdr_ops_dataset}}.v_pdr_participant_module` pdr_cons
                ON person_id = pdr_cons.participant_id
                AND mod_module = 'EHRConsentPII'
    )
WHERE
    d_order = 1
    and (
        mod_consent_value != 'ConsentPermission_Yes'
        or withdrawal_time is not null
    )
Group by
    1
