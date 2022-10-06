WITH in_person_participant AS (
    SELECT
        case
            when external_id like 'VA%' then 'VA_BOSTON_VAMC'
            else external_id
        end as external_id,
        organization,
        participant_id
    FROM
(
            SELECT
                o.external_id,
                o.display_name as organization,
                ps.participant_id,
                c.consent_value,
                c.consent_expired,
                rank() over(
                    partition by ps.participant_id
                    order by
                        c.consent_module_authored desc
                ) as most_consent_date_rank
            FROM
                `{{pdr_project}}.{{rdr_dataset}}.v_pdr_participant` as ps
                INNER JOIN `{{pdr_project}}.{{rdr_dataset}}.v_organization` as o on ps.organization_id = o.organization_id
                INNER JOIN `{{pdr_project}}.{{rdr_dataset}}.v_hpo` as h on ps.hpo_id = h.hpo_id
                INNER JOIN `{{pdr_project}}.{{rdr_dataset}}.v_pdr_participant_consent` as c on ps.participant_id = c.participant_id
                LEFT JOIN `{{pdr_project}}.{{rdr_dataset}}.v_pdr_biospec` as bio on ps.participant_id = bio.participant_id
                LEFT JOIN `{{pdr_project}}.{{rdr_dataset}}.v_pdr_participant_pm` as pm on ps.participant_id = pm.participant_id
            WHERE
                (
                    ps.withdrawal_status_id = 1
                    or ps.withdrawal_status = 'NOT_WITHDRAWN'
                )
                AND (c.consent_module = 'EHRConsentPII')
                AND (
                    pm.pm_status_id = 1
                    OR bio.biosp_baseline_tests_confirmed >= 1
                )
        ) a
    WHERE
        a.most_consent_date_rank = 1
        AND a.consent_value = 'ConsentPermission_Yes'
        AND a.consent_expired is null
),
patient_status_yes AS (
    SELECT
        case
            when external_id like 'VA%' then 'VA_BOSTON_VAMC'
            else external_id
        end as external_id,
        organization,
        participant_id
    FROM
        (
            SELECT
                distinct p.organization,
                o.external_id,
                ps.patient_status,
                c.consent_value,
                p.participant_id,
                rank() over(
                    partition by p.participant_id
                    order by
                        ps.patient_status_modified desc
                ) as patient_status_date_rank,
                rank() over(
                    partition by p.participant_id
                    order by
                        c.consent_module_authored desc
                ) as most_consent_date_rank
            FROM
                `{{pdr_project}}.{{rdr_dataset}}.pdr_participant` p,
                UNNEST(patient_statuses) as ps
                INNER JOIN `{{pdr_project}}.{{rdr_dataset}}.v_organization` as o on p.organization_id = o.organization_id
                INNER JOIN `{{pdr_project}}.{{rdr_dataset}}.v_pdr_participant_consent` as c on p.participant_id = c.participant_id
            WHERE
                c.consent_module = 'EHRConsentPII'
                AND (
                    p.withdrawal_status_id = 1
                    or p.withdrawal_status = 'NOT_WITHDRAWN'
                )
        ) a
    WHERE
        a.patient_status_date_rank = 1
        AND a.most_consent_date_rank = 1
        AND consent_value = 'ConsentPermission_Yes'
        AND patient_status = "YES"
)
SELECT
    ipp.external_id,
    count(
        distinct case
            when ipp.participant_id is not null
            and psy.participant_id is not null then ipp.participant_id
        end
    ) as in_person_patient,
    count(
        distinct case
            when ipp.participant_id is null
            and psy.participant_id is not null then psy.participant_id
        end
    ) as patient_only,
    count(
        distinct case
            when ipp.participant_id is not null
            and psy.participant_id is null then ipp.participant_id
        end
    ) as in_person_only
FROM
    in_person_participant ipp FULL
    OUTER JOIN patient_status_yes psy on ipp.participant_id = psy.participant_id
    and ipp.external_id = psy.external_id
WHERE
    ipp.participant_id is not null
    AND psy.participant_id is not null
GROUP BY
    1