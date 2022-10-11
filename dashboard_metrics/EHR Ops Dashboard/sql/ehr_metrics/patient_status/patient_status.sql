SELECT
    distinct p.organization,
    case
        when o.external_id like 'VA%' then 'VA_BOSTON_VAMC'
        else o.external_id
    end as external_id,
    ps.patient_status,
    cast(ps.patient_status_created as date) as created_date,
    cast(ps.patient_status_modified as date) as modified_date,
    cast(ps.patient_status_authored as date) as authored_date,
    c.consent_value,
    p.participant_id,
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