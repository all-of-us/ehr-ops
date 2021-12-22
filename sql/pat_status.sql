SELECT distinct
p.organization,
    case when o.external_id like 'VA%' then 'VA_BOSTON_VAMC'
    else o.external_id end as external_id,
p.enrollment_status,
CAST(pps.created as date) as created_date,
CAST(pps.patient_status_modified as date) as modified_date,
CAST(pps.patient_status_authored as date) as authored_date,
c.consent_value,
p.participant_id,
rank() over(partition by p.participant_id order by c.consent_module_authored desc) as most_consent_date_rank
FROM pdr.mv_participant p
left join  pdr.mv_participant_patient_status as pps on p.participant_id = pps.participant_id
INNER JOIN pdr.mv_organization as o on p.organization_id = o.organization_id
INNER JOIN pdr.mv_participant_consent as c on p.participant_id = c.participant_id
WHERE c.consent = 'EHRConsentPII_ConsentPermission'
AND (p.withdrawal_status_id = 1 or p.withdrawal_status = 'NOT_WITHDRAWN')



SELECT count ( distinct c.participant_id )
FROM pdr.mv_participant p
left join  pdr.mv_participant_patient_status as pps on p.participant_id = pps.participant_id
INNER JOIN pdr.mv_organization as o on p.organization_id = o.organization_id
INNER JOIN pdr.mv_participant_consent as c on p.participant_id = c.participant_id
WHERE c.consent = 'EHRConsentPII_ConsentPermission'
AND (p.withdrawal_status_id = 1 or p.withdrawal_status = 'NOT_WITHDRAWN')








