SELECT 
    case when external_id like 'VA%' then 'VA_BOSTON_VAMC'
    else external_id end as external_id,
COUNT(DISTINCT participant_id) AS pts_cnt_yes
FROM
(
SELECT distinct
p.organization,
o.external_id,
ps.patient_status, 
c.consent_value,
p.participant_id,
rank() over(partition by p.participant_id order by ps.patient_status_modified desc) as patient_status_date_rank,
rank() over(partition by p.participant_id order by c.consent_module_authored desc) as most_consent_date_rank
FROM {{pdr_schema}}.mv_participant_all p
LEFT JOIN {{pdr_schema}}.mv_participant_patient_status as ps
ON p.participant_id = ps.participant_id
INNER JOIN {{pdr_schema}}.mv_organization as o on p.organization_id = o.organization_id
INNER JOIN {{pdr_schema}}.mv_participant_consent as c on p.participant_id = c.participant_id
WHERE c.consent = 'EHRConsentPII_ConsentPermission'
AND (p.withdrawal_status_id = 1 or p.withdrawal_status = 'NOT_WITHDRAWN')
)a 
WHERE a.patient_status_date_rank = 1
AND a.most_consent_date_rank = 1
AND consent_value = 'ConsentPermission_Yes'
AND patient_status = 'YES'

GROUP BY 1