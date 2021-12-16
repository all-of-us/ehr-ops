SELECT 
    case when external_id like 'VA%' then 'VA_BOSTON_VAMC'
    else external_id end as external_id,
COUNT(DISTINCT participant_id) AS pts_cnt_no
FROM
(
SELECT distinct
p.organization,
o.external_id,
ps.patient_status, 
p.participant_id,
rank() over(partition by p.participant_id order by ps.patient_status_modified desc) as patient_status_date_rank
FROM {{pdr_schema}}.mv_participant_all p
LEFT JOIN {{pdr_schema}}.mv_participant_patient_status as ps
ON p.participant_id = ps.participant_id
INNER JOIN {{pdr_schema}}.mv_organization as o on p.organization_id = o.organization_id
)a 
WHERE a.patient_status_date_rank = 1
AND patient_status = 'NO'

GROUP BY 1