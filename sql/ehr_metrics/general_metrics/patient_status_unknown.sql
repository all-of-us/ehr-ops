SELECT 
    case when external_id like 'VA%' then 'VA_BOSTON_VAMC'
    else external_id end as external_id,
COUNT(DISTINCT participant_id) AS pts_cnt_unknown,
FROM
(
SELECT distinct
p.organization,
o.external_id,
ps.patient_status, 
p.participant_id,
rank() over(partition by p.participant_id order by ps.patient_status_modified desc) as patient_status_date_rank,
FROM `{{pdr_project}}.{{rdr_dataset}}.pdr_participant` p, UNNEST(patient_statuses) as ps
INNER JOIN `{{pdr_project}}.{{rdr_dataset}}.v_organization` as o on p.organization_id = o.organization_id
)a 
WHERE a.patient_status_date_rank = 1
AND patient_status = "UNKNOWN"

GROUP BY 1