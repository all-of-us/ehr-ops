--- This query is to check number of participants who are eligible for EHR data submission
--- * Note: We merge all VA organizations together as one HPO

SELECT distinct
    case when external_id like 'VA%' then 'VA_BOSTON_VAMC'
    else external_id end as external_id,
    awardee,
    case when display_name like 'VA%' then 'Veteran Affairs'
    else display_name end as org,
    COUNT(DISTINCT(participant_id)) AS all_eligible_participants
FROM(
    select ps.participant_id,
    o.external_id,
    h.display_name as awardee,
    o.display_name,
    c.mod_consent_value,
    mod_authored,
    rank() over(partition by ps.participant_id order by c.mod_authored desc) as most_consent_date_rank
    FROM `{{pdr_project}}.{{rdr_ops_dataset}}.v_pdr_participant`  as ps
    INNER JOIN `{{pdr_project}}.{{rdr_ops_dataset}}.v_pdr_participant` as p on ps.participant_id = p.participant_id
    INNER JOIN `{{pdr_project}}.{{rdr_ops_dataset}}.v_organization` as o on ps.organization_id = o.organization_id
    INNER JOIN `{{pdr_project}}.{{rdr_ops_dataset}}.v_hpo` as h on ps.hpo_id = h.hpo_id
    INNER JOIN `{{pdr_project}}.{{rdr_ops_dataset}}.v_pdr_participant_module` as c on ps.participant_id = c.participant_id
    WHERE  p.is_ghost_id = 0
    AND (p.hpo_id != 21)
    AND (ps.withdrawal_status_id = 1 or ps.withdrawal_status = 'NOT_WITHDRAWN')
    AND c.mod_module = 'EHRConsentPII'
    GROUP BY 1,2,3,4,5,mod_authored
    ORDER BY ps.participant_id) a
where a.most_consent_date_rank = 1
and a.mod_consent_value = 'ConsentPermission_Yes'
group by 1,2,3