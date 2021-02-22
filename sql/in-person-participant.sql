--- This query is to check number of participants with in-person visit (either physical measurement or biospecimen has been measured/collected on spot)
SELECT external_id,
Organization,
COUNT(DISTINCT(participant_id)) AS Eligible_Participants_with_an_In_Person_Visit
FROM (
SELECT
    o.external_id,
    o.display_name as Organization,
    ps.participant_id,
    rank() over(partition by ps.participant_id order by c.consent_module_authored desc) as most_consent_date_rank
    FROM `{{pdr_project}}.{{rdr_ops_dataset}}.v_pdr_participant`  as ps
    INNER JOIN `{{pdr_project}}.{{rdr_ops_dataset}}.v_organization` as o on ps.organization_id = o.organization_id
    INNER JOIN `{{pdr_project}}.{{rdr_ops_dataset}}.v_hpo` as h on ps.hpo_id = h.hpo_id
    INNER JOIN `{{pdr_project}}.{{rdr_ops_dataset}}.v_pdr_biospec` as bio on ps.participant_id = bio.participant_id
    INNER JOIN `{{pdr_project}}.{{rdr_ops_dataset}}.v_pdr_participant_consent` as c on ps.participant_id = c.participant_id
    INNER JOIN `{{pdr_project}}.{{rdr_ops_dataset}}.v_pdr_participant_pm` as pm on ps.participant_id = pm.participant_id
    WHERE (ps.withdrawal_status_id = 1 or ps.withdrawal_status = 'NOT_WITHDRAWN')
    AND (c.consent = 'EHRConsentPII_ConsentPermission' AND c.consent_value = 'ConsentPermission_Yes')
    AND (pm.pm_status_id=1 OR bio.biosp_baseline_tests_confirmed >= 1)
    GROUP BY 1,2,3,consent_module_authored
    ) a
WHERE most_consent_date_rank = 1
GROUP BY 1,2