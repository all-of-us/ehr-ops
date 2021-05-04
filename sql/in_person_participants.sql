--- This query is to check number of participants with in-person visit (either physical measurement or biospecimen has been measured/collected on spot)
SELECT
    CASE WHEN external_id LIKE 'VA%' THEN 'VA_BOSTON_VAMC'
    ELSE external_id END AS external_id,
    CASE WHEN Organization LIKE 'VA%' THEN 'Veteran Affairs'
    ELSE Organization END AS Organization,
COUNT(DISTINCT(participant_id)) AS Eligible_Participants_with_an_In_Person_Visit
FROM (
SELECT
    o.external_id,
    o.display_name AS Organization,
    ps.participant_id,
    rank() OVER(PARTITION BY ps.participant_id ORDER BY c.consent_module_authored DESC) AS most_consent_date_rank
    FROM `{{pdr_project}}.{{rdr_ops_dataset}}.v_pdr_participant`  AS ps
    INNER JOIN `{{pdr_project}}.{{rdr_ops_dataset}}.v_organization` AS o ON ps.organization_id = o.organization_id
    INNER JOIN `{{pdr_project}}.{{rdr_ops_dataset}}.v_hpo` AS h ON ps.hpo_id = h.hpo_id
    INNER JOIN `{{pdr_project}}.{{rdr_ops_dataset}}.v_pdr_biospec` AS bio ON ps.participant_id = bio.participant_id
    INNER JOIN `{{pdr_project}}.{{rdr_ops_dataset}}.v_pdr_participant_consent` AS c ON ps.participant_id = c.participant_id
    INNER JOIN `{{pdr_project}}.{{rdr_ops_dataset}}.v_pdr_participant_pm` AS pm ON ps.participant_id = pm.participant_id
    WHERE (ps.withdrawal_status_id = 1 OR ps.withdrawal_status = 'NOT_WITHDRAWN')
    AND (c.consent = 'EHRConsentPII_ConsentPermission' AND c.consent_value = 'ConsentPermission_Yes')
    AND (pm.pm_status_id=1 OR bio.biosp_baseline_tests_confirmed >= 1)
    GROUP BY 1,2,3,consent_module_authored
    ) a
WHERE most_consent_date_rank = 1
GROUP BY 1,2