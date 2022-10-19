WITH drug_wide_net AS (
    SELECT
        dc.drug_class_name,
        dc.concept_id drug_class_concept_id,
        c.concept_id drug_concept_id,
        c.concept_name drug_concept_name,
        c.concept_class_id,
        c.vocabulary_id
    FROM
        `{{curation_project}}.{{ehr_ops_dataset}}.concept` c
        JOIN `{{curation_project}}.{{ehr_ops_dataset}}.concept_ancestor` ca ON ca.descendant_concept_id = c.concept_id
        JOIN `{{curation_project}}.{{ehr_ops_dataset}}.drug_class` dc ON dc.concept_id = ca.ancestor_concept_id
    WHERE
        c.vocabulary_id IN ('RxNorm', 'RxNorm Extension')
)
SELECT
    drug_class_name,
    AVG(
        DATE_DIFF(
            de.drug_exposure_end_date,
            de.drug_exposure_start_date,
            YEAR
        )
    ) mean_drug_duration_days
FROM
    `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_drug_exposure` de
    JOIN drug_wide_net dwn ON dwn.drug_concept_id = de.drug_concept_id
GROUP BY
    drug_class_name
ORDER BY
    mean_drug_duration_days DESC