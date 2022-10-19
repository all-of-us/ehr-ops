-- Drug counts per ingredient
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
),
drug_counts AS (
    SELECT
        mde.src_hpo_id,
        dwn.drug_class_name,
        COUNT(DISTINCT de.drug_exposure_id) drug_count
    FROM
        `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_drug_exposure` de
        JOIN `{{curation_project}}.{{ehr_ops_dataset}}._mapping_drug_exposure` mde ON mde.drug_exposure_id = de.drug_exposure_id
        JOIN drug_wide_net dwn ON dwn.drug_concept_id = de.drug_concept_id
    GROUP BY
        mde.src_hpo_id,
        dwn.drug_class_name
)
SELECT
    m.HPO_ID,
    dc.drug_class_name,
    IF(cnt.drug_count IS NULL, 0, cnt.drug_count) drug_count
FROM
    `{{curation_project}}.operations_analytics.v_org_hpo_mapping` m
    CROSS JOIN `{{curation_project}}.{{ehr_ops_dataset}}.drug_class` dc
    LEFT JOIN drug_counts cnt ON cnt.src_hpo_id = LOWER(m.HPO_ID)
    AND cnt.drug_class_name = dc.drug_class_name
ORDER BY
    m.HPO_ID,
    dc.drug_class_name