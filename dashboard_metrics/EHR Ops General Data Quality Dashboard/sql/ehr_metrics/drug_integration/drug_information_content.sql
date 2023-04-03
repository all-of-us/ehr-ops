-- Calculate the average precedence of drug records submitted
WITH drug_class_ranks AS (
  SELECT * FROM (
    SELECT 1 precendence, drug_class_name FROM UNNEST([
      'Branded Pack', 'Quant Clinical Drug', 'Quant Branded Drug', 'Clinical Drug Box',
      'Branded Drug Box', 'Quant Clinical Box', 'Quant Branded Box', 'Clinical Pack Box',
      'Branded Pack Box', 'Marketed Product'
      ]) AS drug_class_name UNION ALL
    SELECT 1 precedence, 'Quant Clinical Drug' drug_class_name UNION ALL
    SELECT 2 precendence, 'Clinical Pack' drug_class_name UNION ALL
    SELECT 3 precendence, 'Branded Drug' drug_class_name UNION ALL
    SELECT 4 precendence, 'Clinical Drug' drug_class_name UNION ALL
    SELECT 5 precendence, 'Branded Drug Component' drug_class_name UNION ALL
    SELECT 6 precendence, 'Clinical Drug Component' drug_class_name UNION ALL
    SELECT 7 precendence, 'Branded Drug Form' drug_class_name UNION ALL
    SELECT 8 precendence, 'Clinical Drug Form' drug_class_name UNION ALL
    SELECT 9 precendence, 'Ingredient' drug_class_name
  )
)
SELECT
  mde.src_hpo_id, AVG(precendence) average_precendence
FROM `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_drug_exposure` de
JOIN `aou-res-curation-prod.{{ehr_ops_dataset}}._mapping_drug_exposure` mde
  ON mde.drug_exposure_id = de.drug_exposure_id
JOIN `{{curation_project}}.{{ehr_ops_dataset}}.concept` c
  ON c.concept_id = de.drug_concept_id
LEFT JOIN drug_class_ranks dr
  ON dr.drug_class_name = c.concept_class_id
WHERE c.vocabulary_id IN ('RxNorm', 'RxNorm Extension')
GROUP BY mde.src_hpo_id
ORDER BY average_precendence ASC;



SELECT
  c.concept_class_id, COUNT(*)
FROM `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_drug_exposure` de
JOIN `{{curation_project}}.{{ehr_ops_dataset}}.concept` c
  ON c.concept_id = de.drug_concept_id
GROUP BY c.concept_class_id
ORDER BY COUNT(*) DESC;