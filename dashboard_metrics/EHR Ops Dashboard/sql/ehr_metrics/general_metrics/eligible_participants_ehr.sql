-- This query returns number of participants with at least one EHR record in visit_occurrence, measurement,
-- procedure_occurrence, drug_exposure, observation and visit_occurrence tables.
SELECT
src_hpo_id,
count(distinct(person_id)) as Participants_With_EHR_Data
        FROM

            (SELECT
                src_hpo_id,
                person_id
            FROM
               `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_condition_occurrence` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT src_hpo_id, condition_occurrence_id
                FROM
                     `{{curation_project}}.{{ehr_ops_dataset}}._mapping_condition_occurrence`)  AS t2
            ON
                t1.condition_occurrence_id=t2.condition_occurrence_id
    UNION ALL
    		SELECT
                src_hpo_id,
                person_id
            FROM
               `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_measurement` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT src_hpo_id, measurement_id
                FROM
                     `{{curation_project}}.{{ehr_ops_dataset}}._mapping_measurement`)  AS t2
            ON
                t1.measurement_id=t2.measurement_id

    UNION ALL
            SELECT
                src_hpo_id,
                person_id
            FROM
               `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_procedure_occurrence` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT src_hpo_id, procedure_occurrence_id
                FROM
                     `{{curation_project}}.{{ehr_ops_dataset}}._mapping_procedure_occurrence`)  AS t2
            ON
                t1.procedure_occurrence_id=t2.procedure_occurrence_id
    UNION ALL
            SELECT
                src_hpo_id,
                person_id
            FROM
               `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_drug_exposure` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT src_hpo_id, drug_exposure_id
                FROM
                     `{{curation_project}}.{{ehr_ops_dataset}}._mapping_drug_exposure`)  AS t2
            ON
                t1.drug_exposure_id=t2.drug_exposure_id
    UNION ALL
            SELECT
                src_hpo_id,
                person_id
            FROM
               `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_observation` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT src_hpo_id, observation_id, src_table_id
                FROM
                     `{{curation_project}}.{{ehr_ops_dataset}}._mapping_observation`)  AS t2
            ON
                t1.observation_id=t2.observation_id
            WHERE t2.src_table_id not like '%person%'
    UNION ALL
            SELECT
                src_hpo_id,
                person_id
            FROM
               `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_visit_occurrence` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT src_hpo_id, visit_occurrence_id
                FROM
                     `{{curation_project}}.{{ehr_ops_dataset}}._mapping_visit_occurrence`)  AS t2
            ON
                t1.visit_occurrence_id=t2.visit_occurrence_id
    )
    GROUP BY
        1