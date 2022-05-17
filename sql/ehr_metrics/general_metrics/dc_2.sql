--- This query is to check DC-2 metric
--- DC-2 metric is checking if any date exists beyond 30 days of death date
with visit_death_date as (
    SELECT
        src_hpo_id,
        COUNT(distinct t1.visit_occurrence_id) AS total,
        COUNT(distinct case when (DATE_DIFF(visit_start_date, death_date, DAY)>30) then t1.visit_occurrence_id end) as wrong_visit_start_death_date,
        COUNT(distinct case when (DATE_DIFF(visit_end_date, death_date, DAY)>30) then t1.visit_occurrence_id end) as wrong_visit_end_death_date
    FROM
       `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_visit_occurrence` AS t1
    INNER JOIN
        `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_death` AS t2
    ON
        t1.person_id=t2.person_id
    INNER JOIN
        (select src_hpo_id,
            visit_occurrence_id
            from `{{curation_project}}.{{ehr_ops_dataset}}._mapping_visit_occurrence`)  AS t3
    ON
        t1.visit_occurrence_id=t3.visit_occurrence_id
    GROUP BY
        1),
    condition_death_date as (
    SELECT
        src_hpo_id,
        COUNT(distinct t1.condition_occurrence_id) AS total,
        COUNT(distinct case when (DATE_DIFF(condition_start_date, death_date, DAY)>30) then t1.condition_occurrence_id end) as wrong_condition_start_death_date,
        COUNT(distinct case when (DATE_DIFF(condition_end_date, death_date, DAY)>30) then t1.condition_occurrence_id end) as wrong_condition_end_death_date,
    FROM
       `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_condition_occurrence` AS t1
    INNER JOIN
        `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_death` AS t2
        ON
            t1.person_id=t2.person_id
    INNER JOIN
        (select src_hpo_id,
        condition_occurrence_id
        from `{{curation_project}}.{{ehr_ops_dataset}}._mapping_condition_occurrence`)  AS t3
    ON
        t1.condition_occurrence_id=t3.condition_occurrence_id
    GROUP BY
        1),
    drug_death_date as (
    SELECT
        src_hpo_id,
        COUNT(distinct t1.drug_exposure_id) AS total,
        COUNT(distinct case when (DATE_DIFF(drug_exposure_start_date, death_date, DAY)>30) then t1.drug_exposure_id end) as wrong_drug_start_death_date,
        COUNT(distinct case when (DATE_DIFF(drug_exposure_end_date, death_date, DAY)>30) then t1.drug_exposure_id end) as wrong_drug_end_death_date
    FROM
       `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_drug_exposure` AS t1
    INNER JOIN
        `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_death` AS t2
        ON
            t1.person_id=t2.person_id
    INNER JOIN
        (select src_hpo_id,
            drug_exposure_id
            from `{{curation_project}}.{{ehr_ops_dataset}}._mapping_drug_exposure`) AS t3
    ON
        t1.drug_exposure_id=t3.drug_exposure_id
    GROUP BY
        1),
    measurement_death_date as (
    SELECT
        src_hpo_id,
        COUNT(distinct t1.measurement_id) AS total,
        COUNT(distinct case when (DATE_DIFF(measurement_date, death_date, DAY)>30) then t1.measurement_id end) as wrong_measurement_death_date
    FROM
       `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_measurement` AS t1
    INNER JOIN
       `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_death` AS t2
        ON
            t1.person_id=t2.person_id
    INNER JOIN
        (select src_hpo_id,
            measurement_id
            from `{{curation_project}}.{{ehr_ops_dataset}}._mapping_measurement`) AS t3
    ON
        t1.measurement_id=t3.measurement_id
    GROUP BY
        1),
    procedure_death_date as (
    SELECT
        src_hpo_id,
        COUNT(distinct t1.procedure_occurrence_id) AS total,
        COUNT(distinct case when (DATE_DIFF(procedure_date, death_date, DAY)>30) then t1.procedure_occurrence_id end) as wrong_procedure_death_date
    FROM
       `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_procedure_occurrence` AS t1
    INNER JOIN
        `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_death` AS t2
        ON
            t1.person_id=t2.person_id
    INNER JOIN
        (select src_hpo_id,
        procedure_occurrence_id
        from `{{curation_project}}.{{ehr_ops_dataset}}._mapping_procedure_occurrence`) AS t3
    ON
        t1.procedure_occurrence_id=t3.procedure_occurrence_id
    GROUP BY
        1),
    observation_death_date as (
        SELECT
        src_hpo_id,
        COUNT(distinct t1.observation_id) AS total,
        COUNT(distinct case when (DATE_DIFF(observation_date, death_date, DAY)>30) then t1.observation_id end) as wrong_observation_death_date
    FROM
        `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_observation` AS t1
    INNER JOIN
         `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_death` AS t2
        ON
            t1.person_id=t2.person_id
    INNER JOIN
        (select src_hpo_id,
            observation_id
            from `{{curation_project}}.{{ehr_ops_dataset}}._mapping_observation`
        where src_table_id not like '%person%')  AS t3
    ON
        t1.observation_id=t3.observation_id
    GROUP BY
        1),
    device_death_date as (
    SELECT
        src_hpo_id,
        COUNT(distinct t1.device_exposure_id) AS total,
        COUNT(distinct case when (DATE_DIFF(device_exposure_start_date, death_date, DAY)>30) then t1.device_exposure_id end) as wrong_device_start_death_date,
        COUNT(distinct case when (DATE_DIFF(device_exposure_end_date, death_date, DAY)>30) then t1.device_exposure_id end) as wrong_device_end_death_date
    FROM
       `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_device_exposure` AS t1
    INNER JOIN
       `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_death` AS t2
        ON
            t1.person_id=t2.person_id
    INNER JOIN
        (select src_hpo_id,
            device_exposure_id
        from `{{curation_project}}.{{ehr_ops_dataset}}._mapping_device_exposure`) AS t3
    ON
        t1.device_exposure_id=t3.device_exposure_id
    GROUP BY
        1),
    hpo_id as (
    SELECT DISTINCT
       src_hpo_id
    FROM `{{curation_project}}.{{ehr_ops_dataset}}._mapping_person` AS p
    )

    select hpo_id.src_hpo_id,
    IFNULL(v.total, 0) as visit_total_rows,
    IFNULL(wrong_visit_start_death_date, 0) as wrong_visit_start_death_date,
    IFNULL(wrong_visit_end_death_date, 0) as wrong_visit_end_death_date,
    IFNULL(c.total, 0) as condition_total_rows,
    IFNULL(wrong_condition_start_death_date, 0) as wrong_condition_start_death_date,
    IFNULL(wrong_condition_end_death_date, 0) as wrong_condition_end_death_date,
    IFNULL(d.total, 0) as drug_total_rows,
    IFNULL(wrong_drug_start_death_date, 0) as wrong_drug_start_death_date,
    IFNULL(wrong_drug_end_death_date, 0) as wrong_drug_end_death_date,
    IFNULL(de.total, 0) as device_total_rows,
    IFNULL(wrong_device_start_death_date, 0) as wrong_device_start_death_date,
    IFNULL(wrong_device_end_death_date, 0) as wrong_device_end_death_date,
    IFNULL(m.total, 0) as measurement_total_rows,
    IFNULL(wrong_measurement_death_date, 0) as wrong_measurement_death_date,
    IFNULL(o.total, 0) as observation_total_rows,
    IFNULL(wrong_observation_death_date, 0) as wrong_observation_death_date,
    IFNULL(p.total, 0) as procedure_total_rows,
    IFNULL(wrong_procedure_death_date, 0) as wrong_procedure_death_date
    from hpo_id
    left join visit_death_date v on hpo_id.src_hpo_id = v.src_hpo_id
    left join condition_death_date c on hpo_id.src_hpo_id = c.src_hpo_id
    left join drug_death_date d on hpo_id.src_hpo_id = d.src_hpo_id
    left join device_death_date de on hpo_id.src_hpo_id = de.src_hpo_id
    left join measurement_death_date m on hpo_id.src_hpo_id = m.src_hpo_id
    left join observation_death_date o on hpo_id.src_hpo_id = o.src_hpo_id
    left join procedure_death_date p on hpo_id.src_hpo_id = p.src_hpo_id
