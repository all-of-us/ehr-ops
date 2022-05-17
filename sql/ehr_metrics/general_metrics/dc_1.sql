--- This query is to check DC-1 metric
--- DC-1 metric is checking if any end date is before start date in all OMOP tables
with
    visit_date as (
    SELECT
        src_hpo_id,
        sum(case when (t1.visit_end_datetime is not null) then 1 else 0 end) AS total_rows,
        sum(case when (t1.visit_start_datetime>t1.visit_end_datetime) then 1 else 0 end) as wrong_date_rows
    FROM
       `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_visit_occurrence` AS t1
    INNER JOIN
        (select src_hpo_id,
            visit_occurrence_id
        from  `{{curation_project}}.{{ehr_ops_dataset}}._mapping_visit_occurrence`) AS t2
    ON
        t1.visit_occurrence_id=t2.visit_occurrence_id

    GROUP BY
        1),

    condition_date as (
    SELECT
        src_hpo_id,
        sum(case when (t1.condition_end_datetime is not null) then 1 else 0 end) AS total_rows,
        sum(case when (t1.condition_start_datetime>t1.condition_end_datetime) then 1 else 0 end) as wrong_date_rows
    FROM
        `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_condition_occurrence` AS t1
    INNER JOIN
        (select src_hpo_id,
            condition_occurrence_id
        from  `{{curation_project}}.{{ehr_ops_dataset}}._mapping_condition_occurrence`) AS t2
    ON
        t1.condition_occurrence_id=t2.condition_occurrence_id

    GROUP BY
        1),

    drug_date as (
        SELECT
        src_hpo_id,
        sum(case when (t1.drug_exposure_end_datetime is not null) then 1 else 0 end) AS total_rows,
        sum(case when (t1.drug_exposure_start_datetime>t1.drug_exposure_end_datetime) then 1 else 0 end) as wrong_date_rows
    FROM
        `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_drug_exposure` AS t1
    INNER JOIN
        (select src_hpo_id,
            drug_exposure_id
        from  `{{curation_project}}.{{ehr_ops_dataset}}._mapping_drug_exposure`) AS t2
    ON
        t1.drug_exposure_id=t2.drug_exposure_id

    GROUP BY
        1),

    device_date as (
        SELECT
        src_hpo_id,
        sum(case when (t1.device_exposure_end_datetime is not null) then 1 else 0 end) AS total_rows,
        sum(case when (t1.device_exposure_start_datetime>t1.device_exposure_end_datetime) then 1 else 0 end) as wrong_date_rows
    FROM
        `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_device_exposure` AS t1
    INNER JOIN
        (select src_hpo_id,
            device_exposure_id
        from `{{curation_project}}.{{ehr_ops_dataset}}._mapping_device_exposure`) AS t2
    ON
        t1.device_exposure_id=t2.device_exposure_id

    GROUP BY
        1)

    select v.src_hpo_id,
    IFNULL(v.total_rows, 0) as visit_total_rows,
    IFNULL(v.wrong_date_rows, 0) as visit_wrong_date_rows,
    IFNULL(c.total_rows, 0) as condition_total_rows,
    IFNULL(c.wrong_date_rows, 0) as condition_wrong_date_rows,
    IFNULL(d.total_rows, 0) as drug_total_rows,
    IFNULL(d.wrong_date_rows, 0) as drug_wrong_date_rows,
    IFNULL(de.total_rows, 0) as device_total_rows,
    IFNULL(de.wrong_date_rows, 0) as device_wrong_date_rows

    from visit_date v
    left join condition_date c
    on v.src_hpo_id = c.src_hpo_id
    left join drug_date d
    on v.src_hpo_id = d.src_hpo_id
    left join device_date de
    on v.src_hpo_id = de.src_hpo_id