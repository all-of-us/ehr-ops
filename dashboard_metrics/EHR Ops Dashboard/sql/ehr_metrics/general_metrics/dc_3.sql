--- This query is to check DC-3 metric
--- DC-3 metric is checking if any date prior to 1900 (for observation table) or 1980 (for other clinical data tables)
with
    visit_date as (
    SELECT
        src_hpo_id,
        sum(case when (t1.visit_start_datetime is not null) then 1 else 0 end) AS total_start_rows,
        sum(case when (t1.visit_end_datetime is not null) then 1 else 0 end) AS total_end_rows,
        sum(case when (t1.visit_start_datetime<'1980-01-01') then 1 else 0 end) as wrong_start_date_rows,
        sum(case when (t1.visit_end_datetime<'1980-01-01') then 1 else 0 end) as wrong_end_date_rows
    FROM
       `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_visit_occurrence` AS t1
    INNER JOIN
    (select src_hpo_id,
        visit_occurrence_id
    from `{{curation_project}}.{{ehr_ops_dataset}}._mapping_visit_occurrence`) AS t2
    ON
        t1.visit_occurrence_id=t2.visit_occurrence_id
    GROUP BY
        1),

    procedure_date as (
        SELECT
        src_hpo_id,
        sum(case when (t1.procedure_datetime is not null) then 1 else 0 end) AS total_rows,
        sum(case when (t1.procedure_datetime<'1980-01-01') then 1 else 0 end) as wrong_date_rows
    FROM
       `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_procedure_occurrence` AS t1
    INNER JOIN
    (select src_hpo_id,
        procedure_occurrence_id
    from `{{curation_project}}.{{ehr_ops_dataset}}._mapping_procedure_occurrence`) AS t2
    ON
        t1.procedure_occurrence_id=t2.procedure_occurrence_id
    GROUP BY
        1),

    observation_date as (
        SELECT
        src_hpo_id,
        sum(case when (t1.observation_datetime is not null) then 1 else 0 end) AS total_rows,
        sum(case when (t1.observation_datetime<'1900-01-01') then 1 else 0 end) as wrong_date_rows
    FROM
       `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_observation` AS t1
    INNER JOIN
    (select src_hpo_id,
        observation_id
    from `{{curation_project}}.{{ehr_ops_dataset}}._mapping_observation`
    where src_table_id not like '%person%') AS t2
    ON
        t1.observation_id=t2.observation_id
    GROUP BY
        1),

    measurement_date as (
        SELECT
        src_hpo_id,
        sum(case when (t1.measurement_datetime is not null) then 1 else 0 end) AS total_rows,
        sum(case when (t1.measurement_datetime<'1980-01-01') then 1 else 0 end) as wrong_date_rows
    FROM
       `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_measurement` AS t1
    INNER JOIN
    (select src_hpo_id,
        measurement_id
    from `{{curation_project}}.{{ehr_ops_dataset}}._mapping_measurement`) AS t2
    ON
        t1.measurement_id=t2.measurement_id
    GROUP BY
        1),

    condition_date as (
        SELECT
        src_hpo_id,
        sum(case when (t1.condition_start_datetime is not null) then 1 else 0 end) AS total_start_rows,
        sum(case when (t1.condition_end_datetime is not null) then 1 else 0 end) AS total_end_rows,
        sum(case when (t1.condition_start_datetime<'1980-01-01') then 1 else 0 end) as wrong_start_date_rows,
        sum(case when (t1.condition_end_datetime<'1980-01-01') then 1 else 0 end) as wrong_end_date_rows
    FROM
       `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_condition_occurrence` AS t1
    INNER JOIN
    (select src_hpo_id,
        condition_occurrence_id
    from `{{curation_project}}.{{ehr_ops_dataset}}._mapping_condition_occurrence`) AS t2
    ON
        t1.condition_occurrence_id=t2.condition_occurrence_id
    GROUP BY
        1),

    drug_date as (
        SELECT
        src_hpo_id,
        sum(case when (t1.drug_exposure_start_datetime is not null) then 1 else 0 end) AS total_start_rows,
        sum(case when (t1.drug_exposure_end_datetime is not null) then 1 else 0 end) AS total_end_rows,
        sum(case when (t1.drug_exposure_start_datetime<'1980-01-01') then 1 else 0 end) as wrong_start_date_rows,
        sum(case when (t1.drug_exposure_end_datetime<'1980-01-01') then 1 else 0 end) as wrong_end_date_rows
    FROM
       `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_drug_exposure` AS t1
    INNER JOIN
    (select src_hpo_id,
        drug_exposure_id
    from `{{curation_project}}.{{ehr_ops_dataset}}._mapping_drug_exposure`) AS t2
    ON
        t1.drug_exposure_id=t2.drug_exposure_id
    GROUP BY
        1),

    device_date as (
        SELECT
        src_hpo_id,
        sum(case when (t1.device_exposure_start_datetime is not null) then 1 else 0 end) AS total_start_rows,
        sum(case when (t1.device_exposure_end_datetime is not null) then 1 else 0 end) AS total_end_rows,
        sum(case when (t1.device_exposure_start_datetime<'1980-01-01') then 1 else 0 end) as wrong_start_date_rows,
        sum(case when (t1.device_exposure_end_datetime<'1980-01-01') then 1 else 0 end) as wrong_end_date_rows
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

    select vd.src_hpo_id,
    IFNULL(vd.total_start_rows, 0) as visit_start_total_rows,
    IFNULL(vd.wrong_start_date_rows, 0) as visit_start_wrong_date_rows,
    IFNULL(vd.total_end_rows, 0) as visit_end_total_rows,
    IFNULL(vd.wrong_end_date_rows, 0) as visit_end_wrong_date_rows,
    IFNULL(cd.total_start_rows, 0) as condition_start_total_rows,
    IFNULL(cd.wrong_start_date_rows, 0) as condition_start_wrong_date_rows,
    IFNULL(cd.total_end_rows, 0) as condition_end_total_rows,
    IFNULL(cd.wrong_end_date_rows, 0) as condition_end_wrong_date_rows,
    IFNULL(dd.total_start_rows, 0) as drug_start_total_rows,
    IFNULL(dd.wrong_start_date_rows, 0) as drug_start_wrong_date_rows,
    IFNULL(dd.total_end_rows, 0) as drug_end_total_rows,
    IFNULL(dd.wrong_end_date_rows, 0) as drug_end_wrong_date_rows,
    IFNULL(m.total_rows, 0) as measurement_total_rows,
    IFNULL(m.wrong_date_rows, 0) as measurement_wrong_date_rows,
    IFNULL(o.total_rows, 0) as observation_total_rows,
    IFNULL(o.wrong_date_rows, 0) as observation_wrong_date_rows,
    IFNULL(p.total_rows, 0) as procedure_total_rows,
    IFNULL(p.wrong_date_rows, 0) as procedure_wrong_date_rows,
    IFNULL(ded.total_start_rows, 0) as device_start_total_rows,
    IFNULL(ded.wrong_start_date_rows, 0) as device_start_wrong_date_rows,
    IFNULL(ded.total_end_rows, 0) as device_end_total_rows,
    IFNULL(ded.wrong_end_date_rows, 0) as device_end_wrong_date_rows
    from visit_date vd
    left join condition_date cd
    on vd.src_hpo_id = cd.src_hpo_id
    left join drug_date dd
    on vd.src_hpo_id = dd.src_hpo_id
    left join measurement_date m
    on vd.src_hpo_id = m.src_hpo_id
    left join observation_date o
    on vd.src_hpo_id = o.src_hpo_id
    left join procedure_date p
    on vd.src_hpo_id = p.src_hpo_id
    left join device_date ded
    on vd.src_hpo_id = ded.src_hpo_id
