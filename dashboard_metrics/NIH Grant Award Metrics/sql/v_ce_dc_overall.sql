with
    visit_date as (
    SELECT
        t1.person_id,
        sum(case when (t1.visit_end_datetime is not null) then 1 else 0 end) AS total_rows,
        sum(case when (t1.visit_start_datetime>t1.visit_end_datetime) then 1 else 0 end) as wrong_date_rows,
    FROM
       `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_visit_occurrence` AS t1
    JOIN
        `{{curation_project}}.{{ehr_ops_dataset}}._mapping_visit_occurrence` AS mt1
    USING(visit_occurrence_id)
    WHERE mt1.src_hpo_id = 'care_evolution_omop_dv'
    Group by 1
    ),

    condition_date as (
        SELECT
        t1.person_id,
        sum(case when (t1.condition_end_datetime is not null) then 1 else 0 end) AS total_rows,
        sum(case when (t1.condition_start_datetime>t1.condition_end_datetime) then 1 else 0 end) as wrong_date_rows
    FROM
        `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_condition_occurrence` AS t1
    JOIN
        `{{curation_project}}.{{ehr_ops_dataset}}._mapping_condition_occurrence` AS mt1
    USING(condition_occurrence_id)
    WHERE mt1.src_hpo_id = 'care_evolution_omop_dv'
    GROUP BY
        1),

    drug_date as (
        SELECT
        t1.person_id,
        sum(case when (t1.drug_exposure_end_datetime is not null) then 1 else 0 end) AS total_rows,
        sum(case when (t1.drug_exposure_start_datetime>t1.drug_exposure_end_datetime) then 1 else 0 end) as wrong_date_rows
    FROM
        `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_drug_exposure` AS t1
    JOIN
        `{{curation_project}}.{{ehr_ops_dataset}}._mapping_drug_exposure` AS mt1
    USING(drug_exposure_id)
    WHERE mt1.src_hpo_id = 'care_evolution_omop_dv'
    GROUP BY
        1),

    device_date as (
        SELECT
        t1.person_id,
        sum(case when (t1.device_exposure_end_datetime is not null) then 1 else 0 end) AS total_rows,
        sum(case when (t1.device_exposure_start_datetime>t1.device_exposure_end_datetime) then 1 else 0 end) as wrong_date_rows
    FROM
        `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_device_exposure` AS t1
    JOIN
        `{{curation_project}}.{{ehr_ops_dataset}}._mapping_device_exposure` AS mt1
    USING(device_exposure_id)
    WHERE mt1.src_hpo_id = 'care_evolution_omop_dv'
    GROUP BY
        1
    ),

    visit_death_date as (
        SELECT
        t1.person_id,
        COUNT(distinct t1.visit_occurrence_id) AS total,
        COUNT(distinct case when (DATE_DIFF(visit_start_date, death_date, DAY)>30) then t1.visit_occurrence_id end) as wrong_visit_start_death_date,
        COUNT(distinct case when (DATE_DIFF(visit_end_date, death_date, DAY)>30) then t1.visit_occurrence_id end) as wrong_visit_end_death_date
    FROM
       `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_visit_occurrence` AS t1
    INNER JOIN
        `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_aou_death` AS t2
    ON
        t1.person_id=t2.person_id
    JOIN
        `{{curation_project}}.{{ehr_ops_dataset}}._mapping_visit_occurrence` AS mt1
    USING(visit_occurrence_id)
    WHERE mt1.src_hpo_id = 'care_evolution_omop_dv'
    GROUP BY
        1
    ),

    condition_death_date as (
        SELECT
        t1.person_id,
        COUNT(distinct t1.condition_occurrence_id) AS total,
        COUNT(distinct case when (DATE_DIFF(condition_start_date, death_date, DAY)>30) then t1.condition_occurrence_id end) as wrong_condition_start_death_date,
        COUNT(distinct case when (DATE_DIFF(condition_end_date, death_date, DAY)>30) then t1.condition_occurrence_id end) as wrong_condition_end_death_date,
    FROM
       `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_condition_occurrence` AS t1
    INNER JOIN
        `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_aou_death` AS t2
        ON
            t1.person_id=t2.person_id
    JOIN
        `{{curation_project}}.{{ehr_ops_dataset}}._mapping_condition_occurrence` AS mt1
    USING(condition_occurrence_id)
    WHERE mt1.src_hpo_id = 'care_evolution_omop_dv'
    GROUP BY
        1
    ),

    drug_death_date as (
        SELECT
        t1.person_id,
        COUNT(distinct t1.drug_exposure_id) AS total,
        COUNT(distinct case when (DATE_DIFF(drug_exposure_start_date, death_date, DAY)>30) then t1.drug_exposure_id end) as wrong_drug_start_death_date,
        COUNT(distinct case when (DATE_DIFF(drug_exposure_end_date, death_date, DAY)>30) then t1.drug_exposure_id end) as wrong_drug_end_death_date
    FROM
       `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_drug_exposure` AS t1
    INNER JOIN
        `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_aou_death` AS t2
        ON
            t1.person_id=t2.person_id
    JOIN
        `{{curation_project}}.{{ehr_ops_dataset}}._mapping_drug_exposure` AS mt1
    USING(drug_exposure_id)
    WHERE mt1.src_hpo_id = 'care_evolution_omop_dv'
    GROUP BY
        1),

    measurement_death_date as (
        SELECT
        t1.person_id,
        COUNT(distinct t1.measurement_id) AS total,
        COUNT(distinct case when (DATE_DIFF(measurement_date, death_date, DAY)>30) then t1.measurement_id end) as wrong_measurement_death_date
    FROM
       `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_measurement` AS t1
    INNER JOIN
       `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_aou_death` AS t2
        ON
            t1.person_id=t2.person_id
    JOIN
        `{{curation_project}}.{{ehr_ops_dataset}}._mapping_measurement` AS mt1
    USING(measurement_id)
    WHERE mt1.src_hpo_id = 'care_evolution_omop_dv'
    GROUP BY
        1),

    procedure_death_date as (
        SELECT
        t1.person_id,
        COUNT(distinct t1.procedure_occurrence_id) AS total,
        COUNT(distinct case when (DATE_DIFF(procedure_date, death_date, DAY)>30) then t1.procedure_occurrence_id end) as wrong_procedure_death_date
    FROM
       `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_procedure_occurrence` AS t1
    INNER JOIN
        `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_aou_death` AS t2
        ON
            t1.person_id=t2.person_id
    JOIN
        `{{curation_project}}.{{ehr_ops_dataset}}._mapping_procedure_occurrence` AS mt1
    USING(procedure_occurrence_id)
    WHERE mt1.src_hpo_id = 'care_evolution_omop_dv'
    GROUP BY
        1),

    observation_death_date as (
        SELECT
        t1.person_id,
        COUNT(distinct t1.observation_id) AS total,
        COUNT(distinct case when (DATE_DIFF(observation_date, death_date, DAY)>30) then t1.observation_id end) as wrong_observation_death_date
    FROM
        `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_observation` AS t1
    INNER JOIN
         `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_aou_death` AS t2
        ON
            t1.person_id=t2.person_id
    JOIN
        `{{curation_project}}.{{ehr_ops_dataset}}._mapping_observation` AS mt1
    USING(observation_id)
    WHERE mt1.src_hpo_id = 'care_evolution_omop_dv'
    GROUP BY
        1),

    device_death_date as (
        SELECT
        t1.person_id,
        COUNT(distinct t1.device_exposure_id) AS total,
        COUNT(distinct case when (DATE_DIFF(device_exposure_start_date, death_date, DAY)>30) then t1.device_exposure_id end) as wrong_device_start_death_date,
        COUNT(distinct case when (DATE_DIFF(device_exposure_end_date, death_date, DAY)>30) then t1.device_exposure_id end) as wrong_device_end_death_date
    FROM
       `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_device_exposure` AS t1
    INNER JOIN
       `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_aou_death` AS t2
        ON
            t1.person_id=t2.person_id
    JOIN
        `{{curation_project}}.{{ehr_ops_dataset}}._mapping_device_exposure` AS mt1
    USING(device_exposure_id)
    WHERE mt1.src_hpo_id = 'care_evolution_omop_dv'
    GROUP BY
        1),

    visit_date_dc3 as (
        SELECT
        t1.person_id,
        sum(case when (t1.visit_start_datetime is not null) then 1 else 0 end) AS total_start_rows,
        sum(case when (t1.visit_end_datetime is not null) then 1 else 0 end) AS total_end_rows,
        sum(case when (t1.visit_start_datetime<'1980-01-01') then 1 else 0 end) as wrong_start_date_rows,
        sum(case when (t1.visit_end_datetime<'1980-01-01') then 1 else 0 end) as wrong_end_date_rows
    FROM
       `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_visit_occurrence` AS t1
    JOIN
        `{{curation_project}}.{{ehr_ops_dataset}}._mapping_visit_occurrence` AS mt1
    USING(visit_occurrence_id)
    WHERE mt1.src_hpo_id = 'care_evolution_omop_dv'
    GROUP BY
        1),

    procedure_date_dc3 as (
        SELECT
        t1.person_id,
        sum(case when (t1.procedure_datetime is not null) then 1 else 0 end) AS total_rows,
        sum(case when (t1.procedure_datetime<'1980-01-01') then 1 else 0 end) as wrong_date_rows
    FROM
       `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_procedure_occurrence` AS t1
    JOIN
        `{{curation_project}}.{{ehr_ops_dataset}}._mapping_procedure_occurrence` AS mt1
    USING(procedure_occurrence_id)
    WHERE mt1.src_hpo_id = 'care_evolution_omop_dv'
    GROUP BY
        1),

    observation_date_dc3 as (
        SELECT
        t1.person_id,
        sum(case when (t1.observation_datetime is not null) then 1 else 0 end) AS total_rows,
        sum(case when (t1.observation_datetime<'1900-01-01') then 1 else 0 end) as wrong_date_rows
    FROM
       `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_observation` AS t1
    JOIN
        `{{curation_project}}.{{ehr_ops_dataset}}._mapping_observation` AS mt1
    USING(observation_id)
    WHERE mt1.src_hpo_id = 'care_evolution_omop_dv'
    GROUP BY
        1),

    measurement_date_dc3 as (
        SELECT
        t1.person_id,
        sum(case when (t1.measurement_datetime is not null) then 1 else 0 end) AS total_rows,
        sum(case when (t1.measurement_datetime<'1980-01-01') then 1 else 0 end) as wrong_date_rows
    FROM
       `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_measurement` AS t1
    JOIN
        `{{curation_project}}.{{ehr_ops_dataset}}._mapping_measurement` AS mt1
    USING(measurement_id)
    WHERE mt1.src_hpo_id = 'care_evolution_omop_dv'
    GROUP BY
        1),

    condition_date_dc3 as (
        SELECT
        t1.person_id,
        sum(case when (t1.condition_start_datetime is not null) then 1 else 0 end) AS total_start_rows,
        sum(case when (t1.condition_end_datetime is not null) then 1 else 0 end) AS total_end_rows,
        sum(case when (t1.condition_start_datetime<'1980-01-01') then 1 else 0 end) as wrong_start_date_rows,
        sum(case when (t1.condition_end_datetime<'1980-01-01') then 1 else 0 end) as wrong_end_date_rows
    FROM
       `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_condition_occurrence` AS t1
    JOIN
        `{{curation_project}}.{{ehr_ops_dataset}}._mapping_condition_occurrence` AS mt1
    USING(condition_occurrence_id)
    WHERE mt1.src_hpo_id = 'care_evolution_omop_dv'
    GROUP BY
        1),

    drug_date_dc3 as (
        SELECT
        t1.person_id,
        sum(case when (t1.drug_exposure_start_datetime is not null) then 1 else 0 end) AS total_start_rows,
        sum(case when (t1.drug_exposure_end_datetime is not null) then 1 else 0 end) AS total_end_rows,
        sum(case when (t1.drug_exposure_start_datetime<'1980-01-01') then 1 else 0 end) as wrong_start_date_rows,
        sum(case when (t1.drug_exposure_end_datetime<'1980-01-01') then 1 else 0 end) as wrong_end_date_rows
    FROM
       `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_drug_exposure` AS t1
    JOIN
        `{{curation_project}}.{{ehr_ops_dataset}}._mapping_drug_exposure` AS mt1
    USING(drug_exposure_id)
    WHERE mt1.src_hpo_id = 'care_evolution_omop_dv'
    GROUP BY
        1),

    device_date_dc3 as (
        SELECT
        t1.person_id,
        sum(case when (t1.device_exposure_start_datetime is not null) then 1 else 0 end) AS total_start_rows,
        sum(case when (t1.device_exposure_end_datetime is not null) then 1 else 0 end) AS total_end_rows,
        sum(case when (t1.device_exposure_start_datetime<'1980-01-01') then 1 else 0 end) as wrong_start_date_rows,
        sum(case when (t1.device_exposure_end_datetime<'1980-01-01') then 1 else 0 end) as wrong_end_date_rows
    FROM
       `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_device_exposure` AS t1
    JOIN
        `{{curation_project}}.{{ehr_ops_dataset}}._mapping_device_exposure` AS mt1
    USING(device_exposure_id)
    WHERE mt1.src_hpo_id = 'care_evolution_omop_dv'
    GROUP BY
        1),

    visit_date_dc4 as (
    SELECT
       t1.person_id,
        sum(case when (t1.visit_start_datetime is not null) then 1 else 0 end) as total_start_rows,
        sum(case when (t1.visit_end_datetime is not null) then 1 else 0 end) as total_end_rows,
        sum(case when (TIMESTAMP_DIFF(t1.visit_start_datetime,TIMESTAMP(visit_start_date), DAY))> 1 then 1 else 0 end) as start_not_match,
        sum(case when (TIMESTAMP_DIFF(t1.visit_end_datetime,TIMESTAMP(visit_end_date), DAY))> 1 then 1 else 0 end) as end_not_match
    FROM
       `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_visit_occurrence` AS t1
    JOIN
        `{{curation_project}}.{{ehr_ops_dataset}}._mapping_visit_occurrence` AS mt1
    USING(visit_occurrence_id)
    WHERE mt1.src_hpo_id = 'care_evolution_omop_dv'
    GROUP BY 1),

	procedure_date_dc4 as (
		SELECT
        t1.person_id,
        sum(case when (t1.procedure_datetime is not null) then 1 else 0 end) as total_rows,
        sum(case when (TIMESTAMP_DIFF(t1.procedure_datetime,TIMESTAMP(procedure_date), DAY))> 1 then 1 else 0 end) as not_match
    FROM
       `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_procedure_occurrence` AS t1
    JOIN
        `{{curation_project}}.{{ehr_ops_dataset}}._mapping_procedure_occurrence` AS mt1
    USING(procedure_occurrence_id)
    WHERE mt1.src_hpo_id = 'care_evolution_omop_dv'
    GROUP BY 1),

	observation_date_dc4 as (
		SELECT
        t1.person_id,
        sum(case when (t1.observation_datetime is not null) then 1 else 0 end) as total_rows,
        sum(case when (TIMESTAMP_DIFF(t1.observation_datetime,TIMESTAMP(observation_date), DAY)) > 1 then 1 else 0 end) as not_match
    FROM
       `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_observation` AS t1
    JOIN
        `{{curation_project}}.{{ehr_ops_dataset}}._mapping_observation` AS mt1
    USING(observation_id)
    WHERE mt1.src_hpo_id = 'care_evolution_omop_dv'
    GROUP BY 1),

	measurement_date_dc4 as (
		SELECT
        t1.person_id,
        sum(case when (t1.measurement_datetime is not null) then 1 else 0 end) as total_rows,
        sum(case when (TIMESTAMP_DIFF(t1.measurement_datetime,TIMESTAMP(measurement_date), DAY)) > 1 then 1 else 0 end) as not_match
    FROM
       `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_measurement` AS t1
    JOIN
        `{{curation_project}}.{{ehr_ops_dataset}}._mapping_measurement` AS mt1
    USING(measurement_id)
    WHERE mt1.src_hpo_id = 'care_evolution_omop_dv'
    GROUP BY 1),

	condition_date_dc4 as (
		SELECT
        t1.person_id,
        sum(case when (t1.condition_start_datetime is not null) then 1 else 0 end) as total_start_rows,
        sum(case when (t1.condition_end_datetime is not null) then 1 else 0 end) as total_end_rows,
        sum(case when (TIMESTAMP_DIFF(t1.condition_start_datetime,TIMESTAMP(condition_start_date), DAY)) > 1 then 1 else 0 end) as start_not_match,
        sum(case when (TIMESTAMP_DIFF(t1.condition_end_datetime,TIMESTAMP(condition_end_date), DAY)) > 1 then 1 else 0 end) as end_not_match
    FROM
       `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_condition_occurrence` AS t1
    JOIN
        `{{curation_project}}.{{ehr_ops_dataset}}._mapping_condition_occurrence` AS mt1
    USING(condition_occurrence_id)
    WHERE mt1.src_hpo_id = 'care_evolution_omop_dv'
    GROUP BY 1),

	drug_date_dc4 as (
		SELECT
        t1.person_id,
        sum(case when (t1.drug_exposure_start_datetime is not null) then 1 ELSE 0 end) as total_start_rows,
        sum(case when (t1.drug_exposure_end_datetime is not null) then 1 else 0 end) as total_end_rows,
        sum(case when (TIMESTAMP_DIFF(t1.drug_exposure_start_datetime,TIMESTAMP(drug_exposure_start_date), DAY)) > 1 then 1 else 0 end) as start_not_match,
        sum(case when (TIMESTAMP_DIFF(t1.drug_exposure_end_datetime,TIMESTAMP(drug_exposure_end_date), DAY)) > 1 then 1 else 0 end) as end_not_match
    FROM
       `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_drug_exposure` AS t1
    JOIN
        `{{curation_project}}.{{ehr_ops_dataset}}._mapping_drug_exposure` AS mt1
    USING(drug_exposure_id)
    WHERE mt1.src_hpo_id = 'care_evolution_omop_dv'
    GROUP BY 1),

	device_date_dc4 as (
		SELECT
        t1.person_id,
        sum(case when (t1.device_exposure_start_datetime is not null) then 1 else 0 end) as total_start_rows,
        sum(case when (t1.device_exposure_end_datetime is not null) then 1 else 0 end) as total_end_rows,
        sum(case when (TIMESTAMP_DIFF(t1.device_exposure_start_datetime,TIMESTAMP(device_exposure_start_date), DAY)) > 1 then 1 else 0 end) as start_not_match,
        sum(case when (TIMESTAMP_DIFF(t1.device_exposure_end_datetime,TIMESTAMP(device_exposure_end_date), DAY)) > 1 then 1 else 0 end) as end_not_match
    FROM
       `{{curation_project}}.{{ehr_ops_dataset}}.unioned_ehr_device_exposure` AS t1
    JOIN
        `{{curation_project}}.{{ehr_ops_dataset}}._mapping_device_exposure` AS mt1
    USING(device_exposure_id)
    WHERE mt1.src_hpo_id = 'care_evolution_omop_dv'
    GROUP BY 1)


SELECT DISTINCT
    participant_id,
    ORGANIZATION,
    hpo,
    hpo_display_name,
    org_display_name,
    external_id,
    IFNULL(v.total_rows, 0) as visit_total_rows,
    IFNULL(v.wrong_date_rows, 0) as visit_wrong_date_rows,
    IFNULL(c.total_rows, 0) as condition_total_rows,
    IFNULL(c.wrong_date_rows, 0) as condition_wrong_date_rows,
    IFNULL(d.total_rows, 0) as drug_total_rows,
    IFNULL(d.wrong_date_rows, 0) as drug_wrong_date_rows,
    IFNULL(de.total_rows, 0) as device_total_rows,
    IFNULL(de.wrong_date_rows, 0) as device_wrong_date_rows,
    IFNULL(vv.total, 0) as visit_total_rows_2,
    IFNULL(wrong_visit_start_death_date, 0) as wrong_visit_start_death_date_2,
    IFNULL(wrong_visit_end_death_date, 0) as wrong_visit_end_death_date_2,
    IFNULL(cc.total, 0) as condition_total_rows_2,
    IFNULL(wrong_condition_start_death_date, 0) as wrong_condition_start_death_date_2,
    IFNULL(wrong_condition_end_death_date, 0) as wrong_condition_end_death_date_2,
    IFNULL(dd.total, 0) as drug_total_rows_2,
    IFNULL(wrong_drug_start_death_date, 0) as wrong_drug_start_death_date_2,
    IFNULL(wrong_drug_end_death_date, 0) as wrong_drug_end_death_date_2,
    IFNULL(dede.total, 0) as device_total_rows_2,
    IFNULL(wrong_device_start_death_date, 0) as wrong_device_start_death_date_2,
    IFNULL(wrong_device_end_death_date, 0) as wrong_device_end_death_date_2,
    IFNULL(mm.total, 0) as measurement_total_rows_2,
    IFNULL(wrong_measurement_death_date, 0) as wrong_measurement_death_date_2,
    IFNULL(oo.total, 0) as observation_total_rows_2,
    IFNULL(wrong_observation_death_date, 0) as wrong_observation_death_date_2,
    IFNULL(pp.total, 0) as procedure_total_rows_2,
    IFNULL(wrong_procedure_death_date, 0) as wrong_procedure_death_date_2,

    IFNULL(vd3.total_start_rows, 0) as visit_start_total_rows_3,
    IFNULL(vd3.wrong_start_date_rows, 0) as visit_start_wrong_date_rows_3,
    IFNULL(vd3.total_end_rows, 0) as visit_end_total_rows_3,
    IFNULL(vd3.wrong_end_date_rows, 0) as visit_end_wrong_date_rows_3,
    IFNULL(cd3.total_start_rows, 0) as condition_start_total_rows_3,
    IFNULL(cd3.wrong_start_date_rows, 0) as condition_start_wrong_date_rows_3,
    IFNULL(cd3.total_end_rows, 0) as condition_end_total_rows_3,
    IFNULL(cd3.wrong_end_date_rows, 0) as condition_end_wrong_date_rows_3,
    IFNULL(dd3.total_start_rows, 0) as drug_start_total_rows_3,
    IFNULL(dd3.wrong_start_date_rows, 0) as drug_start_wrong_date_rows_3,
    IFNULL(dd3.total_end_rows, 0) as drug_end_total_rows_3,
    IFNULL(dd3.wrong_end_date_rows, 0) as drug_end_wrong_date_rows_3,
    IFNULL(mdc3.total_rows, 0) as measurement_total_rows_3,
    IFNULL(mdc3.wrong_date_rows, 0) as measurement_wrong_date_rows_3,
    IFNULL(odc3.total_rows, 0) as observation_total_rows_3,
    IFNULL(odc3.wrong_date_rows, 0) as observation_wrong_date_rows_3,
    IFNULL(pdc3.total_rows, 0) as procedure_total_rows_3,
    IFNULL(pdc3.wrong_date_rows, 0) as procedure_wrong_date_rows_3,
    IFNULL(dedc3.total_start_rows, 0) as device_start_total_rows_3,
    IFNULL(dedc3.wrong_start_date_rows, 0) as device_start_wrong_date_rows_3,
    IFNULL(dedc3.total_end_rows, 0) as device_end_total_rows_3,
    IFNULL(dedc3.wrong_end_date_rows, 0) as device_end_wrong_date_rows_3,

    IFNULL(vd4.total_start_rows, 0) as visit_start_total_rows_4,
    IFNULL(vd4.start_not_match, 0) as visit_start_not_match_4,
    IFNULL(vd4.total_end_rows, 0) as visit_end_total_rows_4,
    IFNULL(vd4.end_not_match, 0) as visit_end_not_match_4,
    IFNULL(cd4.total_start_rows, 0) as condition_start_total_rows_4,
    IFNULL(cd4.start_not_match, 0) as condition_start_not_match_4,
    IFNULL(cd4.total_end_rows, 0) as condition_end_total_rows_4,
    IFNULL(cd4.end_not_match, 0) as condition_end_not_match_4,
    IFNULL(dd4.total_start_rows, 0) as drug_start_total_rows_4,
    IFNULL(dd4.start_not_match, 0) as drug_start_not_match_4,
    IFNULL(dd4.total_end_rows, 0) as drug_end_total_rows_4,
    IFNULL(dd4.end_not_match, 0) as drug_end_not_match_4,
    IFNULL(m4.total_rows, 0) as measurement_total_rows_4,
    IFNULL(m4.not_match, 0) as measurement_not_match_4,
    IFNULL(o4.total_rows, 0) as observation_total_rows_4,
    IFNULL(o4.not_match, 0) as observation_not_match_4,
    IFNULL(p4.total_rows, 0) as procedure_total_rows_4,
    IFNULL(p4.not_match, 0) as procedure_not_match_4,
    IFNULL(ded4.total_start_rows, 0) as device_start_total_rows,
    IFNULL(ded4.start_not_match, 0) as device_start_not_match,
    IFNULL(ded4.total_end_rows, 0) as device_end_total_rows,
    IFNULL(ded4.end_not_match, 0) as device_end_not_match

    from `aou-ehr-ops-curation-prod.ehr_ops_resources.v_ehr_rdr_participant` fin
    LEFT JOIN visit_date v ON fin.participant_id = v.person_id
    LEFT JOIN condition_date c ON fin.participant_id = c.person_id
    LEFT JOIN drug_date d on fin.participant_id = d.person_id
    LEFT JOIN device_date de on fin.participant_id = de.person_id
    LEFT JOIN visit_death_date vv on fin.participant_id = vv.person_id
    LEFT JOIN condition_death_date cc on fin.participant_id = cc.person_id
    LEFT JOIN drug_death_date dd on fin.participant_id = dd.person_id
    LEFT JOIN device_death_date dede on fin.participant_id = dede.person_id
    LEFT JOIN measurement_death_date mm on fin.participant_id = mm.person_id
    LEFT JOIN observation_death_date oo on fin.participant_id = oo.person_id
    LEFT JOIN procedure_death_date pp on fin.participant_id = pp.person_id
    LEFT JOIN visit_date_dc3 vd3 on fin.participant_id = vd3.person_id
    LEFT JOIN condition_date_dc3 cd3 on fin.participant_id = cd3.person_id
    LEFT JOIN drug_date_dc3 dd3 on fin.participant_id = dd3.person_id
    LEFT JOIN measurement_date_dc3 mdc3 on fin.participant_id = mdc3.person_id
    LEFT JOIN observation_date_dc3 odc3 on fin.participant_id = odc3.person_id
    LEFT JOIN procedure_date_dc3 pdc3 on fin.participant_id = pdc3.person_id
    LEFT JOIN device_date_dc3 dedc3 on fin.participant_id = dedc3.person_id
    LEFT JOIN visit_date_dc4 vd4 on fin.participant_id = vd4.person_id
    LEFT JOIN condition_date_dc4 cd4 on fin.participant_id = cd4.person_id
    LEFT JOIN drug_date_dc4 dd4 on fin.participant_id = dd4.person_id
    LEFT JOIN measurement_date_dc4 m4 on fin.participant_id = m4.person_id
    LEFT JOIN observation_date_dc4 o4 on fin.participant_id = o4.person_id
    LEFT JOIN procedure_date_dc4 p4 on fin.participant_id = p4.person_id
    LEFT JOIN device_date_dc4 ded4 on fin.participant_id = ded4.person_id
    WHERE ORGANIZATION = 'CARE_EVOLUTION_OMOP_DV'