--- This query is to check DC-4 metric
--- DC-4 metric is checking if any date and datetime not match with each other
with
	visit_date as (
    SELECT
        src_hpo_id,
        sum(case when (t1.visit_start_datetime is not null) then 1 else 0 end) as total_start_rows,
        sum(case when (t1.visit_end_datetime is not null) then 1 else 0 end) as total_end_rows,
        sum(case when (ABS(EXTRACT( EPOCH FROM t1.visit_start_datetime - visit_start_date)/3600))> 1 then 1 else 0 end) as start_not_match,
        sum(case when (ABS(EXTRACT( EPOCH FROM t1.visit_end_datetime - visit_end_date)/3600)) > 1 then 1 else 0 end) as end_not_match
    FROM
       {{curation_ops_schema}}.unioned_ehr_visit_occurrence AS t1
    INNER JOIN
    (select src_hpo_id,
        visit_occurrence_id
    from {{curation_ops_schema}}._mapping_visit_occurrence) AS t2
    ON
        t1.visit_occurrence_id=t2.visit_occurrence_id
    GROUP BY 1),

	procedure_date as (
		SELECT
        src_hpo_id,
        sum(case when (t1.procedure_datetime is not null) then 1 else 0 end) as total_rows,
        sum(case when (ABS(EXTRACT(EPOCH FROM t1.procedure_datetime - procedure_date)/3600))> 1 then 1 else 0 end) as not_match
    FROM
       {{curation_ops_schema}}.unioned_ehr_procedure_occurrence AS t1
    INNER JOIN
    (select src_hpo_id,
        procedure_occurrence_id
    from {{curation_ops_schema}}._mapping_procedure_occurrence)  AS t2
    ON
        t1.procedure_occurrence_id=t2.procedure_occurrence_id
    GROUP BY 1),

	observation_date as (
		SELECT
        src_hpo_id,
        sum(case when (t1.observation_datetime is not null) then 1 else 0 end) as total_rows,
        sum(case when (ABS(EXTRACT(EPOCH FROM t1.observation_datetime - observation_date)/3600)) > 1 then 1 else 0 end) as not_match
    FROM
       {{curation_ops_schema}}.unioned_ehr_observation AS t1
    INNER JOIN
    (select src_hpo_id,
        observation_id
    from {{curation_ops_schema}}._mapping_observation
    where src_table_id not like '%person%')  AS t2
    ON
        t1.observation_id=t2.observation_id
    GROUP BY 1),

	measurement_date as (
		SELECT
        src_hpo_id,
        sum(case when (t1.measurement_datetime is not null) then 1 else 0 end) as total_rows,
        sum(case when (ABS(EXTRACT(EPOCH FROM t1.measurement_datetime - measurement_date)/3600)) > 1 then 1 else 0 end) as not_match
    FROM
       {{curation_ops_schema}}.unioned_ehr_measurement AS t1
    INNER JOIN
    (select src_hpo_id,
        measurement_id
    from {{curation_ops_schema}}._mapping_measurement) AS t2
    ON
        t1.measurement_id=t2.measurement_id
    GROUP BY 1),

	condition_date as (
		SELECT
        src_hpo_id,
        sum(case when (t1.condition_start_datetime is not null) then 1 else 0 end) as total_start_rows,
        sum(case when (t1.condition_end_datetime is not null) then 1 else 0 end) as total_end_rows,
        sum(case when (ABS(EXTRACT(EPOCH FROM t1.condition_start_datetime - condition_start_date)/3600)) > 1 then 1 else 0 end) as start_not_match,
        sum(case when (ABS(EXTRACT(EPOCH FROM t1.condition_end_datetime - condition_end_date)/3600)) > 1 then 1 else 0 end) as end_not_match
    FROM
       {{curation_ops_schema}}.unioned_ehr_condition_occurrence AS t1
    INNER JOIN
    (select src_hpo_id,
        condition_occurrence_id
    from {{curation_ops_schema}}._mapping_condition_occurrence) AS t2
    ON
        t1.condition_occurrence_id=t2.condition_occurrence_id
    GROUP BY 1),

	drug_date as (
		SELECT
        src_hpo_id,
        sum(case when (t1.drug_exposure_start_datetime is not null) then 1 ELSE 0 end) as total_start_rows,
        sum(case when (t1.drug_exposure_end_datetime is not null) then 1 else 0 end) as total_end_rows,
        sum(case when (ABS(EXTRACT(EPOCH FROM t1.drug_exposure_start_datetime - drug_exposure_start_date)/3600)) > 1 then 1 else 0 end) as start_not_match,
        sum(case when (ABS(EXTRACT(EPOCH FROM t1.drug_exposure_end_datetime - drug_exposure_end_date)/3600)) > 1 then 1 else 0 end) as end_not_match
    FROM
       {{curation_ops_schema}}.unioned_ehr_drug_exposure AS t1
    INNER JOIN
    (select src_hpo_id,
        drug_exposure_id
    from {{curation_ops_schema}}._mapping_drug_exposure)  AS t2
    ON
        t1.drug_exposure_id=t2.drug_exposure_id
    GROUP BY 1),

	device_date as (
		SELECT
        src_hpo_id,
        sum(case when (t1.device_exposure_start_datetime is not null) then 1 else 0 end) as total_start_rows,
        sum(case when (t1.device_exposure_end_datetime is not null) then 1 else 0 end) as total_end_rows,
        sum(case when (ABS(EXTRACT(EPOCH FROM t1.device_exposure_start_datetime - device_exposure_start_date)/3600)) > 1 then 1 else 0 end) as start_not_match,
        sum(case when (ABS(EXTRACT(EPOCH FROM t1.device_exposure_end_datetime - device_exposure_end_date)/3600)) > 1 then 1 else 0 end) as end_not_match
    FROM
       {{curation_ops_schema}}.unioned_ehr_device_exposure AS t1
    INNER JOIN
    (select src_hpo_id,
        device_exposure_id
    from {{curation_ops_schema}}._mapping_device_exposure) AS t2
    ON
        t1.device_exposure_id=t2.device_exposure_id
    GROUP BY 1)

    select vd.src_hpo_id,
    COALESCE(vd.total_start_rows, 0) as visit_start_total_rows,
    COALESCE(vd.start_not_match, 0) as visit_start_not_match,
    COALESCE(vd.total_end_rows, 0) as visit_end_total_rows,
    COALESCE(vd.end_not_match, 0) as visit_end_not_match,
    COALESCE(cd.total_start_rows, 0) as condition_start_total_rows,
    COALESCE(cd.start_not_match, 0) as condition_start_not_match,
    COALESCE(cd.total_end_rows, 0) as condition_end_total_rows,
    COALESCE(cd.end_not_match, 0) as condition_end_not_match,
    COALESCE(dd.total_start_rows, 0) as drug_start_total_rows,
    COALESCE(dd.start_not_match, 0) as drug_start_not_match,
    COALESCE(dd.total_end_rows, 0) as drug_end_total_rows,
    COALESCE(dd.end_not_match, 0) as drug_end_not_match,
    COALESCE(m.total_rows, 0) as measurement_total_rows,
    COALESCE(m.not_match, 0) as measurement_not_match,
    COALESCE(o.total_rows, 0) as observation_total_rows,
    COALESCE(o.not_match, 0) as observation_not_match,
    COALESCE(p.total_rows, 0) as procedure_total_rows,
    COALESCE(p.not_match, 0) as procedure_not_match,
    COALESCE(ded.total_start_rows, 0) as device_start_total_rows,
    COALESCE(ded.start_not_match, 0) as device_start_not_match,
    COALESCE(ded.total_end_rows, 0) as device_end_total_rows,
    COALESCE(ded.end_not_match, 0) as device_end_not_match
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
