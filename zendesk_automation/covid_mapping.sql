with
    ehr_orders_concepts as (
        select distinct c.concept_id 
        from `aou-res-curation-prod.ehr_ops.concept` as c
        join `aou-res-curation-prod.ehr_ops.concept_ancestor` as ca
        on c.concept_id = ca.descendant_concept_id
        where ca.ancestor_concept_id = 756055
        and c.vocabulary_id in ('CPT4', 'HCPCS')
        and c.standard_concept = 'S'
    ),
    covid as (
        select distinct mm.src_hpo_id, count(*) as total_covid_measurements
        from `aou-res-curation-prod.ehr_ops.unioned_ehr_measurement` as m
        join `aou-res-curation-prod.ehr_ops.concept_ancestor` as ca on m.measurement_concept_id   = ca.descendant_concept_id
        join `aou-res-curation-prod.ehr_ops._mapping_measurement` as mm on m.measurement_id = mm.measurement_id
        where ca.ancestor_concept_id  = 756055
        and m.measurement_type_concept_id != 32833 --exclude EHR orders
        and m.measurement_concept_id not in (select concept_id from ehr_orders_concepts) --exclude EHR orders concepts
        and m.measurement_concept_id != 3015746 --exclude due to too general definition
        group by mm.src_hpo_id
    ),
    covid_result_mapping_issue as (
        select distinct mm.src_hpo_id, count(*) as covid_mapping_issue_count
        from `aou-res-curation-prod.ehr_ops.unioned_ehr_measurement` as m
        join `aou-res-curation-prod.ehr_ops.concept_ancestor` as ca on m.measurement_concept_id   = ca.descendant_concept_id
        join `aou-res-curation-prod.ehr_ops._mapping_measurement` as mm on m.measurement_id = mm.measurement_id
        where ca.ancestor_concept_id  = 756055
            and (m.value_as_concept_id is null or m.value_as_concept_id =0)
            and m.measurement_type_concept_id != 32833 --exclude EHR orders
            and m.measurement_concept_id not in (select concept_id from ehr_orders_concepts) --exclude EHR orders concepts
            and m.measurement_concept_id != 3015746 --exclude due to too general definition
        group by mm.src_hpo_id
    )

select covid.src_hpo_id, covid_mapping_issue_count, total_covid_measurements
from covid
    left join covid_result_mapping_issue on covid.src_hpo_id = covid_result_mapping_issue.src_hpo_id
where LOWER(covid.src_hpo_id) in UNNEST(@included_hpos)
and covid_result_mapping_issue.covid_mapping_issue_count is not null