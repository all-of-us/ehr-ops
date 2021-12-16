-- This query checks on the mapping of COVID-19 measurement results

with
    covid as (
        select distinct mm.src_hpo_id, count(*) as total_covid_measurements
        from {{curation_ops_schema}}.unioned_ehr_measurement as m
        join {{vocab_schema}}.concept_ancestor as ca on m.measurement_concept_id   = ca.descendant_concept_id
        join {{curation_ops_schema}}._mapping_measurement as mm on m.measurement_id = mm.measurement_id
        where ca.ancestor_concept_id  = 756055
        group by mm.src_hpo_id
    ),
    covid_result_mapping_issue as (
        select distinct mm.src_hpo_id, count(*) as covid_mapping_issue_count
        from {{curation_ops_schema}}.unioned_ehr_measurement as m
        join {{vocab_schema}}.concept_ancestor as ca on m.measurement_concept_id   = ca.descendant_concept_id
        join {{curation_ops_schema}}._mapping_measurement as mm on m.measurement_id = mm.measurement_id
        where ca.ancestor_concept_id  = 756055
            and (m.value_as_concept_id is null or m.value_as_concept_id =0)
            and m.measurement_concept_id != 700360
        group by mm.src_hpo_id
    )

select covid.src_hpo_id, covid_mapping_issue_count, total_covid_measurements
from covid
    left join covid_result_mapping_issue on covid.src_hpo_id = covid_result_mapping_issue.src_hpo_id