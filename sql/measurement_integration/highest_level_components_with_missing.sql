with highest_level_components as (
    SELECT DISTINCT csd1.ancestor_concept_id concept_id, csd1.ancestor_concept_name concept_name
    FROM `aou-pdr-data-prod.curation_data_view.measurement_concept_sets_descendants` csd1
    LEFT JOIN `aou-pdr-data-prod.curation_data_view.measurement_concept_sets_descendants` csd2
    ON csd1.ancestor_concept_name = csd2.descendant_concept_name
    WHERE csd2.ancestor_concept_id IS NULL
),
hpo_list as (
    SELECT
        LOWER(hpo.HPO_ID) hpo_id
    FROM `aou-pdr-data-prod.curation_data_view.v_org_hpo_mapping` hpo
),
wide_net as (
    SELECT
        hlc.concept_id ancestor_concept_id, hlc.concept_name ancestor_concept_name,
        csd.descendant_concept_id
    FROM highest_level_components hlc
    JOIN `aou-pdr-data-prod.curation_data_view.measurement_concept_sets_descendants` csd
        ON csd.ancestor_concept_id = hlc.concept_id
    JOIN `aou-pdr-data-prod.curation_data_view.concept` c
        ON c.concept_id = csd.descendant_concept_id
    WHERE c.vocabulary_id = 'LOINC'
        AND c.concept_class_id = 'Lab Test'
),
wide_measurement_counts as (
    select
        mm.src_hpo_id, wn.ancestor_concept_id, wn.ancestor_concept_name,
        count(distinct m.measurement_id) as measurement_wide
    from
        `aou-pdr-data-prod.curation_data_view.unioned_ehr_measurement` m
        join `aou-pdr-data-prod.curation_data_view._mapping_measurement` mm on m.measurement_id = mm.measurement_id
        join wide_net wn on wn.descendant_concept_id = m.measurement_concept_id
    where
        m.measurement_concept_id in (
            select
                descendant_concept_id
            from
                wide_net
        )
    group by
        1, 2, 3
)
select
    hpo.hpo_id src_hpo_id, hlc.concept_id ancestor_concept_id, hlc.concept_name
    ancestor_concept_name,
    IFNULL(measurement_wide, 0) measurement_wide
from hpo_list hpo
cross join highest_level_components hlc
left join wide_measurement_counts wmc 
    on wmc.src_hpo_id = hpo.hpo_id
        and wmc.ancestor_concept_id = hlc.concept_id 

