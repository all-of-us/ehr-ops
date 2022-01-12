with highest_level_components as (
    SELECT DISTINCT csd1.ancestor_concept_id concept_id, csd1.ancestor_concept_name concept_name
    FROM {{curation_ops_schema}}.measurement_concept_sets_descendants csd1
    LEFT JOIN {{curation_ops_schema}}.measurement_concept_sets_descendants csd2
    ON csd1.ancestor_concept_id = csd2.descendant_concept_id
    WHERE csd2.ancestor_concept_id IS NULL
),
hpo_list as (
    SELECT
        LOWER(hpo.HPO_ID) hpo_id
    FROM {{curation_dm_schema}}.v_org_hpo_mapping_temp hpo
),
recommended_concept_ids as (
    select
        hlc.concept_id ancestor_concept_id, hlc.concept_name ancestor_concept_name,
        c.concept_id descendant_concept_id
    from
        {{vocab_schema}}.concept c
    JOIN {{curation_ops_schema}}.measurement_concept_sets_descendants csd
        ON csd.descendant_concept_id = c.concept_id
    JOIN highest_level_components hlc
        ON hlc.concept_id = csd.ancestor_concept_id
    where
        c.concept_id in (
            select
                concept_id
            from
                {{curation_dm_schema}}.recommended_measurement_concepts
        )
),
wide_net as (
    SELECT DISTINCT
        hlc.concept_id ancestor_concept_id, csd.descendant_concept_id,
        hlc.concept_name ancestor_concept_name
    FROM highest_level_components hlc
    JOIN {{curation_ops_schema}}.measurement_concept_sets_descendants csd
        ON csd.ancestor_concept_id = hlc.concept_id
    JOIN {{vocab_schema}}.concept c
        ON c.concept_id = csd.descendant_concept_id 
    WHERE c.vocabulary_id = 'LOINC'
        AND c.concept_class_id IN ('Lab Test', 'Clinical Observation')
        AND hlc.concept_id IN (SELECT ancestor_concept_id FROM recommended_concept_ids)
),
recommended_measurement_counts as (
    select
        mm.src_hpo_id, rci.ancestor_concept_id, rci.ancestor_concept_name,
        count(distinct m.measurement_id) as measurement_recommended
    from
        {{curation_ops_schema}}.unioned_ehr_measurement m
        join {{curation_ops_schema}}._mapping_measurement mm on m.measurement_id = mm.measurement_id
        join recommended_concept_ids rci on rci.descendant_concept_id = m.measurement_concept_id
    where
        m.measurement_concept_id in (
            select
                descendant_concept_id
            from
                recommended_concept_ids
        )
    group by
        1, 2, 3
),
wide_measurement_counts as (
    select
        mm.src_hpo_id, wn.ancestor_concept_id, wn.ancestor_concept_name,
        count(distinct m.measurement_id) as measurement_wide
    from
        {{curation_ops_schema}}.unioned_ehr_measurement m
        join {{curation_ops_schema}}._mapping_measurement mm on m.measurement_id = mm.measurement_id
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
    COALESCE(measurement_wide, 0) measurement_wide,
    COALESCE(measurement_recommended, 0) measurement_recommended
from hpo_list hpo
cross join highest_level_components hlc
left join wide_measurement_counts wmc 
    on wmc.src_hpo_id = hpo.hpo_id
        and wmc.ancestor_concept_id = hlc.concept_id
left join recommended_measurement_counts r
    on r.src_hpo_id = hpo.hpo_id
        and r.ancestor_concept_id = hlc.concept_id
WHERE hlc.concept_id IN (SELECT ancestor_concept_id FROM recommended_concept_ids)