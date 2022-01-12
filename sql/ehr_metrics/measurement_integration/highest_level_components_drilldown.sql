with highest_level_components as (
    SELECT DISTINCT csd1.ancestor_concept_id concept_id, csd1.ancestor_concept_name concept_name
    FROM {{curation_ops_schema}}.measurement_concept_sets_descendants csd1
    LEFT JOIN {{curation_ops_schema}}.measurement_concept_sets_descendants csd2
    ON csd1.ancestor_concept_id = csd2.descendant_concept_id
    WHERE csd2.ancestor_concept_id IS NULL
),
recommended_concept_ids as (
    select DISTINCT
        hlc.concept_id ancestor_concept_id, c.concept_id descendant_concept_id
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
        hlc.concept_id ancestor_concept_id, csd.descendant_concept_id
    FROM highest_level_components hlc
    JOIN {{curation_ops_schema}}.measurement_concept_sets_descendants csd
        ON csd.ancestor_concept_id = hlc.concept_id
    JOIN {{vocab_schema}}.concept c
        ON c.concept_id = csd.descendant_concept_id 
    WHERE c.vocabulary_id = 'LOINC'
        AND c.concept_class_id IN ('Lab Test', 'Clinical Observation')
        AND hlc.concept_id IN (SELECT ancestor_concept_id FROM recommended_concept_ids)
),
non_recommended_codes as (
  select
        mm.src_hpo_id, m.measurement_id, wn.ancestor_concept_id,
        m.measurement_concept_id, c.concept_code, c.concept_name, c.vocabulary_id
    from
        {{curation_ops_schema}}.unioned_ehr_measurement m
        join {{curation_ops_schema}}._mapping_measurement mm on m.measurement_id = mm.measurement_id
        join {{vocab_schema}}.concept c on c.concept_id = m.measurement_concept_id
        join wide_net wn on wn.descendant_concept_id = c.concept_id
    where
        m.measurement_concept_id in (
            select
                descendant_concept_id
            from
                wide_net
        ) and
        m.measurement_concept_id not in (
            select
                descendant_concept_id
            from
                recommended_concept_ids            
        )
),
aggregate_non_recommended_codes as (
    select
        src_hpo_id, ancestor_concept_id,
        c.concept_name ancestor_concept_name,
        nrc.measurement_concept_id, nrc.concept_name concept_name,
        COUNT(*) lab_count
    from non_recommended_codes nrc
    join {{vocab_schema}}.concept c
        on c.concept_id = nrc.ancestor_concept_id
    group by src_hpo_id, ancestor_concept_id, ancestor_concept_name,
        nrc.measurement_concept_id, nrc.concept_name
)
select * from aggregate_non_recommended_codes
order by src_hpo_id, ancestor_concept_name, lab_count desc