with highest_level_components as (
    SELECT DISTINCT csd1.ancestor_concept_id concept_id, csd1.ancestor_concept_name concept_name
    FROM `aou-pdr-data-prod.curation_data_view.measurement_concept_sets_descendants` csd1
    LEFT JOIN `aou-pdr-data-prod.curation_data_view.measurement_concept_sets_descendants` csd2
    ON csd1.ancestor_concept_name = csd2.descendant_concept_name
    WHERE csd2.ancestor_concept_id IS NULL
),
wide_net as (
    SELECT
        hlc.concept_id ancestor_concept_id, csd.descendant_concept_id
    FROM highest_level_components hlc
    JOIN `aou-pdr-data-prod.curation_data_view.measurement_concept_sets_descendants` csd
        ON csd.ancestor_concept_id = hlc.concept_id
    JOIN `aou-pdr-data-prod.curation_data_view.concept` c
        ON c.concept_id = csd.descendant_concept_id 
    WHERE c.vocabulary_id = 'LOINC'
        AND c.concept_class_id = 'Lab Test'
),
recommended_codes as (
    select
        *
    from
        unnest(
            [
"14647-2",
"2093-3",
"14646-4",
"2085-9",
"26015-8",
"26017-4",
"49130-8",
"9832-7",
"9833-5",
"13457-7",
"18262-6",
"2089-1",
"22748-8",
"39469-2",
"49132-4",
"55440-2",
"69419-0",
"13458-5",
"2091-7",
"25371-6",
"49133-2",
"66126-4",
"12951-0",
"14927-8",
"1644-4",
"2571-8",
"3043-7",
"3048-6",
"30524-3",
"47210-0",
"70218-3",
"2947-0",
"2951-2",
"12812-4",
"12813-2",
"2823-3",
"6298-4",
"75940-7",
"2069-3",
"2075-0",
"1959-6",
"1963-8",
"14937-7",
"3094-0",
"59570-2",
"6299-2",
"14682-9",
"2160-0",
"38483-4",
"59826-8",
"14749-6",
"15074-8",
"2339-0",
"2340-8",
"2341-6",
"2345-7",
"72516-8",
"26464-8",
"49498-9",
"6690-2",
"804-5",
"26453-1",
"789-8",
"790-6",
"20509-6",
"55782-7",
"59260-0",
"718-7",
"20570-8",
"31100-1",
"4544-3",
"4545-0",
"48703-3",
"26515-7",
"49497-1",
"777-3",
"778-1",
"14631-6",
"1975-2",
"42719-5",
"54363-7",
"14629-0",
"15152-2",
"1968-7",
"29760-6",
"1751-7",
"2862-1",
"54347-0",
"61151-7",
"61152-5",
"62234-0",
"62235-7",
"76631-1",
"2885-2",
"1783-0",
"6768-6",
"1920-8",
"30239-8",
"88112-8",
"1742-6",
"1743-4",
"1744-2",
"76625-3",
"2324-2",
"2756-5",
"50560-2",
"5803-2",
"15076-3",
"22705-8",
"2350-7",
"53328-1",
"5792-7",
"59156-0",
"2888-6",
"50561-0",
"5804-0",
"1978-6",
"20505-4",
"41016-7",
"53327-3",
"68367-2",
"70199-5",
"20405-7",
"3107-0",
"34927-4",
"34928-2",
"50563-6",
"60025-4",
"22702-5",
"49779-2",
"50557-8",
"5797-6",
"59158-6",
"20407-3",
"2657-5",
"20408-1",
"24122-4",
"30405-5",
"51487-7",
"58805-3",
"2349-9",
"25428-4",
"50555-2",
"20454-5",
"2887-8",
"53525-2",
"57735-3",
"1977-8",
"50551-1",
"5770-3",
"58450-8",
"13658-0",
"5818-0",
"62487-4",
"53963-5",
"2514-8",
"33903-6",
"57734-6",
"32710-6",
"50558-6",
"5802-4",
"33052-2",
"53316-6",
"53964-3",
"32167-9",
"50552-9",
"50553-7",
"5778-6",
"2106-3",
"2112-1",
"80384-1",
"2880-3",
"76665-9",
"76666-7",
"76667-5",
"76668-3",
"14744-7",
"2342-4",
"76669-1",
"76670-9",
"76671-7",
"76672-5",
"1746-7",
"2861-3",
"76480-3",
"2520-5",
"27941-4",
"60023-9",
"60024-7",
"26465-5",
"46088-1",
"46089-9",
"46090-7",
"48034-3",
"76674-1",
"805-2",
"806-0",
"26454-9",
"46087-3",
"46091-5",
"46092-3",
"76673-3",
"791-4",
"792-2",
"2464-6",
"76489-4",
"2471-1",
"2457-0",
"11272-2",
"30428-7",
"47282-9",
"62242-3",
"787-2",
"28539-5",
"47278-7",
"62243-1",
"785-6",
"28540-3",
"47279-5",
"62246-4",
"786-4",
"21000-5",
"30384-2",
"30385-9",
"47277-9",
"62247-2",
"788-0",
"23761-0",
"26511-6",
"770-8",
"26478-8",
"30365-1",
"736-9",
"737-7",
"26485-3",
"5905-5",
"744-3",
"26450-7",
"713-8",
"714-6",
"26450-7",
"713-8",
"714-6",
"26499-4",
"66139-7",
"74398-9",
"751-8",
"753-4",
"26474-7",
"30364-4",
"66140-5",
"731-0",
"732-8",
"74401-1",
"26484-6",
"66143-9",
"742-7",
"743-5",
"74399-7",
"26449-9",
"66141-3",
"711-2",
"712-0",
"74404-5",
"26444-0",
"66142-1",
"704-7",
"705-4",
"74406-0"]
        ) as code
),
recommended_concept_ids as (
    select
        hlc.concept_id ancestor_concept_id, c.concept_id descendant_concept_id
    from
        `aou-pdr-data-prod.curation_data_view.concept` c
    JOIN `aou-pdr-data-prod.curation_data_view.measurement_concept_sets_descendants` csd
        ON csd.descendant_concept_id = c.concept_id
    JOIN highest_level_components hlc
        ON hlc.concept_id = csd.ancestor_concept_id
    where
        concept_code in (
            select
                code
            from
                recommended_codes
        )
),
recommended_measurement_counts as (
    select
        mm.src_hpo_id,
        count(distinct m.measurement_id) as measurement_recommended
    from
        `aou-pdr-data-prod.curation_data_view.unioned_ehr_measurement` m
        join `aou-pdr-data-prod.curation_data_view._mapping_measurement` mm on m.measurement_id = mm.measurement_id
    where
        m.measurement_concept_id in (
            select
                descendant_concept_id
            from
                recommended_concept_ids
        )
    group by
        1
),
wide_measurement_counts as (
    select
        mm.src_hpo_id,
        count(distinct m.measurement_id) as measurement_wide
    from
        `aou-pdr-data-prod.curation_data_view.unioned_ehr_measurement` m
        join `aou-pdr-data-prod.curation_data_view._mapping_measurement` mm on m.measurement_id = mm.measurement_id
    where
        m.measurement_concept_id in (
            select
                descendant_concept_id
            from
                wide_net
        )
    group by
        1
)
select
    w.src_hpo_id,
    measurement_recommended,
    measurement_wide
from
    recommended_measurement_counts r
    left join wide_measurement_counts w on r.src_hpo_id = w.src_hpo_id
