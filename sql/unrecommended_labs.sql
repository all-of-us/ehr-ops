with full_list as (
    (
        select
            distinct descendant_concept_id,
            descendant_concept_name,
            c.concept_code
        from
            `aou-pdr-data-prod.curation_data_view.measurement_concept_sets_descendants` mc
            join `aou-pdr-data-prod.curation_data_view.concept` c on mc.descendant_concept_id = c.concept_id
        where
            ancestor_concept_id in (
                select
                    distinct ancestor_concept_id
                from
                    `aou-pdr-data-prod.curation_data_view.measurement_concept_sets_descendants`
                where
                    descendant_concept_id in (
                        3049187,
                        3053283,
                        40775801,
                        40779224,
                        40782562,
                        40782579,
                        40785850,
                        40785861,
                        40785869,
                        40789180,
                        40789190,
                        40789527,
                        40791227,
                        40792413,
                        40792440,
                        40795730,
                        40795740,
                        40795754,
                        40771922
                    )
                    and Panel_Name = "CMP"
            )
            and Panel_Name = "CMP"
            and ancestor_concept_id != descendant_concept_id
        group by
            1,
            2,
            3
    )
    union
    all (
        select
            distinct descendant_concept_id,
            descendant_concept_name,
            c.concept_code
        from
            `aou-pdr-data-prod.curation_data_view.measurement_concept_sets_descendants` mc
            join `aou-pdr-data-prod.curation_data_view.concept` c on mc.descendant_concept_id = c.concept_id
        where
            ancestor_concept_id in (
                select
                    distinct ancestor_concept_id
                from
                    `aou-pdr-data-prod.curation_data_view.measurement_concept_sets_descendants`
                where
                    descendant_concept_id in (
                        40785788,
                        40785796,
                        40779195,
                        40795733,
                        40795725,
                        40772531,
                        40779190,
                        40785793,
                        40779191,
                        40782561,
                        40789266
                    )
                    and Panel_Name = "CBC w/Diff"
            )
            and Panel_Name = "CBC w/Diff"
            and ancestor_concept_id != descendant_concept_id
        group by
            1,
            2,
            3
    )
    union
    all (
        select
            distinct descendant_concept_id,
            descendant_concept_name,
            c.concept_code
        from
            `aou-pdr-data-prod.curation_data_view.measurement_concept_sets_descendants` mc
            join `aou-pdr-data-prod.curation_data_view.concept` c on mc.descendant_concept_id = c.concept_id
        where
            ancestor_concept_id in (
                select
                    distinct ancestor_concept_id
                from
                    `aou-pdr-data-prod.curation_data_view.measurement_concept_sets_descendants`
                where
                    descendant_concept_id in (
                        40789356,
                        40789120,
                        40789179,
                        40772748,
                        40782735,
                        40789182,
                        40786033,
                        40779159
                    )
                    and Panel_Name = "CBC"
            )
            and Panel_Name = "CBC"
            and ancestor_concept_id != descendant_concept_id
        group by
            1,
            2,
            3
    )
    union
    all (
        select
            distinct descendant_concept_id,
            descendant_concept_name,
            c.concept_code
        from
            `aou-pdr-data-prod.curation_data_view.measurement_concept_sets_descendants` mc
            join `aou-pdr-data-prod.curation_data_view.concept` c on mc.descendant_concept_id = c.concept_id
        where
            ancestor_concept_id in (
                select
                    distinct ancestor_concept_id
                from
                    `aou-pdr-data-prod.curation_data_view.measurement_concept_sets_descendants`
                where
                    descendant_concept_id in (40782589, 40795800, 40772572)
                    and Panel_Name = "Lipids"
            )
            and Panel_Name = "Lipids"
            and ancestor_concept_id != descendant_concept_id
        group by
            1,
            2,
            3
    )
    union
    all (
        (
            select
                distinct descendant_concept_id,
                descendant_concept_name,
                c.concept_code
            from
                `aou-pdr-data-prod.curation_data_view.measurement_concept_sets_descendants` mc
                join `aou-pdr-data-prod.curation_data_view.concept` c on mc.descendant_concept_id = c.concept_id
            where
                ancestor_concept_id in (
                    select
                        distinct ancestor_concept_id
                    from
                        `aou-pdr-data-prod.curation_data_view.measurement_concept_sets_descendants`
                    where
                        descendant_concept_id in (
                            3037426,
                            3009531,
                            3029709,
                            3007950,
                            40760844,
                            3029305,
                            3025022,
                            21492990,
                            3032051,
                            3037242,
                            3042812,
                            3032459,
                            3030173,
                            3021601,
                            3030758,
                            3023539,
                            3008116,
                            3037072,
                            40763084,
                            3015736,
                            3035350,
                            40765224,
                            3016360,
                            3029870,
                            3032039,
                            3035999,
                            3008204,
                            3009417,
                            40763083,
                            3022621,
                            40763085,
                            3027162,
                            3029925,
                            3018954,
                            40762251,
                            3028893,
                            3026910,
                            3051227
                        )
                )
                and ancestor_concept_id != descendant_concept_id
            group by
                1,
                2,
                3
        )
        union
        all (
            (
                select
                    distinct descendant_concept_id,
                    descendant_concept_name,
                    c.concept_code
                from
                    `aou-pdr-data-prod.curation_data_view.measurement_concept_sets_descendants` mc
                    join `aou-pdr-data-prod.curation_data_view.concept` c on mc.descendant_concept_id = c.concept_id
                where
                    ancestor_concept_id in (
                        select
                            distinct ancestor_concept_id
                        from
                            `aou-pdr-data-prod.curation_data_view.measurement_concept_sets_descendants`
                        where
                            descendant_concept_id in (
                                3037426,
                                3009531,
                                3050746,
                                3013678,
                                3027388,
                                3011258,
                                40770956,
                                3009744,
                                3029709,
                                3007950,
                                3006140,
                                3009261,
                                40760844,
                                3016723,
                                3029305,
                                3030477,
                                40762887,
                                3041012,
                                3007943,
                                3025022,
                                3020044,
                                21492990,
                                3032051,
                                3037242,
                                3042812,
                                40758903,
                                3019038,
                                3020491,
                                3009542,
                                40762351,
                                3032459,
                                3012030,
                                3030173,
                                40765008,
                                3021601,
                                3030758,
                                3022038,
                                3051950,
                                3006576,
                                3020650,
                                3002385,
                                3019909,
                                3014053,
                                40757494,
                                3023539,
                                3008116,
                                3005456,
                                3025839,
                                3035995,
                                3020399,
                                3023103,
                                3037072,
                                3013682,
                                3024920,
                                40763084,
                                3015736,
                                3018834,
                                3022919,
                                46235203,
                                3005755,
                                3015182,
                                3001110,
                                3030260,
                                40765003,
                                3035350,
                                3011424,
                                43055143,
                                3019550,
                                40765224,
                                3048773,
                                40761551,
                                3016360,
                                3000483,
                                3000963,
                                42868692,
                                3016293,
                                3039896,
                                42868673,
                                3023314,
                                3029870,
                                3032039,
                                46235106,
                                3035999,
                                3006513,
                                3040519,
                                3013752,
                                3000285,
                                40762632,
                                3005772,
                                3037081,
                                46235204,
                                3027597,
                                3008204,
                                3010873,
                                3009417,
                                3024380,
                                3020564,
                                3051341,
                                40763083,
                                46235205,
                                3022621,
                                3024641,
                                3027997,
                                3051825,
                                40763085,
                                3013721,
                                3022192,
                                3022548,
                                3027162,
                                3004295,
                                3024731,
                                3051314,
                                3024629,
                                21490733,
                                3006923,
                                3004501,
                                3018572,
                                3029925,
                                3049383,
                                3002888,
                                3008770,
                                40765004,
                                3023599,
                                3018954,
                                40765007,
                                36305398,
                                3003338,
                                3024128,
                                40762251,
                                3019897,
                                3028833,
                                3028893,
                                3027484,
                                40762249,
                                3005570,
                                3014576,
                                3026910,
                                3019676,
                                3013826,
                                46235206,
                                3035941,
                                3028638,
                                3051227
                            )
                            and ancestor_concept_id != descendant_concept_id
                    )
                group by
                    1,
                    2,
                    3
            )
            union
            all (
                select
                    distinct concept_id as descendant_concept_id,
                    concept_name as descendant_concept_name,
                    c.concept_code
                from
                    `aou-pdr-data-prod.curation_data_view.concept` c
                where
                    concept_id in (
                        3029870,
                        3032039,
                        3035999,
                        3008204,
                        3009417,
                        40763083,
                        3022621,
                        40763085,
                        3027162,
                        3029925,
                        3018954,
                        40762251,
                        3028893,
                        3026910,
                        3051227,
                        3037426,
                        3009531,
                        3029709,
                        3007950,
                        40760844,
                        3029305,
                        3025022,
                        21492990,
                        3032051,
                        3037242,
                        3042812,
                        3032459,
                        3030173,
                        3021601,
                        3030758,
                        3023539,
                        3008116,
                        3037072,
                        40763084,
                        3015736,
                        3035350,
                        40765224,
                        3016360
                    )
                group by
                    1,
                    2,
                    3
            )
        )
    )
),
standard_codes as (
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
measurement_concept_ids as (
    select
        concept_id
    from
        `aou-pdr-data-prod.curation_data_view.concept`
    where
        concept_code in (
            select
                code
            from
                standard_codes
        )
),
non_recommended_codes as (
  select
        mm.src_hpo_id,
        m.measurement_concept_id, c.concept_code, c.concept_name, c.vocabulary_id
    from
        `aou-pdr-data-prod.curation_data_view.unioned_ehr_measurement` m
        join `aou-pdr-data-prod.curation_data_view._mapping_measurement` mm on m.measurement_id = mm.measurement_id
        join `aou-pdr-data-prod.curation_data_view.concept` c on c.concept_id = m.measurement_concept_id
    where
        m.measurement_concept_id in (
            select
                descendant_concept_id
            from
                full_list
        ) and
        m.measurement_concept_id not in (
            select
                concept_id
            from
                measurement_concept_ids            
        )
),
non_recommended_codes_with_components as (
    select
        nrc.*, rel_c.concept_id component_concept_id,
        rel_c.concept_code component_concept_code,
        cs.concept_synonym_name component_concept_name,
        REGEXP_EXTRACT(cs.concept_synonym_name, r'([^.\^\/]+)') analyte_subpart
    from non_recommended_codes nrc
    join `aou-pdr-data-prod.curation_data_view.concept_relationship` cr
        on cr.concept_id_1 = nrc.measurement_concept_id
    join `aou-pdr-data-prod.curation_data_view.concept` rel_c
        ON rel_c.concept_id = cr.concept_id_2
    join `aou-pdr-data-prod.curation_data_view.concept_synonym` cs
        on cs.concept_id = rel_c.concept_id 
    where cr.relationship_id = 'Has component' 
),
aggregate_non_recommended_codes as (
    select
        src_hpo_id, analyte_subpart,
        COUNT(*) lab_count
    from non_recommended_codes_with_components nrc
    group by src_hpo_id, analyte_subpart
)
select * from aggregate_non_recommended_codes
order by src_hpo_id, lab_count desc
