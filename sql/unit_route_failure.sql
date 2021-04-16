WITH
    sites AS (
        SELECT DISTINCT mp.src_hpo_id
        FROM `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_person` p
        LEFT JOIN `{{pdr_project}}.{{curation_dataset}}._mapping_person` mp
          ON p.person_id = mp.person_id
    ),
    unit_success AS (
        SELECT DISTINCT mm.src_hpo_id, COUNT(distinct m.measurement_id) AS number_successful_units
        FROM `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_measurement` m
        JOIN `{{pdr_project}}.{{curation_dataset}}._mapping_measurement` mm
          ON m.measurement_id = mm.measurement_id
        JOIN `{{pdr_project}}.{{curation_dataset}}.concept` c
          ON m.unit_concept_id = c.concept_id
        WHERE
            LOWER(c.standard_concept) = 's'
          AND
            LOWER(c.domain_id) LIKE '%unit%'
          AND (safe_cast(replace(replace(replace(value_source_value, "<", ""), ">", ""), "=", "") as float64) is not null 
            OR safe_cast(value_as_number AS float64) is not null)
        GROUP BY 1
        ORDER BY number_successful_units DESC
    ),
    route_success AS (
        SELECT DISTINCT mde.src_hpo_id, COUNT(de.drug_exposure_id) as number_valid_routes
        FROM `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_drug_exposure` de
        LEFT JOIN `{{pdr_project}}.{{curation_dataset}}._mapping_drug_exposure` mde
            ON de.drug_exposure_id = mde.drug_exposure_id 
        LEFT JOIN `{{pdr_project}}.{{curation_dataset}}.concept` c
            ON de.route_concept_id = c.concept_id
        WHERE
            LOWER(c.standard_concept) = 's'
          AND
            LOWER(c.domain_id) LIKE '%route%'
        GROUP BY 1
        ORDER BY number_valid_routes DESC
    ),
    measurement_w_unit AS (
        SELECT DISTINCT mm.src_hpo_id, COUNT(distinct m.measurement_id) AS rows_w_units
        FROM `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_measurement` m
        LEFT JOIN `{{pdr_project}}.{{curation_dataset}}._mapping_measurement` mm
          ON m.measurement_id = mm.measurement_id
        WHERE
          safe_cast(replace(replace(replace(value_source_value, "<", ""), ">", ""), "=", "") as float64) is not null 
          OR safe_cast(value_as_number AS float64) is not null
        GROUP BY 1
        ORDER BY rows_w_units DESC
    )

select sites.src_hpo_id, unit_success.number_successful_units, measurement_w_unit.rows_w_units, route_success.number_valid_routes
from sites
    left join unit_success on sites.src_hpo_id = unit_success.src_hpo_id
    left join route_success on sites.src_hpo_id = route_success.src_hpo_id
    left join measurement_w_unit on sites.src_hpo_id = measurement_w_unit.src_hpo_id