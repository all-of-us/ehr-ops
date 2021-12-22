create function {{curation_dm_schema}}.safe_cast(p_in text, p_default int default null)
   returns int
as
$$
begin
  begin
    return $1::int;
  exception
    when others then
       return p_default;
  end;
end;
$$
language plpgsql;


-- This query checks against the coverage of unit_concept_id in measurement table and route_concept_id in drug_exposure table.
WITH
    sites AS (
        SELECT DISTINCT mp.src_hpo_id
        FROM {{curation_ops_schema}}.unioned_ehr_person p
        LEFT JOIN {{curation_ops_schema}}._mapping_person mp
          ON p.person_id = mp.person_id
    ),
    unit_success AS (
        SELECT DISTINCT mm.src_hpo_id, COUNT(distinct m.measurement_id) AS number_successful_units
        FROM {{curation_ops_schema}}.unioned_ehr_measurement m
        JOIN {{curation_ops_schema}}._mapping_measurement mm
          ON m.measurement_id = mm.measurement_id
        JOIN {{vocab_schema}}.concept c
          ON m.unit_concept_id = c.concept_id
        WHERE
            LOWER(c.standard_concept) = 's'
          AND
            LOWER(c.domain_id) LIKE '%unit%'
          AND ({{curation_dm_schema}}.safe_cast(replace(replace(replace(value_source_value, "<", ""), ">", ""), "=", "")) is not null
            OR value_as_number is not null)
        GROUP BY 1
        ORDER BY number_successful_units DESC
    ),
    route_success AS (
        SELECT DISTINCT mde.src_hpo_id, COUNT(de.drug_exposure_id) as number_valid_routes
        FROM {{curation_ops_schema}}.unioned_ehr_drug_exposure de
        LEFT JOIN {{curation_ops_schema}}._mapping_drug_exposure mde
            ON de.drug_exposure_id = mde.drug_exposure_id 
        LEFT JOIN {{vocab_schema}}.concept c
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
        FROM {{curation_ops_schema}}.unioned_ehr_measurement m
        LEFT JOIN {{curation_ops_schema}}._mapping_measurement mm
          ON m.measurement_id = mm.measurement_id
        WHERE
          {{curation_dm_schema}}.safe_cast(replace(replace(replace(value_source_value, "<", ""), ">", ""), "=", "")) is not null
          OR value_as_number is not null
        GROUP BY 1
        ORDER BY rows_w_units DESC
    )

select sites.src_hpo_id, unit_success.number_successful_units, measurement_w_unit.rows_w_units, route_success.number_valid_routes
from sites
    left join unit_success on sites.src_hpo_id = unit_success.src_hpo_id
    left join route_success on sites.src_hpo_id = route_success.src_hpo_id
    left join measurement_w_unit on sites.src_hpo_id = measurement_w_unit.src_hpo_id