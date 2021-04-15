WITH 
    visit AS (
        SELECT DISTINCT mvo.src_hpo_id, vo.visit_occurrence_id
        FROM `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_visit_occurrence` vo
        LEFT JOIN `{{pdr_project}}.{{curation_dataset}}._mapping_visit_occurrence` mvo
            ON vo.visit_occurrence_id = mvo.visit_occurrence_id
    )

select condition.src_hpo_id, 
    IFNULL(condition.condition_rows_w_no_valid_vo, 0) as condition_rows_w_no_valid_vo, 
    IFNULL(procedure.procedure_rows_w_no_valid_vo, 0) as procedure_rows_w_no_valid_vo
from
    (
    SELECT DISTINCT mco.src_hpo_id, COUNT(co.condition_occurrence_id) as condition_rows_w_no_valid_vo
    FROM `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_condition_occurrence` co
    LEFT JOIN `{{pdr_project}}.{{curation_dataset}}._mapping_condition_occurrence` mco
        ON co.condition_occurrence_id = mco.condition_occurrence_id
    LEFT JOIN visit on visit.visit_occurrence_id = co.visit_occurrence_id
    WHERE 
        co.visit_occurrence_id NOT IN (visit.visit_occurrence_id)
        OR co.visit_occurrence_id = 0
        OR co.visit_occurrence_id IS NULL
    GROUP BY 1
    ORDER BY condition_rows_w_no_valid_vo DESC
    ) as condition
left join
    (
    SELECT DISTINCT mpo.src_hpo_id, COUNT(po.procedure_occurrence_id) as procedure_rows_w_no_valid_vo
    FROM `{{pdr_project}}.{{curation_dataset}}.unioned_ehr_procedure_occurrence` po
    LEFT JOIN `{{pdr_project}}.{{curation_dataset}}._mapping_procedure_occurrence` mpo
        ON po.procedure_occurrence_id = mpo.procedure_occurrence_id
    LEFT JOIN visit on visit.visit_occurrence_id = po.visit_occurrence_id
    WHERE 
        po.visit_occurrence_id NOT IN (visit.visit_occurrence_id)
        OR po.visit_occurrence_id = 0
        OR po.visit_occurrence_id IS NULL
    GROUP BY 1
    ORDER BY procedure_rows_w_no_valid_vo DESC
    ) as procedure
on condition.src_hpo_id = procedure.src_hpo_id