## **EHR Ops Dashboard**
This repository is to guide you through EHR Ops Dashboard. EHR Ops Dashboard is a tableau dashboard provided by AllofUs analytics team to reflect the data quality across over HPOs.

### High Level Summary ###
1. Definition of metrics and related queries
2. How to read the dashboard
3. What's to expect in new coming features 
---
#### Definition of Metrics and Related Queries ####

**1. Data Transfer Rate A** 
- Definition: The number of participants with EHR data per HPO divided by the number of all participants eligible for EHR data transfer
- Metrics: Numerator is generated by `eligible_participants_ehr.sql`. Denominator is generated by `all_eligible_participants.sql`

**2. Data Transfer Rate B**
- Definition: The number of participants with EHR data per HPO divided by the number of all participants eligible for EHR data transfer who have completed an in-person visit (physical measurements OR biospecimen) 
- Metrics: Numerator is generated by `eligible_participants_ehr.sql`. Denominator is generated by `in_person_participants.sql`

**3. General Conformance**
- GC-1 \
Every row in the condition_occurrence, visit_occurrence, procedure_occurrence, drug_exposure, measurement and observation tables should have a well defined concept_id
    - GC-1 Standard
        - Every row should have a standard concept_id with correct domain and it's not null or 0. 
        - The metric is generated by `gc_1_standard.sql`.
    - GC-1 Source
        - Every row should have a source concept_id with correct domain and it's not null or 0.
        - The metric is generated by `gc_1_source.sql`.
- GC-2 \
Every foreign keys should be valid. For example, the foreign keys checked in condition_occurrence_table includes condition_concept_id, visit_occurrence_id, person_id, condition_type_concept_id, condition_source_concept_id, condition_status_concept_id
- GC-3
    - GC-3 Standard
        - Every row should have a concept_id which is not null or 0. 
        - The metric is generated by `gc_1_standard.sql`.
    - GC-3 Source
        - Every row should have a source concept_id which is not null or 0.
        - The metric is generated by `gc_1_source.sql`.

**4. Date Conformance**
- DC-1
    - There should not be any end dates before start dates in any of the clinical data tables.
    - The metric is generated by `dc_1.sql`
- DC-2
    - No data points should exist beyond 30 days of death date, if applicable.
    - The metric is generated by `dc_2.sql`
- DC-3
    - No dates should be prior to 1900 (for observation) or 1980 (for other clinical data tables).
    - The metric is generated by `dc_3.sql`
- DC-4
    - Date and datetime fields should match.
    - The metric is generated by `dc_4.sql`

**5. General DQ Metrics**
- Unit Concept Failure
    - In measurement table, rows with numerical value_source_value or value_as_number should have standard concept_id and have the domain "Unit"
    - The metric is generated by `unit_route_failure.sql`
- Route Concept Failure
    - In drug_exposure table, concept_ids should both be standard and have the domain "Route"
    - The metric is generated by `unit_route_failure.sql`
- Visit ID Failure
    - In condition and procedure tables, rows should not have null/0 visit_occurrence_id and these visit_occrrence_id should also exist in visit_occurrence table
    - The metric is generated by `visit_id_failure.sql`
- Duplicates
    - In each table, there should not be duplicated rows. 
    - The metric is generated by `duplicates.sql`

**6. COVID Mapping**
    - In measurement table, rows related to COVID-19 results (i.e. rows with concept_id being desendant of 756055) should have neither null or 0 in value_as_concept_id and the value_source_value field should be mapped to standard concepts we provide on our website. 
    - https://sites.google.com/view/ehrupload/data-quality-metrics/covid-19-data-mapping?authuser=0 
    - The metric is generated by `covid_mapping.sql`

**7. Physical Measurements**
    - All sites should be submitting data on participants' physical measurements. Specifically, we check against body height, body weight, and BMI.
    - The metric is generated by `physical_meas.sql`

#### How to Read the Dashboard ####

- Filter Session
    - You could filter on the awardee or organization level to focus on the sites that you are interested in.
    - Click the filter and then apply the selections.

- Aggregate Data Transfer Rate Session
    - This session shows aggregate number of awardees/organizations with data transfer rate falling in different threshold.
    - The calculation of Data Transfer Rate A and B are in the tooltips when you hover over the aggreagate numbers. 
    - If you click on the number, the dashboard will be filtered to show the sites which are in that number category only.

- Data Transfer Rate Session
    - This session is a detailed dashboard with all organizations' metrics for Data Transfer Rate and most recent submission time.
    - You could hover over the green bar to see the break-down of participants with different status (eligible participants, eligible participants with EHR data and eligible participants with in-person visit) 
    - You could also hover over the blue dot to see details of the metrics.
    - The calculation and definition are in the tooltips.

- Data Conformance Session
    - This session is showing the general conformance and date conformance metrics.
    - If you hover over the cell, the success rate break-down by each table will show up. This is for your reference to check your table in further detail. 

- Definition Session
    - This session is giving you the definition of each jargon and metric. 

#### What's to expect in new coming features? ####

Here are the metrics still under design and implementation:
- Trending chart of metrics
- Other measurements and drug integration

## SQL Compilation
Use `compile.py` to render, or compile, query templates into runnable SQL.

### Requirements
* Python 3.7+

_Note: The examples below assume a modern shell environment._

### Example 1: Compiling a single template file
Assuming this is the contents of `query_1.sql`:
```sql
SELECT 
 field_1
,field_2
FROM `{{project_id}}.{{dataset_id}}.table_1`
```
executing
```bash
python compile.py query_1.sql --project_id 'drc-prod' --dataset_id 'ehr-ops'  
```
stores the compiled file at `.compiled/query_1.sql`
```sql
SELECT 
 field_1
,field_2
FROM `drc-prod.ehr-ops.table_1`
```

### Example 2: Compiling multiple files
Assuming these template files exist: 
```
query_1.sql
metrics/query_2.sql
```
executing the command
```bash
python compile.py **/*.sql --project_id 'drc-prod' --dataset_id 'ehr-ops'   
```
compiles them and saves the results at: 
```
.compiled/query_1.sql
.compiled/metrics/query_2.sql
```
