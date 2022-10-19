# Drug Integration

This work contains a set of metrics for assessing the level of data quality in OMOP-formatted drug data. These metrics will cover the following criteria:

- Drug ingredient distributions
- Drug duration distributions

## File Breakdown

drug_ingredient_counts.sql: Returns the counts of individual drug ingredients placed into 10 drug classes.

drug_integration_rate.sql: Returns the proportions of drug classes represented in submissions

drug_recommended_rate.sql: Returns the proportions of recommended drug concepts represented in submissions

drug_class_durations.sql: Exploratory query to identify median drug durations of 10 drug classes

drug_class.csv: Contains 10 drug classes that are focused on for analysis by the queries.