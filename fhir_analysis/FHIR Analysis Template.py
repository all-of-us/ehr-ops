# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.16.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# # Load Packages

# +
# Add any needed packages

from google.cloud import bigquery
import configparser

config = configparser.ConfigParser()
config.read('config.ini')
# -

# # Setup Connection

# +
# Fill in identifiers

EHR_OPS_PROJECT_ID = config['project']['EHR_OPS_PROJECT_ID']
EHR_OPS_DATASET_ID = config['project']['EHR_OPS_DATASET_ID']

# Add any other needed identifiers
# -

client = bigquery.Client(project=EHR_OPS_PROJECT_ID)

# # Execute Queries

# ## Query 1

# Description: Pulls the first 10 rows in the measurement table

# Define Query 1
q1 = f"""
    SELECT
        *
    FROM `{EHR_OPS_PROJECT_ID}.{EHR_OPS_DATASET_ID}.unioned_ehr_measurement`
    LIMIT 10
"""

results1 = client.query(q1).to_dataframe()
results1

# ## Query 2

# Description: Pulls the first 10 rows in the procedure_occurrence table

# +
# Define Query 2

q2 = f"""
    SELECT
        *
    FROM `{EHR_OPS_PROJECT_ID}.{EHR_OPS_DATASET_ID}.unioned_ehr_procedure_occurrence`
    LIMIT 10   
"""
# -

results2 = client.query(q2).to_dataframe()
results2
