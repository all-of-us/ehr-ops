# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
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
# -

# # Setup Connection

# +
# Fill in identifiers

EHR_OPS_PROJECT_ID = ""
EXPORT_DATASET_ID = ""

# Add any other needed identifiers
EXPORT_DATASET2_ID = ""
# -

client = bigquery.Client(project=EHR_OPS_PROJECT_ID)

# # Execute Queries

# ## FHIR Patient Resource identifiers

# Description: Lists all identifiers associated with a FHIR Patient resource

# Define Query 1
q1 = f"""
SELECT
  id, identifier.system,
  identifier.value, identifier.system,
FROM `{EHR_OPS_PROJECT_ID}.{EXPORT_DATASET_ID}.Patient` p,
UNNEST(p.identifier) AS identifier
--WHERE p.id IN ('my_identifier_here')
LIMIT 100
"""

results1 = client.query(q1).to_dataframe()
results1

# ## FHIR Patients across Submissions

# Description: Identifies FHIR Patients submitted across submissions

# +
# Define Query 2

q2 = f"""
    WITH med_identifier1 AS (
      SELECT DISTINCT
        md.id
      FROM `{EHR_OPS_PROJECT_ID}.{EXPORT_DATASET_ID}.Patient` md,
        UNNEST(md.identifier) local_identifier
    ),
    med_identifier2 AS (
      SELECT DISTINCT
        md.id
      FROM `{EHR_OPS_PROJECT_ID}.{EXPORT_DATASET2_ID}.Patient` md,
        UNNEST(md.identifier) local_identifier
    )
    SELECT
      id
    FROM med_identifier2 md2
    EXCEPT DISTINCT
    SELECT
      id
    FROM  med_identifier1 md

"""
# -

results2 = client.query(q2).to_dataframe()
results2


# Description: Lists all participantIds mapped to the given FhirPatientID

q3 = '''
    SELECT other.patientId
    FROM
    (SELECT
      identifier.other
    FROM `{EHR_OPS_PROJECT_ID}.{EXPORT_DATASET_ID}.Patient` p,
    UNNEST(p.link) AS identifier
    WHERE p.id IN ('{FhirPatientID}')
    ) p2
'''

results3 = client.query(q3).to_dataframe()
results3