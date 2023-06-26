#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
import logging

from fastapi.responses import JSONResponse
from starlette import status

from services.base_app_cron_job import BaseAppCronJob
from google.cloud import bigquery

from datetime import date

_logger = logging.getLogger('aou_cloud')

OMOP_CLINICAL_TABLES = [
    'person', 'observation_period', 'visit_occurrence', 'visit_detail',
    'condition_occurrence', 'drug_exposure', 'procedure_occurrence',
    'device_exposure', 'measurement', 'observation', 'note', 'note_nlp',
    'specimen', 'fact_relationship', 'survey_conduct'
]

OMOP_HEALTH_SYTEM_TABLES = [
    'location', 'location_history', 'care_site', 'provider'
]

OMOP_VOCABULARY_TABLES = [
    'concept', 'vocabulary', 'domain', 'concept_class', 'concept_relationship',
    'relationship', 'concept_synonym', 'concept_ancestor',
    'source_to_concept_map', 'drug_strength', 'cohort'
]

OMOP_DERIVED_TABLES = ['drug_era', 'dose_era', 'condition_era']

OMOP_METADATA_TABLES = [
    'metadata',
    #'cdm_source' #exclude this table to re-generate manually
]

OMOP_TABLES = OMOP_CLINICAL_TABLES + OMOP_VOCABULARY_TABLES + OMOP_HEALTH_SYTEM_TABLES + OMOP_DERIVED_TABLES + OMOP_METADATA_TABLES


# TODO: Rename class and add to __all__ list in __init__.py.
class CopyEHRTablesJob(BaseAppCronJob):
    """ Copy OMOP tables from Curation project to EHR Ops project """
    # Name is an all lower case url friendly name for the job and should be unique.
    # TODO: Change 'job_name' once you create a copy of this file.
    job_name: str = 'copy_ehr_tables'

    def get_ehr_tables(self, client, dataset_parameters):
        curation_project = dataset_parameters['curation_project']
        ehr_ops_project = dataset_parameters['ehr_ops_project']

        unioned_ehr_dataset_name = dataset_parameters['ehr_ops_dataset']
        unioned_ehr_dataset_name = f'{curation_project}.{unioned_ehr_dataset_name}'
        omop_dataset_name = dataset_parameters['omop_dataset']
        omop_dataset_name = f'{ehr_ops_project}.{omop_dataset_name}'

        unioned_ehr_tables = list(client.list_tables(unioned_ehr_dataset_name))

        unioned_ehr_tables_names = [
            table.table_id for table in unioned_ehr_tables
        ]
        unioned_ehr_tables_dict = dict(
            zip(unioned_ehr_tables_names, unioned_ehr_tables))

        selected_omop_tables = []
        job_config = bigquery.CopyJobConfig()
        job_config.write_disposition = "WRITE_TRUNCATE"

        for omop_table in OMOP_TABLES:
            destination_omop_table_id = f'{omop_dataset_name}.{omop_table}'
            prefixed_omop_table_name = f'unioned_ehr_{omop_table}'

            if prefixed_omop_table_name in unioned_ehr_tables_dict:
                omop_table = prefixed_omop_table_name
            elif omop_table in unioned_ehr_tables_dict:
                omop_table = omop_table
            else:
                continue
                # raise Exception(
                #     f'OMOP table {omop_table} missing from unioned_ehr dataset'
                # )

            source_omop_table_id = f'{unioned_ehr_tables_dict[omop_table].project}.{unioned_ehr_tables_dict[omop_table].dataset_id}.{unioned_ehr_tables_dict[omop_table].table_id}'
            job = client.copy_table(source_omop_table_id,
                                    destination_omop_table_id,
                                    job_config=job_config)

            job.result()
            selected_omop_tables.append(omop_table)
            _logger.info(f'Copied table {omop_table}.')

        return selected_omop_tables

    def populate_cdm_source_table(self, client, dataset_parameters):
        ehr_ops_project = dataset_parameters['ehr_ops_project']
        omop_dataset_name = dataset_parameters['omop_dataset']
        omop_dataset_name = f'{ehr_ops_project}.{omop_dataset_name}'

        table_id = f'{omop_dataset_name}.cdm_source'

        # table_exists = False
        # try:
        #     client.get_table(table_id)
        #     table_exists = True
        # except:
        #     table_exists = False

        # if not table_exists:
        #     _logger.info(f'Creating cdm_source table because does not exist.')
        schema = [
            bigquery.SchemaField("cdm_source_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("cdm_source_abbreviation",
                                 "STRING",
                                 mode="NULLABLE"),
            bigquery.SchemaField("cdm_holder", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("source_description",
                                 "STRING",
                                 mode="NULLABLE"),
            bigquery.SchemaField("source_documentation_reference",
                                 "STRING",
                                 mode="NULLABLE"),
            bigquery.SchemaField("cdm_etl_reference",
                                 "STRING",
                                 mode="NULLABLE"),
            bigquery.SchemaField("source_release_date",
                                 "DATE",
                                 mode="NULLABLE"),
            bigquery.SchemaField("cdm_release_date", "DATE", mode="NULLABLE"),
            bigquery.SchemaField("cdm_version", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("vocabulary_version",
                                 "STRING",
                                 mode="NULLABLE"),
        ]

        rows_to_insert = [{
            "cdm_source_name": "AoU Unioned EHR Dataset",
            "cdm_source_abbreviation": "AoU",
            "source_description":
            "The union of EHR OMOP tables from all HPO sites participating in the AoU program.",
            "cdm_version": "5.3",
            "source_release_date": date.today().isoformat()
        }]

        load_job_config = bigquery.LoadJobConfig(schema=schema)
        load_job_config.write_disposition = "WRITE_TRUNCATE"

        job = client.load_table_from_json(rows_to_insert,
                                          destination=table_id,
                                          job_config=load_job_config)
        job.result()

        _logger.info(f'Successfully updated cdm_source table.')

    def run(self):
        """
        Entry point for cron job.
        :returns: JSONResponse
        """
        # Notes:
        #   self.payload: The request POST payload is stored in this property.
        #   self.gcp_env: The app context helper class is stored in this property.

        # Ensure we are pointed at the dev environment if running locally.
        self.gcp_env.override_project('aou-ehr-ops-curation-test')

        project = self.gcp_env.project
        app_config = self.gcp_env.get_app_config(project=project)

        client = bigquery.Client(project=project)
        dataset_parameters = app_config.dataset_parameters.to_dict()

        self.get_ehr_tables(client, dataset_parameters)
        self.populate_cdm_source_table(client, dataset_parameters)

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=f'Job {self.gcp_env.project}.{self.job_name} has completed.'
        )
