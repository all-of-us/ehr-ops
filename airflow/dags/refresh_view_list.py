from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks import bigquery
from airflow.models import Variable
import pendulum
import networkx as nx
import json
from pathlib import Path
import re
from google.cloud import bigquery as python_client_bigquery

import os

os.environ["no_proxy"] = "*"


@dag(schedule_interval=None,
     start_date=pendulum.datetime(2023, 1, 1, tz="UTC"))
def refresh_view_list():

    EHR_OPS_PROJECT_ID = Variable.get('ehr_ops_project_id')
    EHR_OPS_RESOURCES_DATASET_ID = Variable.get('ehr_ops_resources_dataset_id')

    @task(task_id=f'retrieve_metric_view_list')
    def retrieve_metric_view_list():
        hook = bigquery.BigQueryHook(gcp_conn_id='aou_ehr_ops_curation_test',
                                     use_legacy_sql=False)

        client = hook.get_client()
        # client = python_client_bigquery.Client(
        #     project=hook._get_field('project'),
        #     credentials=hook._get_credentials())

        sql = f"""
            SELECT
                table_catalog project_id, table_schema dataset_id, table_name view_name
            FROM `{EHR_OPS_PROJECT_ID}.{EHR_OPS_RESOURCES_DATASET_ID}.INFORMATION_SCHEMA.TABLES`
            WHERE table_type='VIEW'
        """

        results = client.query(sql)
        views = [dict(row) for row in results]

        return views

    @task(task_id='update_view_list_variable')
    def update_view_list_variable(views):
        Variable.set("bq_view_list", views, serialize_json=True)

    update_view_list_variable(retrieve_metric_view_list())


refresh_view_list()