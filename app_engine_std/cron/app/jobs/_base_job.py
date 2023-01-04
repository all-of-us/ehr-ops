#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
import logging
from datetime import datetime
from hashlib import md5
from services.app_context_manager import GenericJSONStructure

from services.app_context_base import AppEnvContextBase
from aou_cloud.services.gcp_cloud_tasks import GCPCloudTask
from aou_cloud.services.gcp_google_pubsub import GCPGooglePubSubTopic
from fastapi import HTTPException
from fastapi.responses import JSONResponse
from starlette import status

from aou_cloud.services.system_utils import JSONObject
from google.cloud import bigquery
import networkx as nx
from time import sleep

_logger = logging.getLogger('aou_cloud')


class BaseCronJob:
    """ Base cron job class """
    # Name is an all lower case url friendly name for the job and should be unique.
    job_name: str = 'unknown'
    gcp_env: AppEnvContextBase = None
    payload: JSONObject = None

    def __init__(self, gcp_env, payload: GenericJSONStructure = None):
        """
        :param gcp_env: AppEnvObject object.
        """
        self.gcp_env = gcp_env
        if payload:
            # Using JSONObject here will convert the binary keys and values in the dict to utf-8.
            self.payload = JSONObject(payload)
        _logger.info(f'Starting Cron Job: {self.job_name}')

    def run(self):
        """
        (Virtual Function) Entry point for cron job, override in child class.
        :returns: JSONResponse
        """
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=
            f'Job {self.gcp_env.project}.{self.job_name} has failed, invalid configuration.'
        )


class ManagedCronJob(BaseCronJob):
    """ Cron job that executes in a dependency order """
    task_list: dict = None
    publish_response: bool = False
    pub_sub_success_topic: str = None
    pub_sub_failed_topic: str = None

    def __init__(
        self,
        gcp_env,
        payload: GenericJSONStructure = None,
    ):
        super().__init__(gcp_env, payload)

        # Ensure we are pointed at PDR dev environment if running locally.
        self.gcp_env.override_project('aou-ehr-ops-curation-test')

        self.job_table = f"{self.gcp_env.project}.ehr_ops_resources.job"
        self.job_instance_table = f"{self.gcp_env.project}.ehr_ops_resources.job_instance"
        self.task_instance_table = f"{self.gcp_env.project}.ehr_ops_resources.task_instance"

    def register_job(self):
        client = bigquery.Client(project=self.gcp_env.project)

        # Check if job is already registered
        check_job_existence_query = client.query(
            f"SELECT job_id FROM `{self.job_table}` WHERE job_name='{self.job_name}'"
        )
        result = check_job_existence_query.result()

        job_id = md5(f'{self.job_name}'.encode('utf-8')).hexdigest()
        # Register job if it is not already
        if len(list(result)) == 0:
            register_job_query = client.query(f"""
                INSERT `{self.job_table}` (job_id, job_name)
                VALUES('{job_id}', '{self.job_name}')
            """)
            register_job_query.result()

            _logger.info(f'Registered job {self.job_name} for tracking')

        return job_id

    def create_job_instance(self):
        client = bigquery.Client(project=self.gcp_env.project)
        start_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        job_instance_id = md5(
            f'{self.job_name}{start_datetime}'.encode('utf-8')).hexdigest()

        create_job_instance_query = client.query(f"""
            INSERT `{self.job_instance_table}` (job_instance_id, job_id, start_datetime, end_datetime, status)
            SELECT
                '{job_instance_id}' job_instance_id, job_id,
                TIMESTAMP('{start_datetime}') start_datetime, NULL end_datetime,
                'RUNNING' status
            FROM `{self.job_table}`
            WHERE job_name = '{self.job_name}'
        """)
        create_job_instance_query.result()

        _logger.info(f'Created job instance.')

        return job_instance_id

    def create_task_instance_queue(self, job_id, job_instance_id):
        client = bigquery.Client(project=self.gcp_env.project)
        task_instance_ids = []
        for task_id, _ in self.task_list.items():
            start_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            task_instance_id = md5(f'{job_id}{task_id}{start_datetime}'.encode(
                'utf-8')).hexdigest()
            create_task_instance_query = client.query(f"""
                INSERT `{self.task_instance_table}` (task_instance_id, job_id, job_instance_id, task_name, start_datetime, end_datetime, status)
                VALUES(
                    '{task_instance_id}', '{job_id}', '{job_instance_id}', '{task_id}',
                    NULL, NULL,
                    'QUEUED')
            """)
            create_task_instance_query.result()

            _logger.info(f'Queued task instance {task_id}.')

            self.task_list[task_id]['task_kwargs']['payload'][
                'task_instance_id'] = task_instance_id
            self.task_list[task_id]['task_kwargs']['payload'][
                'task_id'] = task_id
            self.task_list[task_id]['task_kwargs']['payload'][
                'job_instance_id'] = job_instance_id

            task_instance_ids.append(task_instance_id)

        return task_instance_ids

    def check_if_dag(self):
        g = nx.DiGraph()
        for task_id, attr in self.task_list.items():
            g.add_node(task_id)
            dependencies = attr['task_kwargs']['payload']['depends_on']
            for dependency in dependencies:
                g.add_edge(task_id, dependency)

        assert nx.is_directed_acyclic_graph(g)

        # sort tasks in topological order so queue is ordered well
        sorted_tasks = list(reversed(list(nx.topological_sort(g))))
        return sorted_tasks

    def check_task_status(self, task_instance_id):
        client = bigquery.Client(project=self.gcp_env.project)
        status_query = client.query(
            f"SELECT status FROM `{self.task_instance_table}` WHERE task_instance_id = '{task_instance_id}'"
        )
        s = next(status_query.result())['status']

        return s

    def update_job_status(self, job_instance_id, new_status):
        client = bigquery.Client(project=self.gcp_env.project)
        if new_status in ('SUCCESS', 'FAILED'):
            end_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            update_status_query = client.query(f"""
                UPDATE `{self.job_instance_table}`
                SET 
                    status = '{new_status}',
                    end_datetime = TIMESTAMP('{end_datetime}')
                WHERE job_instance_id = '{job_instance_id}'
            """)
        else:
            update_status_query = client.query(f"""
                UPDATE `{self.job_instance_table}`
                SET 
                    status = '{new_status}'
                WHERE job_instance_id = '{job_instance_id}'
            """)

        update_status_query.result()

    def run(self):
        """
        Entry point for cron cloud task job.
        :returns: JSONResponse
        """
        # Notes:
        #   self.payload: The request POST payload is stored in this property.
        #   self.gcp_env: The app context helper class is stored in this property.

        # Ensure we are pointed at PDR dev environment if running locally.
        self.gcp_env.override_project('aou-ehr-ops-curation-test')

        # Ensure the job is registered
        job_id = self.register_job()

        # Ensure that tasks are in a dag
        sorted_task_ids = self.check_if_dag()

        # Initialize job instance
        job_instance_id = self.create_job_instance()

        # Initialize task queue
        task_instance_ids = self.create_task_instance_queue(
            job_id, job_instance_id)

        # Start tasks

        for task_id in sorted_task_ids:
            task_kwargs = self.task_list[task_id]['task_kwargs']
            GCPCloudTask().execute(**task_kwargs)
        # for _, args in self.task_list.items():
        #     task_kwargs = args['task_kwargs']

        # Poll job until complete or prematurely terminated
        running_tasks = task_instance_ids
        running_tasks_dict = {}
        running = True
        while running:
            sleep(5)
            for task_instance_id in list(running_tasks):
                task_status = self.check_task_status(task_instance_id)
                running_tasks_dict[task_instance_id] = task_status
                if task_status not in ('RUNNING', 'QUEUED'):
                    running_tasks.remove(task_instance_id)
            if len(running_tasks) == 0:
                running = False

        if all([s == 'SUCCESS' for s in running_tasks_dict.values()]):
            self.update_job_status(job_instance_id, 'SUCCESS')
            if self.publish_response:
                topic = GCPGooglePubSubTopic(self.gcp_env.project,
                                             self.pub_sub_success_topic)
                topic.publish("completed")
            return JSONResponse(
                status_code=status.HTTP_200_OK,
                content=
                f'Job {self.gcp_env.project}.{self.job_name} has completed.')

        self.update_job_status(job_instance_id, 'FAILED')
        if self.publish_response:
            topic = GCPGooglePubSubTopic(self.gcp_env.project,
                                         self.pub_sub_failed_topic)
            topic.publish("failed")
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content=
            f'Job {self.gcp_env.project}.{self.job_name} has failed due to failure of 1 or more tasks.'
        )
