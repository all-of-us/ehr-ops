#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
import logging
import traceback
from services.app_context_manager import GenericJSONStructure

from services.app_context_base import AppEnvContextBase
from fastapi import HTTPException
from starlette import status
from fastapi.responses import JSONResponse

from aou_cloud.services.system_utils import JSONObject
from google.cloud import bigquery
from time import sleep
from datetime import datetime, timedelta

_logger = logging.getLogger('aou_cloud')


class BaseCronTask:
    """ Base cron task class """
    # Name is an all lower case url friendly name for the task and should be unique.
    task_name: str = 'unknown'
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
        _logger.info(f'Starting Cron Task: {self.task_name}')

    def run(self):
        """
        (Virtual Function) Entry point for cron task, override in child class.
        :returns: JSONResponse
        """
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=
            f'Job {self.gcp_env.project}.{self.task_name} has failed, invalid configuration.'
        )


class ManagedCronTask:
    """ Cron task that is executed in a dependency order """
    # Name is an all lower case url friendly name for the task and should be unique.
    task_name: str = 'unknown'
    gcp_env: AppEnvContextBase = None
    payload: JSONObject = None
    timeout: int = 600

    def __init__(self, gcp_env, payload: GenericJSONStructure = None):
        """
        :param gcp_env: AppEnvObject object.
        """
        self.gcp_env = gcp_env
        if payload:
            # Using JSONObject here will convert the binary keys and values in the dict to utf-8.
            self.payload = JSONObject(payload)

        # Ensure we are pointed at PDR dev environment if running locally.
        self.gcp_env.override_project('aou-ehr-ops-curation-test')

        self.task_instance_table = f"{self.gcp_env.project}.ehr_ops_resources.task_instance"
        self.task_instance_id = self.payload.task_instance_id
        self.task_id = self.payload.task_id
        self.job_instance_id = self.payload.job_instance_id

        self.depends_on = self.payload.depends_on

        _logger.info(f'Starting Cron Task: {self.task_id}')

    def check_status(self, task_instance_id):
        client = bigquery.Client(project=self.gcp_env.project)
        status_query = client.query(
            f"SELECT status FROM `{self.task_instance_table}` WHERE task_instance_id = '{task_instance_id}'"
        )
        s = next(status_query.result())['status']

        return s

    def get_dependency_statuses(self):
        client = bigquery.Client(project=self.gcp_env.project)

        statuses = []
        for dependency in self.depends_on:
            # dependency_task_id_query = client.query(f"""
            #     SELECT
            #         task_instance_id
            #     FROM `{self.task_instance_table}`
            #     WHERE job_instance_id = '{self.job_instance_id}'
            #         AND task_name = '{dependency}'
            # """)
            # result = dependency_task_id_query.result()
            # if result.total_rows > 0:
            #     dependency_task_instance_id = next(result)['task_instance_id']

            #     s = self.check_status(dependency_task_instance_id)
            #     statuses.append(s)

            dependency_task_status_query = client.query(f"""
                SELECT 
                    status 
                FROM `{self.task_instance_table}`
                WHERE job_instance_id = '{self.job_instance_id}'
                    AND task_name = '{dependency}'
            """)
            result = dependency_task_status_query.result()
            if result.total_rows > 0:
                dependency_task_status = next(result)['status']

                statuses.append(dependency_task_status)

        return statuses

    def update_status(self, task_instance_id, new_status):
        client = bigquery.Client(project=self.gcp_env.project)
        if new_status in ('SUCCESS', 'FAILED'):
            end_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            update_status_query = client.query(f"""
                UPDATE `{self.task_instance_table}`
                SET 
                    status = '{new_status}',
                    end_datetime = TIMESTAMP('{end_datetime}')
                WHERE task_instance_id = '{task_instance_id}'
            """)
        elif new_status == 'RUNNING':
            start_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            update_status_query = client.query(f"""
                UPDATE `{self.task_instance_table}`
                SET 
                    status = '{new_status}',
                    start_datetime = TIMESTAMP('{start_datetime}')
                WHERE task_instance_id = '{task_instance_id}'
            """)
        else:
            update_status_query = client.query(f"""
                UPDATE `{self.task_instance_table}`
                SET 
                    status = '{new_status}'
                WHERE task_instance_id = '{task_instance_id}'
            """)

        update_status_query.result()

    def run(self):
        """
        :returns: JSONResponse
        """
        # Notes:
        #   self.payload: The request POST payload is stored in this property.
        #   self.gcp_env: The app context helper class is stored in this property.

        # Ensure we are pointed at PDR dev environment if running locally.
        self.gcp_env.override_project('aou-ehr-ops-curation-test')

        dependency_statuses = self.get_dependency_statuses()

        ready = False
        timeout_time = datetime.now() + timedelta(seconds=self.timeout)

        while not ready:
            sleep(5)
            if not all([s == 'SUCCESS' for s in dependency_statuses]):
                if any([
                        s in ('FAILED', 'UPSTREAM FAILED')
                        for s in dependency_statuses
                ]):

                    self.update_status(self.task_instance_id,
                                       'UPSTREAM FAILED')

                    _logger.error(f'Upstream failure stopped task')
                    raise HTTPException(
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                        detail=
                        f'Cron task {self.gcp_env.project}.{self.task_id} has not run due to upstream failure.'
                    )

                dependency_statuses = self.get_dependency_statuses()

                if timeout_time < datetime.now():
                    _logger.error(
                        f'Task timed out after {self.timeout} seconds while dependencies were executing.'
                    )
                    self.update_status(self.task_instance_id, 'FAILED')
                    return JSONResponse(
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                        content=
                        f'Task {self.gcp_env.project}.{self.task_id} has failed due to timeout.'
                    )

            else:
                ready = True

        _logger.info(f'Running task')
        self.update_status(self.task_instance_id, 'RUNNING')
        try:
            run_response = self._run()
            self.update_status(self.task_instance_id, 'SUCCESS')
            _logger.info(f'Task completed.')
        except Exception as _:

            _logger.error(traceback.format_exc())

            self.update_status(self.task_instance_id, 'FAILED')
            return JSONResponse(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                content=
                f'Task {self.gcp_env.project}.{self.task_id} has failed.')

        return run_response