#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
import logging
from services.app_context_manager import GenericJSONStructure

from services.app_context_base import AppEnvContextBase
from fastapi import HTTPException
from starlette import status

from aou_cloud.services.system_utils import JSONObject


_logger = logging.getLogger('aou_cloud')


class BaseCronJob:
    """ Base cron job class """
    # Name is an all lower case url friendly name for the job and should be unique.
    job_name: str = 'unknown'
    gcp_env: AppEnvContextBase = None
    payload: JSONObject = None

    def __init__(self, gcp_env, payload: GenericJSONStructure=None):
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
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail=f'Job {self.gcp_env.project}.{self.job_name} has failed, invalid configuration.')
