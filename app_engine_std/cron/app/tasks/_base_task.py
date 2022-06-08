#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#

from services.app_context_manager import GenericJSONStructure

from services.app_context_base import AppEnvContextBase
from fastapi import HTTPException
from starlette import status

from aou_cloud.services.system_utils import JSONObject


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

    def run(self):
        """
        (Virtual Function) Entry point for cron task, override in child class.
        :returns: JSONResponse
        """
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail=f'Job {self.gcp_env.project}.{self.task_name} has failed, invalid configuration.')
