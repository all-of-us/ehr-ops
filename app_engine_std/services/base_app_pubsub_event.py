#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
from fastapi import HTTPException
from starlette import status

from python_easy_json import JSONObject

from services.app_context_manager import GenericJSONStructure
from services.app_context_base import AppEnvContextBase
from services.base_app import BaseAppEndpoint


class BaseAppPubSubEvent(BaseAppEndpoint):
    """ Base pipeline event class """
    # Name is an all lower case url friendly name for the job and should be unique.
    event_name: str = 'unknown'
    gcp_env: AppEnvContextBase = None
    payload: JSONObject = None

    def __init__(self, gcp_env, payload: GenericJSONStructure=None):
        """
        :param gcp_env: AppEnvObject object.
        """
        self.gcp_env = gcp_env
        if payload:
            # Using JSONObject or derived class here will convert the binary keys and values in the dict to utf-8.
            self.payload = self._get_annot_cls('payload')(payload, cast_types=True)

    def run(self):
        """
        (Virtual Function) Entry point for pub/sub event, override in child class.
        :returns: JSONResponse
        """
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail=f'Pub/Sub event {self.gcp_env.project}.{self.event_name} \
                             has failed, invalid configuration.')
