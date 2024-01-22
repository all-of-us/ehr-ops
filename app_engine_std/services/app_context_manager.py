#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
import logging
from typing import Any, Dict, AnyStr, List, Union

from aou_cloud.services.gcp_logging_fastapi import format_traceback
from aou_cloud.services.gcp_cloud_app_config import GAE_PROJECT
from ._app_context_async_database_mixin import AppContextAsyncDatabase
from ._app_context_database_mixin import AppContextDatabaseMixin


_logger = logging.getLogger('aou_cloud')


# Support generic JSON payloads to endpoints.
GenericJSONObject = Dict[AnyStr, Any]
GenericJSONArray = List[Any]
GenericJSONStructure = Union[GenericJSONArray, GenericJSONObject]


def get_gcp_project():
    """ Simply return GAE_PROJECT """
    return GAE_PROJECT


#
# Start App Context Objects
#
class AppEnvContextDatabase(AppContextDatabaseMixin):
    """ Non-async aware application context environment object """
    pass


class AppEnvContextAsyncDatabase(AppContextAsyncDatabase):
    """ Async away application context environment object """
    pass

#
# End App Context Objects


# Start App Context Managers
#
class AppEnvContextManager:
    """ Non-async aware application environment context manager """
    def __init__(self, project: str='localhost'):

        self._project = project
        self._env_config_obj = None

    def __enter__(self):
        """ Return object with properties set to config values """
        self._env_config_obj = AppEnvContextDatabase({'project': self._project}, None)
        return self._env_config_obj

    def __exit__(self, exc_type, exc_val, exc_tb):
        """ Clean up or close everything we need to """
        self._env_config_obj.cleanup()

        if exc_type is not None:
            _logger.error(format_traceback(exc_val))

class AppEnvAsyncContextManager:
    """ Async aware application environment context manager """
    def __init__(self, project: str='localhost'):

        self._project = project
        self._env_config_obj = None

    async def __aenter__(self):
        """ Return object with properties set to config values """
        self._env_config_obj = AppEnvContextAsyncDatabase({'project': self._project}, None)
        return self._env_config_obj

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """ Clean up or close everything we need to """
        # To call the cleanup function correctly, we must run it as a co-routine.
        await self._env_config_obj.cleanup()

        if exc_type is not None:
            _logger.error(format_traceback(exc_val))
#
# End App Context Managers