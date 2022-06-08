#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
import logging
import os
import traceback
from typing import Any, Dict, AnyStr, List, Union

from ._app_context_async_database_mixin import AppContextAsyncDatabase
from ._app_context_database_mixin import AppContextDatabaseMixin


_logger = logging.getLogger('aou_cloud')


# Support generic JSON payloads to endpoints.
GenericJSONObject = Dict[AnyStr, Any]
GenericJSONArray = List[Any]
GenericJSONStructure = Union[GenericJSONArray, GenericJSONObject]


# Get project name and credentials
if os.getenv('GAE_ENV', '').startswith('standard'):
    # Production in the standard environment
    import google.auth
    GAE_CREDENTIALS, GAE_PROJECT = google.auth.default()
    GAE_VERSION_ID = os.environ.get('GAE_VERSION')
else:
    GAE_CREDENTIALS = 'local@localhost.net'
    GAE_PROJECT = 'localhost'
    GAE_VERSION_ID = 'develop'


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
            print((traceback.format_exc()))
            _logger.error("program encountered an unexpected error, quitting.")
            exit(1)

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
            print((traceback.format_exc()))
            _logger.error("program encountered an unexpected error, quitting.")
            exit(1)
#
# End App Context Managers