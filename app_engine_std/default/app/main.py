#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
#
# Main entry point for cron service micro app
#
import logging
from fastapi import Depends, FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi_utils.cbv import cbv
from fastapi_utils.inferring_router import InferringRouter
from starlette import status

from aou_cloud.services.gcp_cloud_app_config import GAE_PROJECT
from aou_cloud.services.gcp_logging import GCPLoggingHandlerEnum, enable_gcp_logging
from aou_cloud.services.gcp_logging_fastapi import begin_request_logging, end_request_logging, flush_request_logs

from services.app_context_manager import get_gcp_project


_logger = logging.getLogger('aou_cloud')


app = FastAPI()

@app.on_event("startup")
async def app_startup():
    # Enable a GCP logging handler if we are running in GCP.
    if GAE_PROJECT != 'localhost':
        # Note: Un-comment Simple logger to get full and simple output of everything.
        # enable_gcp_logging(GCPLoggingHandlerEnum.Simple, logging_level, allow_stdout=True)
        enable_gcp_logging(GCPLoggingHandlerEnum.FastAPI, logging.INFO, allow_stdout=False)
#
# There are two ways to execute code for every request;
#   2a : Create middleware functions that can be chained together to validate the request.
#   2b : Create discrete dependency functions that can be added with the router.
#
# @app.middleware('http')  # Step 2a: Create middleware checks
# async def force_https(request: Request, call_next):
#     """ Ensure all requests are https in GCP environments. """
#     # if GAE_PROJECT != 'localhost' and request.scope['scheme'] == 'http':
#     #     return JSONResponse(status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
#     #                         content="All requests must be over https." )
#     response = await call_next(request)
#     return response


@app.middleware('http')  # Step 2a: Create middleware checks
async def check_authentication(request: Request, call_next):
    """ Authenticate user for this request. """
    if GAE_PROJECT != 'localhost':
        begin_request_logging(request)  # Activate GCP logging

    _logger.debug(f'Current project: {GAE_PROJECT}')
    # _logger.debug(f'Request path: {request.url.path}')
    # _logger.debug('Request headers: ')
    # for k, v in request.headers.items():
    #     _logger.debug(f'  {k}: {v}\n')
    #
    _logger.info('Authenticating user')
    if not request.url.path.startswith('/_ah'):
        # TODO: Use Google to verify user if using an IAP, otherwise assume there is an OAuth token in the header
        #       and we need to call Google with the token to get the user account.
        #       For now, always return unauthorized.
        if GAE_PROJECT != 'localhost':
            end_request_logging(request, None)
        return JSONResponse(status_code=status.HTTP_401_UNAUTHORIZED, content="Request was not authorized.")

    response = await call_next(request)
    # End-point requests happen in a different thread, we need to flush the middleware logs here.
    flush_request_logs()

    # Finalize and push any middleware logged events.
    if GAE_PROJECT != 'localhost' and response.status_code != 200:
        end_request_logging(request, response)  # Finalize middleware logging and send any pending log events to GCP.
    return response


async def check_permission(req: Request):  # Step 2b: Or create router dependency checks for each request.
    # f = req.scope['endpoint']
    # HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Unknown endpoint.")
    pass

#
# Step 1: Create a router
#
router = InferringRouter()


@cbv(router)  # Step 3: Create and decorate a class to hold the endpoints
class CronAPIRouter:

    project: str = Depends(get_gcp_project)  # Step 4a: Add class dependencies as class attributes.

    @router.get('/_ah/start')
    def app_start(self):
        _logger.info('Received Google application startup request.')
        return JSONResponse(status_code=status.HTTP_200_OK, content='{ "success": "true" }')

    @router.get('/_ah/stop')
    def app_stop(self):
        _logger.info('Received Google application shutdown request.')
        return JSONResponse(status_code=status.HTTP_200_OK, content='{ "success": "true" }')


app.include_router(router, dependencies=[Depends(check_permission)])
