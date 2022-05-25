#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
#
# Main entry point for cron service micro app
#
import logging
from fastapi import Depends, FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from fastapi_utils.cbv import cbv
from fastapi_utils.inferring_router import InferringRouter
from starlette import status

from aou_cloud.services.gcp_cloud_app_config import GAE_PROJECT
from aou_cloud.services.gcp_logging import GCPLoggingHandlerEnum, enable_gcp_logging
from aou_cloud.services.gcp_logging_fastapi import begin_request_logging, end_request_logging, flush_request_logs

from cron.app import jobs
from cron.app import tasks
from services.app_context_manager import get_gcp_project, AppEnvContextManager, GenericJSONStructure


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
async def check_cron_header(request: Request, call_next):
    """ Make sure we see valid Google service headers in the request. """
    if GAE_PROJECT != 'localhost':
        begin_request_logging(request)  # Activate GCP logging

    _logger.debug(f'Current project: {GAE_PROJECT}')
    # _logger.debug(f'Request path: {request.url.path}')
    # _logger.debug('Request headers: ')
    # for k, v in request.headers.items():
    #     _logger.debug(f'  {k}: {v}\n')
    #
    _logger.info('Validating request headers')
    if not request.url.path.startswith('/_ah'):
        if (GAE_PROJECT != 'localhost') and not \
                (request.headers.get("X-Appengine-Cron") or request.headers.get("X-CloudScheduler")
                or request.headers.get("x-appengine-taskname")):
            _logger.error("Required header missing for this endpoint.")
            # Finalize GCP logging and send any pending log events to GCP.
            if GAE_PROJECT != 'localhost':
                end_request_logging(request, None)
            return JSONResponse(status_code=status.HTTP_401_UNAUTHORIZED, content="Request was not authorized.")

    # End-point requests happen in a different thread, we need to flush the middleware logs here.
    flush_request_logs()
    response = await call_next(request)

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

    @router.post("/run/{name}")
    def start_job(self, name: str, payload: GenericJSONStructure = None, request: Request = None):
        # Endpoint requests run in a different thread than the middleware, initialize endpoint logging here.
        if GAE_PROJECT != 'localhost':
            begin_request_logging(request)

        _logger.info(f'Received request to run cron job {name}.')
        if name != 'unknown':  # Exclude the job base class 'unknown' job name from being matched.
            for job in jobs.__all__:
                if job.job_name == name:
                    _logger.info(f'Running cron job "{job.job_name}"')
                    with AppEnvContextManager(project=self.project) as gcp_env:
                        resp = job(gcp_env, payload).run()
                        # Finalized endpoint logging.
                        if GAE_PROJECT != 'localhost':
                            end_request_logging(request, resp)
                        return resp

        # The middleware will log this event.
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Unknown request path.")

    @router.post("/task/{name}")
    def start_task(self, name: str, payload: GenericJSONStructure = None, request: Request = None):
        # Endpoint requests run in a different thread than the middleware, initialize endpoint logging here.
        if GAE_PROJECT != 'localhost':
            begin_request_logging(request)

        _logger.info(f'Received request to run cron task {name}.')
        if name != 'unknown':  # Exclude the task base class 'unknown' task name from being matched.
            for task in tasks.__all__:
                if task.task_name == name:
                    _logger.info(f'Running cron task "{task.task_name}"')
                    with AppEnvContextManager(project=self.project) as gcp_env:
                        resp = task(gcp_env, payload).run()
                        # Finalized endpoint logging.
                        if GAE_PROJECT != 'localhost':
                            end_request_logging(request, resp)
                        return resp

        # The middleware will log this event.
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Unknown request path.")

app.include_router(router, dependencies=[Depends(check_permission)])
