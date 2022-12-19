#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
#
# Main entry point for cron service micro app
#
import logging
from fastapi import Depends, FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi_utils.cbv import cbv
from fastapi_utils.inferring_router import InferringRouter
from starlette import status

from aou_cloud.services.gcp_cloud_app_config import GAE_PROJECT
from aou_cloud.services.gcp_logging import GCPLoggingHandlerEnum, enable_gcp_logging
from aou_cloud.services.gcp_logging_fastapi import begin_request_logging, end_request_logging
from aou_cloud.services.system_utils import setup_logging

from cron.app import jobs
from cron.app import tasks
from services.app_context_manager import get_gcp_project, AppEnvContextManager, GenericJSONStructure


_logger = logging.getLogger("aou_cloud")


app = FastAPI()

@app.on_event("startup")
async def app_startup():
    # Enable a GCP logging handler if we are running in GCP.
    if GAE_PROJECT == 'localhost':
        setup_logging(_logger, 'fastapi', debug=True)
    else:
        # Note: Un-comment Simple logger to get full and simple output of everything.
        # enable_gcp_logging(GCPLoggingHandlerEnum.Simple, logging.DEBUG, allow_stdout=True)
        level = logging.INFO if GAE_PROJECT == 'aou-ehr-ops-curation-prod' else logging.DEBUG
        enable_gcp_logging(GCPLoggingHandlerEnum.FastAPI, log_level=level, allow_stdout=False)

    # Stop uvicorn from sending random, empty uvicorn access log events.
    logger = logging.getLogger("uvicorn.access")
    logger.handlers.clear()
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
    """
    Make sure we see valid Google service headers in the request.
    Note: Only call 'begin_request_logging()' to send log messages to GCP if we know we won't
          be calling 'call_next()' in this middleware.
    """
    # _logger.debug(f'Request path: {request.url.path}')
    # _logger.debug('Request headers: ')
    # for k, v in request.headers.items():
    #     _logger.debug(f'  {k}: {v}\n')
    #
    # if not request.url.path.startswith('/_ah'):
    #     if (GAE_PROJECT != 'localhost') and not \
    #             (request.headers.get("X-Appengine-Cron") or request.headers.get("X-CloudScheduler")
    #             or request.headers.get("x-appengine-taskname")):
    #
    #         response = HTMLResponse(status_code=status.HTTP_401_UNAUTHORIZED, content="Request was not authorized.")
    #
    #         begin_request_logging(request)  # Activate GCP logging.
    #         _logger.error(f'Required header missing for this endpoint ({request.url.path})')
    #         end_request_logging(request, response)  # Finalize GCP logging.
    #
    #         return response

    response = await call_next(request)
    return response

# pylint: disable=unused-argument
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
        return JSONResponse(status_code=status.HTTP_200_OK, content='{ "success": "true" }')

    @router.get('/_ah/stop')
    def app_stop(self):
        return JSONResponse(status_code=status.HTTP_200_OK, content='{ "success": "true" }')

    @router.post("/run/{name}")
    def start_job(self, name: str, payload: GenericJSONStructure = None, request: Request = None):
        begin_request_logging(request)
        _logger.info(f'Received request to run cron job {name}.')
        # Find the matching job class for this request.
        job = next(filter(lambda job_cls: job_cls.job_name == name, jobs.__all__), None)
        if not job or job.job_name == 'unknown':
            _logger.error(f'Job {name} not found.')
            end_request_logging(request, HTMLResponse(status_code=status.HTTP_404_NOT_FOUND))
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Unknown request path")

        try:
            _logger.info(f'Running cron job "{job.job_name}"')
            with AppEnvContextManager(project=self.project) as gcp_env:
                resp = job(gcp_env, payload).run()
                _logger.info(f'Cron job "{job.job_name}" completed')
                end_request_logging(request, resp)  # Finalize GCP logging.
                return resp
        except Exception as e:
            _logger.error(f'An error occurred in cron job "{job.job_name}"')
            end_request_logging(request, HTMLResponse(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR))
            raise e

    @router.post("/task/{name}")
    def start_task(self, name: str, payload: GenericJSONStructure = None, request: Request = None):
        # Endpoint requests run in a different thread than the middleware, initialize endpoint logging here.
        begin_request_logging(request)  # Activate GCP logging for this thread.
        _logger.info(f'Received request to run cron task {name}.')
        # Find the matching job class for this request.
        task = next(filter(lambda task_cls: task_cls.task_name == name, tasks.__all__), None)
        if not task or task.task_name == 'unknown':
            _logger.error(f'Task {name} not found.')
            end_request_logging(request, JSONResponse(status_code=status.HTTP_404_NOT_FOUND, content="Not found"))
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Unknown request path")

        try:
            _logger.info(f'Running cron task "{task.task_name}"')
            with AppEnvContextManager(project=self.project) as gcp_env:
                resp = task(gcp_env, payload).run()
                _logger.info(f'Cron task "{task.task_name}" completed')
                end_request_logging(request, resp)  # Finalize GCP logging.
                return resp
        except Exception as e:
            _logger.error(f'An error occurred in task "{task.task_name}"')
            end_request_logging(request, HTMLResponse(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR))
            raise e


app.include_router(router, dependencies=[Depends(check_permission)])
