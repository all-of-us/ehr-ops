#
# Gunicorn Settings: https://docs.gunicorn.org/en/stable/settings.html
#
import multiprocessing
import os

project = os.getenv('GCP_PROJECT', 'localhost')
loglevel = "error" if project.endswith('-stable') or project.endswith('-prod') else "debug"

_port = 8080 # local dev/testing.
workers = 1
threads = 1

max_requests = 500
max_requests_jitter = 50
timeout = 60

if os.getenv('GAE_ENV', '').startswith('standard'):
    _port = os.environ.get('PORT', 8081)
    workers = multiprocessing.cpu_count() * 2
    threads = multiprocessing.cpu_count() * 2

bind = "0.0.0.0:{0}".format(_port)

# Do not use "gevent" for worker class, doesn't work on App Engine.
worker_class = "uvicorn.workers.UvicornWorker"
raw_env = [
    "CONFIG_PROVIDER={0}".format(os.environ.get('CONFIG_PROVIDER', None)),
    "STORAGE_PROVIDER={0}".format(os.environ.get('STORAGE_PROVIDER', None)),
]
