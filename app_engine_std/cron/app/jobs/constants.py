from enum import Enum


class Status(Enum):
    SUCCESS = 'SUCCESS'
    FAILED = 'FAILED'
    UPSTREAM_FAILED = 'UPSTREAM FAILED'
    QUEUED = 'QUEUED'
    RUNNING = 'RUNNING'
    CANCELED = 'CANCELED'