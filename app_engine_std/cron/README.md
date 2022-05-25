## PDR Cron Service App

This micro app is for handling all app-engine cron tasks. There is one generic API endpoint named
'/run/'.  Any text after the '/run/' path is used to identify a cron job.  IE: `/run/name-of-job`.

The Cron API service endpoint takes no arguments or payloads.  Once the endpoint has been called,
the 'CronAPIRouter.start_job()' method will be called, which will loop through known the 'jobs'
module class list (See: `cron/jobs/__init__.py`).  When creating a new job, the job class should
derive from the `cron.jobs._base_job.BaseCronJob` base class.

In the new Job class, the class property 'job_name' should be overridden with the job name that 
the API endpoint will match. Job matching is handled automatically by looping through the Job classes
and matching the 'job_name' value.  

Note: Remember to add any new job classes to the `__all__` variable in `cron/jobs/__init__.py` file.

### Jobs

#### QC Report

Publish PDR QC query results to a Google sheet.
