## PDR App Engine Apps

PDR App Engine apps are designed as Micro Service Apps.  Meaning each app is a stand-alone 
web service with no dependencies (library, database, ...) on other apps.  If an app has a
dependency on another app, that dependency should be resolved using read/write APIs allowing 
the app to read/write required data from/to the other app.


#### Shared Code

Shared common code (kept to a minimum) should be stored in the 'services' directory.  This 
should only be used sparingly and for supporting the startup up process for each app.  The 
'services' directory is deployed with each app.


#### Python Requirements Files

The 'requirements.txt' under 'app_engine_std' should have an entry for each app, pointing to 
the 'requirements.txt' file in app directory.  When an app is deployed, only the 
'requirements.txt' in the app directory will be used during the deploy process.


#### App File Structure
The top level directory for an app may be named anything, as long as it is valid for use with Python.
Under the app directory, there must be a 'requirements.txt' file, a directory called 'app' 
and a 'main.py' file under the 'app' directory.

```
app_engine_std
      |  
      +--- Cron
             | 
             + requirements.txt
             + app
                | 
                + main.py
```

In the 'app/main.py' file, there must be an accessible python variable named 'app'.  The 'app'
variable in the main.py should be a 'FastAPI' app object.

Invoking the FastAPI is done by calling the 'uvicorn' python module. Note: the 'CONFIG_BASE' 
environment variable must be set and contain the path to the app configuration directory.  The 
configuration directory should contain the 'app_config.json' and 'db_config.json' files.

`CONFIG_BASE='.configs' python -m uvicorn cron.app.main:app`


### Applications

This is list of the apps.

#### Cron

Cron is a API service used to run python jobs on a schedule. The Cron service 
can support multiple cron jobs, see './cron/README.md' for more information.
