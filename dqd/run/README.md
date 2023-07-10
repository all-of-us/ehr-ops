# DQD Run Stage

This stage is responsible for running the DQD using a [script](https://ohdsi.github.io/DataQualityDashboard/articles/DataQualityDashboard.html) supplied by OHDSI.

## Prerequisites

 1. The build stage has already completed, and there is now a Docker container tagged `dq_env` in existence.
 2. A GCP service account key with BigQuery and Cloud Storage access has been downloaded locally, and is available for use.
 3. A google storage bucket named `dqd_output` has been created in the target GCP project.
 3. `config.yml.example` file is copied into a file named `config.yml`, and configuration parameters necessary for the GCP authentication are filled in.
 
 ## Contents

 **Dockerfile** - The Dockerfile used to execute the DQD. It authenticates to GCP, executes the DQD on a BigQuery instance, and uploads the resulting results.json file to a storage bucket.

**config.yml.example** - An example file for configuration that will be uploaded to docker image. It must be copied over to a new file titled `config.yml` before use, which will be populated with necessary information. **Do not commit any credentials to the repository**.

**dqd_execute.R** - Script that runs the DQD. Desired parameters such as excluded tables or number of threads can be set within the script before execution.

**results_upload.sh** - Script to upload results of the DQD run to a storage bucket with the name `dqd_output/output-{current_date}/results.json`

## Execution

1. (If not done already) Copy the contents of `config.yml.example` to a file named `config.yml`
    * Fill in the necessary parameters in the copied file.
```
cp config.yml.example config.yml
```
    
2. Execute the following command to build the Docker container, replacing the necessary arguments:
```
docker build . --build-arg SERVICE_ACCT_KEY "$(cat path/to/service_account_key.json)"
```
3. Check google storage bucket to confirm that the DQD results.json file has been uploaded.
