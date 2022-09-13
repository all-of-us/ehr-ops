#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
import logging
import os
import sys
from urllib.parse import quote

from aou_cloud.services.gcp_cloud_tasks import GCPCloudTask
from aou_cloud.services.gcp_bigquery import BigQueryJob
from aou_cloud.services.gcp_cloud_function import CloudFunctionPubSubEventContext, CloudFunctionContextManager
from aou_cloud.services.gcp_cloud_function_storage_event import CloudFunctionStorageEventHandler
from aou_cloud.services.system_utils import setup_logging


# Function name must contain only lower case Latin letters, digits or underscore. It must
# start with letter, must not end with a hyphen, and must be at most 63 characters long.
# THERE MUST be a python entrypoint function in this file with the same name as function name.
function_name = 'example_cloud_func'

# [--trigger-bucket=TRIGGER_BUCKET | --trigger-http | --trigger-topic=TRIGGER_TOPIC |
# --trigger-event=EVENT_TYPE --trigger-resource=RESOURCE]
# NOTE: Default function timeout limit is 60s, maximum can be 540s.

_TARGET_BUCKET = 'ehr-ops-data-test'

deploy_args = [
    f'--trigger-resource={_TARGET_BUCKET}',
    '--trigger-event google.storage.object.finalize',
    '--timeout=120',
    '--memory=256'
]

_logger = logging.getLogger('aou_cloud')


class ExampleStorageEventHandler(CloudFunctionStorageEventHandler):
    """
    Load example bucket files into BigQuery.
    """
    def created(self):
        """ Handle storage object created event. """
        # We will receive all events from the bucket, we must filter out and only process the
        # the files are supposed to. Look for a file with a prefix of 'bigquery' in the 'example' folder.
        folder = f'examples/cloud_func_uploads'
        if f'{folder}/bigquery-' not in self.event.name.lower():
            _logger.info(f'Skipping file {self.event.name}, name does not match file pattern.')
            return

        dataset = 'dev_testing'
        table = 'test_bucket_upload_data'
        filename = os.path.basename(self.event.name)

        # Step 1 : Use the 'merge_into_bq_table' to truncate and replace all data.
        self.merge_into_bq_table(project_id=self.gcp_env.project, dataset=dataset, table=table,
                                 manage_core_fields=False, truncate_dest=True, field_delimiter=',',
                                 timestamp_format='%Y-%m-%d %H:%M:%E*S', skip_leading_rows=0)

        # Step 2 : Insert a file upload history record into the history table.
        sql = f"""
            insert into {dataset}.upload_history (id, created, filename, record_cnt) values (
                case when (select max(id)+1 from {dataset}.upload_history) is null then 1 else
                   (select max(id)+1 from {dataset}.upload_history) end,
                CURRENT_DATETIME,
                '{filename}',
                (select count(1) from {dataset}.{table})
            )
          """
        job = BigQueryJob(sql, dataset=dataset, project=self.gcp_env.project)
        job.start_job()

        # Step 3 - Now create a GCP Cloud Task to call the 'example-task' App Engine task endpoint to process the data.
        #          Delay task 20 secs before running, useful to ensure data has been committed/replicated and is
        #          ready.
        payload = {'filename': filename}
        task = GCPCloudTask()
        task.execute('/task/example-task', payload=payload, project_id=self.gcp_env.project, in_seconds=20,
                     queue='cron-default')


def get_deploy_args(gcp_env):  # pylint: disable=unused-argument
    """
    Return the trigger and any additional arguments for the 'gcloud functions deploy' command.
    Warning: function get_deploy_args() must come after all class definitions.
    :param gcp_env: The GCP context object.
    """
    args = [function_name]
    # args.append(deploy_args)
    # use this loop to tweak any of the arguments before deploy or add additional args. This expanded
    # code block is just for illustration.
    for arg in deploy_args:
        if gcp_env.project.endswith('prod'):
            args.append(arg)
        else:
            args.append(arg)

    return args


def example_cloud_func(event, context):
    """
    GCloud Function Entry Point (Storage Pub/Sub Event).
    https://cloud.google.com/functions/docs/concepts/events-triggers#functions_parameters-python
    :param event: (dict):  The dictionary with data specific to this type of event.
                       The `data` field contains a description of the event in
                       the Cloud Storage `object` format described here:
                       https://cloud.google.com/storage/docs/json_api/v1/objects#resource
    :param context: (google.cloud.functions.Context): Metadata of triggering event.
    """
    with CloudFunctionContextManager(function_name, None) as gcp_env:
        func = ExampleStorageEventHandler(gcp_env, event, context)
        func.run()

""" --- Main Program Call --- """
if __name__ == '__main__':
    """ Test code locally """
    setup_logging(_logger, function_name, debug=True)

    context = CloudFunctionPubSubEventContext(event_id=1669022966780817, event_type='google.storage.object.finalize')

    file = f'examples/cloud_func_uploads/bigquery-2022-05-01.csv'
    timestamp = '2022-05-01T15:20:30.1252Z'

    event = {
      "bucket": _TARGET_BUCKET,
      "contentType": "text/csv",
      "crc32c": "jXOx3g==",
      "etag": "CIapmYSb0+wCEAE=",
      "generation": "1603748044887174",
      "id": f"{_TARGET_BUCKET}/{quote(file)}/1603748044887174",
      "kind": "storage#object",
      "md5Hash": "sSsHt9P3l+WieCCevpQsjg==",
      "mediaLink": f"https://www.googleapis.com/download/storage/v1/b/{_TARGET_BUCKET}/o/{quote(file)}?generation=1603748044887174&alt=media",
      "metageneration": "1",
      "name": file,
      "selfLink": f"https://www.googleapis.com/storage/v1/b/{_TARGET_BUCKET}/o/{quote(file)}",
      "size": "1989",
      "storageClass": "STANDARD",
      "timeCreated": timestamp,
      "timeStorageClassUpdated": "2020-12-02T22:10:05.872Z",
      "updated": "2020-10-26T21:34:04.887Z"
    }

    example_cloud_func(event, context)
    sys.exit(0)
