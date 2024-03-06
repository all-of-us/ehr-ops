import functions_framework
from google.cloud import storage
import gzip
from pathlib import Path
import sys

PAYLOAD_DIR = 'unzipped_payloads'


def gunzip(bucket_name, fhir_payload_name, gzipfile_name):

    if not Path(gzipfile_name).suffix in (".gz", ".gzip"):
        print("File is not a .gzip")
        sys.exit(0)

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    gzip_blob_pathname = f"{PAYLOAD_DIR}/{fhir_payload_name}/{gzipfile_name}"

    blob = bucket.blob(gzip_blob_pathname)

    gzip_bytes = blob.download_as_bytes()
    gzip_content = gzip.decompress(gzip_bytes)

    jsonl_filepath = f"{PAYLOAD_DIR}/{fhir_payload_name}/{Path(gzipfile_name).with_suffix('.jsonl')}"
    output_blob = bucket.blob(jsonl_filepath)

    output_blob.upload_from_string(gzip_content, timeout=3600)

    print(f"Done ungzipping {gzipfile_name}.")


@functions_framework.http
def ungzip_ce_file(request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
    """

    body = request.json

    bucket_name = body["bucketname"]
    fhir_payload_name = body["fhir_payload_name"]
    gzipfile = body["gzipfile"]

    print(f"Starting {fhir_payload_name}/{gzipfile} extraction.")

    gunzip(bucket_name, fhir_payload_name, gzipfile)

    return 'OK'
