import functions_framework
from google.cloud import storage
import re
from zipfile import ZipFile
from zipfile import is_zipfile
import io
from pathlib import Path

OUTPUT_FOLDER = 'unzipped_payloads'


def zipextract(bucketname, zipfilename_with_path):

    storage_client = storage.Client()
    input_bucket = storage_client.get_bucket(bucketname)
    output_bucket = storage_client.get_bucket(bucketname)

    destination_blob_pathname = zipfilename_with_path

    matched_str = re.match(r"(.+)\/([\w-]+\.zip)", zipfilename_with_path)
    zipfilename = matched_str.group(2)

    print(f"Unzipping {zipfilename}")

    # filedir = matched_str.group(2)
    output_folder = f"{OUTPUT_FOLDER}/{Path(zipfilename).stem}"

    print("Downloading zip file")
    blob = input_bucket.blob(destination_blob_pathname)
    zipbytes = io.BytesIO(blob.download_as_string())

    print("Uploading gzip files")

    existing_files = list(output_bucket.list_blobs(prefix=output_folder))
    existing_files = [Path(f.name).name for f in existing_files]

    upload_count = 0
    if is_zipfile(zipbytes):
        with ZipFile(zipbytes, 'r') as myzip:

            for contentfilename in list(myzip.namelist()):
                if Path(contentfilename).name not in existing_files:
                    contentfile = myzip.read(contentfilename)
                    blob = output_bucket.blob(
                        f"{output_folder}/{contentfilename}")
                    blob.upload_from_string(contentfile, timeout=3600)

                    upload_count += 1

    print(
        f"Done. Uploaded {upload_count} files. Results saved to {output_folder}."
    )


@functions_framework.http
def unzip_bucket_file(request):
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

    zipfile = body["zipfile"]
    bucket_name = body["bucketname"]

    zipfile_bucket_url = f"fhir_payloads/{zipfile}"

    print(f"Starting {zipfile_bucket_url} extraction.")
    zipextract(bucket_name, zipfile_bucket_url)

    return 'OK'
