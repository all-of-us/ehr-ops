import functions_framework
from google.cloud import storage
import re
from zipfile import ZipFile
from zipfile import is_zipfile
import gzip
import io
from pathlib import Path


OUTPUT_FOLDER = 'unzipped_payloads'

def zipextract(input_bucketname, zipfilename_with_path, output_bucket_name):

    storage_client = storage.Client()
    input_bucket = storage_client.get_bucket(input_bucketname)
    output_bucket = storage_client.get_bucket(output_bucket_name)

    print(input_bucketname, output_bucket_name)

    destination_blob_pathname = zipfilename_with_path

    matched_str = re.match(r"(.+)\/([\w-]+\.zip)", zipfilename_with_path)
    zipfilename = matched_str.group(2)

    print(zipfilename)

    
    # filedir = matched_str.group(2)
    output_folder = f"{OUTPUT_FOLDER}/{Path(zipfilename).stem}"
    
    print("Downloading")
    blob = input_bucket.blob(destination_blob_pathname)
    zipbytes = io.BytesIO(blob.download_as_string())


    print("Uploading")
    if is_zipfile(zipbytes):
        with ZipFile(zipbytes, 'r') as myzip:
            for contentfilename in myzip.namelist():
                contentfile = myzip.read(contentfilename)
                if Path(contentfilename).suffix in (".gz", ".gzip"):
                    # contentfile_gzip = gzip.GzipFile(fileobj=contentfile)
                    contentfile_gzip = gzip.decompress(contentfile)
                    contentfilename_gzip = Path(contentfilename).with_suffix(".jsonl")
                    blob = output_bucket.blob(f"{output_folder}/{contentfilename_gzip}")
                    blob.upload_from_string(contentfile_gzip)

                else:
                    blob = output_bucket.blob(f"{output_folder}/{contentfilename}")
                    blob.upload_from_string(contentfile)



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

    # body = request.json


    zipextract(input_bucket_name, zipfile_bucket_url, output_bucket_name)