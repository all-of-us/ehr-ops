import functions_framework
from google.cloud import storage
import re

# @functions_framework.http
def unzip_bucket_file():
    # body = request.json



    matched_str = re.match(r"gs:\/\/(\w+)\/(.+)\/[\w-]+\.zip", zipfile_bucket_url)
    bucket_name = matched_str.group(1)
    filedir = matched_str.group(2)

    print(bucket_name)
    client = storage.Client()
    print('getting bucket')
    bucket = client.get_bucket(bucket_name)

    print('listing blobs')
    blobs = bucket.list_blobs(prefix=filedir)

    print('Iterating')
    blob_list = []
    for blob in blobs:
        blob_list.append(blob.name)


    return blob_list

print(unzip_bucket_file())