from google.cloud import storage
import argparse
import requests
from tqdm import tqdm
from pathlib import Path
import google.auth.transport.requests
import google.oauth2.id_token

PAYLOAD_DIR = 'unzipped_payloads'


def main(cloud_function_url, bucket_name, fhir_payload_name):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    gzip_blob_pathname = f"{PAYLOAD_DIR}/{fhir_payload_name}"

    gzips_in_folder = list(bucket.list_blobs(prefix=gzip_blob_pathname))
    gzips_in_folder = [
        Path(f.name).name for f in gzips_in_folder
        if Path(f.name).suffix == '.gzip'
    ]

    print(f"{len(gzips_in_folder)} files in folder.")

    auth_req = google.auth.transport.requests.Request()
    id_token = google.oauth2.id_token.fetch_id_token(auth_req,
                                                     cloud_function_url)

    pbar = tqdm(gzips_in_folder)
    for gzip_file in pbar:
        pbar.set_description(gzip_file)

        resp = requests.post(cloud_function_url,
                             json={
                                 'gzipfile': gzip_file,
                                 'fhir_payload_name': fhir_payload_name
                             },
                             headers={'Authorization': f"Bearer {id_token}"})

        resp.raise_for_status()

    print('Done')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Unzip CE payload files asynchronously.")

    parser.add_argument("cloud_function_url")
    parser.add_argument("bucket_name")
    parser.add_argument("fhir_payload_name")

    args = parser.parse_args()

    main(args.cloud_function_url, args.bucket_name, args.fhir_payload_name)
