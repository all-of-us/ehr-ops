from google.cloud import storage
import argparse
from tqdm.asyncio import tqdm
from pathlib import Path
import google.auth.transport.requests
import google.oauth2.id_token

import aiohttp
import asyncio
import time

PAYLOAD_DIR = 'unzipped_payloads'


async def worker(name, queue, pbar, store, cloud_function_url, bucket_name,
                 fhir_payload_name, id_token):
    while True:
        gzip_file = await queue.get()
        error_count_msg = f" / {store['error_count']} errors" if store[
            'error_count'] > 0 else ""
        pbar_description = f"{gzip_file}{error_count_msg}"
        pbar.set_description(pbar_description)
        resp_status = await unzip_gzip_file(
            cloud_function_url,
            bucket_name,
            fhir_payload_name,
            gzip_file,
            id_token,
        )

        pbar.update(1)
        if resp_status >= 400:
            store['error_count'] += 1

        queue.task_done()


async def unzip_gzip_file(cloud_function_url, bucket_name, fhir_payload_name,
                          gzip_file, id_token):

    async with aiohttp.ClientSession() as session:
        async with session.post(
                cloud_function_url,
                json={
                    'bucketname': bucket_name,
                    'gzipfile': gzip_file,
                    'fhir_payload_name': fhir_payload_name
                },
                headers={'Authorization': f"Bearer {id_token}"}) as resp:
            return resp.status


async def main(cloud_function_url,
               bucket_name,
               fhir_payload_name,
               n_workers=2,
               only_new=True):

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    gzip_blob_pathname = f"{PAYLOAD_DIR}/{fhir_payload_name}"

    files_in_folder = list(bucket.list_blobs(prefix=gzip_blob_pathname))
    gzips_in_folder = [
        Path(f.name).name for f in files_in_folder
        if Path(f.name).suffix == '.gzip'
    ]

    if only_new:
        jsonls_in_folder = [
            Path(f.name).name for f in files_in_folder
            if Path(f.name).suffix == '.jsonl'
        ]

        gzips_in_folder = [
            fname for fname in gzips_in_folder
            if Path(fname).with_suffix('.jsonl').name not in jsonls_in_folder
        ]

    print(f"{len(gzips_in_folder)} files in folder.")

    auth_req = google.auth.transport.requests.Request()
    id_token = google.oauth2.id_token.fetch_id_token(auth_req,
                                                     cloud_function_url)

    queue = asyncio.Queue()
    for gzip_file in gzips_in_folder:
        queue.put_nowait(gzip_file)

    with tqdm(total=len(gzips_in_folder)) as pbar:
        tasks = []
        store = {'error_count': 0}
        for i in range(n_workers):
            task = asyncio.create_task(
                worker(f"worker{i}", queue, pbar, store, cloud_function_url,
                       bucket_name, fhir_payload_name, id_token))
            tasks.append(task)

        started_at = time.monotonic()
        await queue.join()

        for task in tasks:
            task.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)

    print('Done')
    print(f"{store['error_count']} errors.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Unzip CE payload files asynchronously.")

    parser.add_argument("cloud_function_url")
    parser.add_argument("bucket_name")
    parser.add_argument("fhir_payload_name")
    parser.add_argument(
        "--workers",
        '-w',
        default=2,
        type=int,
        help="Number of workers to execute Cloud Functions in parallel")
    parser.add_argument(
        "--only-new",
        default=True,
        type=bool,
        help=
        "Whether to only unzip GZIP files without existing unzipped JSONL files"
    )

    args = parser.parse_args()

    asyncio.run(
        main(args.cloud_function_url, args.bucket_name, args.fhir_payload_name,
             args.workers, args.only_new))
