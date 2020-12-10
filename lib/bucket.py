import logging
from os import path
import time

import boto3

from lib.config import config
from lib.metrics import metrics


S3 = boto3.resource("s3")


def s3_download(bucket_name, s3_object, local_file):
    logging.info(f"Fetching object {s3_object} from bucket {bucket_name}")
    # speed up local development
    if config.get("NO_REDOWNLOAD") and path.exists(local_file):
        logging.info(f"Skipped fetching object {s3_object} from bucket {bucket_name}")
        return

    bucket = S3.Bucket(bucket_name)
    start = time.time()
    with open(local_file, "wb") as data:
        bucket.download_fileobj(s3_object, data)
    download_timing = int((time.time() - start) * 1000)
    metrics.timing(
        "download_time_ms", download_timing, tags={"s3_object": s3_object, "bucket": bucket_name}
    )
    logging.info(f"Finished fetching object {s3_object} from bucket {bucket_name}")
