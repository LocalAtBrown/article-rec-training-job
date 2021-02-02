import logging
from os import path

import boto3

from lib.config import config


S3 = boto3.resource("s3")


def s3_download(bucket_name, s3_object, local_file):
    logging.info(f"Fetching object {s3_object} from bucket {bucket_name}")
    # speed up local development
    if config.get("NO_REDOWNLOAD") and path.exists(local_file):
        logging.info(f"Skipped fetching object {s3_object} from bucket {bucket_name}")
        return

    bucket = S3.Bucket(bucket_name)
    with open(local_file, "w+") as data:
        bucket.download_fileobj(s3_object, data)
    logging.info(f"Finished fetching object {s3_object} from bucket {bucket_name}")
