import logging
from os import path
from typing import List

import boto3

from lib.config import config


RESOURCE = boto3.resource("s3")
CLIENT = boto3.client("s3")


def list_objects(bucket: str, prefix: str) -> List[str]:
    response = CLIENT.list_objects_v2(Bucket=bucket, Prefix=prefix)
    return [obj["Key"] for obj in response["Contents"]]


def download_object(bucket_name, s3_object, local_file):
    logging.info(f"Fetching object {s3_object} from bucket {bucket_name}")
    # speed up local development
    if config.get("NO_REDOWNLOAD") and path.exists(local_file):
        logging.info(f"Skipped fetching object {s3_object} from bucket {bucket_name}")
        return

    bucket = RESOURCE.Bucket(bucket_name)
    with open(local_file, "wb") as data:
        bucket.download_fileobj(s3_object, data)
    logging.info(f"Finished fetching object {s3_object} from bucket {bucket_name}")
