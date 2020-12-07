import logging
from os import path

import boto3


S3 = boto3.resource("s3")


def s3_download(bucket_name, s3_object, local_file):
    # speed up local development
    if path.exists(local_file):
        return

    logging.info(f"Fetching object {s3_object} from bucket {bucket_name}")
    bucket = S3.Bucket(bucket_name)
    with open(local_file, "wb") as data:
        bucket.download_fileobj(s3_object, data)
    logging.info(f"Finished fetching object {s3_object} from bucket {bucket_name}")
