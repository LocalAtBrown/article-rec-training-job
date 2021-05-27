import boto3
import logging

from typing import List

RESOURCE = boto3.resource("s3")
CLIENT = boto3.client("s3")
ARTIFACT_BUCKET = 'lnl-monitoring-artifacts'


def list_objects(bucket: str, prefix: str) -> List[str]:
    response = CLIENT.list_objects_v2(Bucket=bucket, Prefix=prefix)
    contents = response.get("Contents", [])
    return [obj["Key"] for obj in contents]


def download_object(bucket_name, s3_object, local_file):
    logging.info(f"Fetching object {s3_object} from bucket {bucket_name}")
    bucket = RESOURCE.Bucket(bucket_name)
    with open(local_file, "wb") as data:
        bucket.download_fileobj(s3_object, data)
    logging.info(f"Finished fetching object {s3_object} from bucket {bucket_name}")


def upload_to_s3(filepath, bucket):
    filename = filepath.split('/')[-1]
    logging.info(f'Uploading {filename} to s3...')
    RESOURCE.Object(bucket, f"article-rec-training-job/{filename}").put(Body=open(filepath, 'rb'))
    logging.info(f'Successfully uploaded {filename} to s3')