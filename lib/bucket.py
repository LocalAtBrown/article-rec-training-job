import boto3
import logging
import numpy as np
import pandas as pd

from functools import wraps
from typing import List

from job.steps.train_model import ImplicitMF
from lib.config import ROOT_DIR

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


def save_outputs(filename):
    def save_outputs_decorator(func):
        @wraps(func)
        def save_outputs_wrapper(*args, **kwargs):
            filepath = f"{ROOT_DIR}/tmp/{filename}"
            result = func(*args, **kwargs)
            if type(result) == np.array:
                np.save(filepath, result.item_vectors)
                upload_to_s3(filepath, bucket=ARTIFACT_BUCKET)
            elif type(result) == pd.DataFrame:
                result.to_csv(filepath)
                upload_to_s3(filepath, bucket=ARTIFACT_BUCKET)
            elif type(result) == ImplicitMF:
                np.save(filepath, result.item_vectors)
                upload_to_s3(filepath, bucket=ARTIFACT_BUCKET)
            else:
                raise NotImplementedError
            return result
        return save_outputs_wrapper
    return save_outputs_decorator


def upload_to_s3(filepath, bucket):
    filename = filepath.split('/')[-1]
    logging.info(f'Uploading {filename} to s3...')
    RESOURCE.Object(bucket, f"article-rec-training-job/{filename}").put(Body=open(filepath, 'rb'))
    logging.info(f'Successfully uploaded {filename} to s3')