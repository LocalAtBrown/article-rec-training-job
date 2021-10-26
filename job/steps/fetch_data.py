import datetime
import logging
import time
import os
import subprocess
import shutil

import pandas as pd
import boto3
from botocore.exceptions import EventStreamError
from retrying import retry
from typing import List, Callable

from lib.config import config, ROOT_DIR
from lib.bucket import list_objects
from lib.metrics import write_metric, Unit

from job.steps.preprocess import preprocess_day
from sites.site import get_bucket_name, Site

DAYS_OF_DATA = config.get("DAYS_OF_DATA")
FIELDS = [
    "collector_tstamp",
    "page_urlpath",
    "contexts_dev_amp_snowplow_amp_id_1",
]
s3 = boto3.client("s3")


def transform_raw_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    requires a dataframe with the following fields:
    - contexts_dev_amp_snowplow_amp_id_1
    - collector_tstamp
    - page_urlpath

    returns a dataframe with the following fields:
    - client_id
    - session_date
    - activity_time
    - landing_page_path
    - event_category (conversions, newsletter sign-ups TK)
    - event_action (conversions, newsletter sign-ups TK)
    """
    df = df.dropna(subset=["contexts_dev_amp_snowplow_amp_id_1"])
    transformed_df = pd.DataFrame()
    transformed_df["client_id"] = df.contexts_dev_amp_snowplow_amp_id_1.apply(
        lambda x: x[0]["ampClientId"]
    )
    transformed_df["activity_time"] = pd.to_datetime(df.collector_tstamp)
    transformed_df["session_date"] = pd.to_datetime(
        transformed_df.activity_time.dt.date
    )
    transformed_df["landing_page_path"] = df.page_urlpath
    transformed_df["event_category"] = "snowplow_amp_page_ping"
    transformed_df["event_category"] = transformed_df["event_category"].astype(
        "category"
    )
    transformed_df["event_action"] = "impression"
    transformed_df["event_action"] = transformed_df["event_action"].astype("category")

    return transformed_df


@retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000)
def retry_s3_select(
    site: Site,
    object_key: str,
    fields: List[str],
    transformer: Callable,
) -> pd.DataFrame:
    path = "/downloads"
    if not os.path.isdir(path):
        os.makedirs(path)
    local_filename = f"{path}/tmp.json"
    event_stream = s3_select(get_bucket_name(site), object_key, fields)

    try:
        event_stream_to_file(event_stream, local_filename)
        df = pd.read_json(local_filename, lines=True)
        df = transformer(df)
        os.remove(local_filename)
    except ValueError:
        logging.exception(f"{object_key} incorrectly formatted, ignored.")
        return pd.DataFrame()
    except EventStreamError as e:
        logging.exception(f"{object_key} encountered ephemeral streaming error.")
        raise e

    return df


def fetch_data(
    site: Site,
    experiment_dt: datetime.datetime = None,
    days: int = DAYS_OF_DATA,
    fields: List[str] = FIELDS,
    transformer: Callable = transform_raw_data,
) -> pd.DataFrame:
    start_ts = time.time()
    dt = experiment_dt or datetime.datetime.now()
    data_dfs = []
    path = "/downloads"

    for _ in range(days):
        if not os.path.isdir(path):
            os.makedirs(path)

        month = pad_date(dt.month)
        day = pad_date(dt.day)
        prefix = f"enriched/good/{dt.year}/{month}/{day}"
        args = f"aws s3 sync s3://{get_bucket_name(site)}/{prefix} {path}".split(" ")
        subprocess.call(args)

        dfs_for_day = []

        for full_path, _, files in os.walk(path):

            for filename in files:
                file_path = os.path.join(full_path, filename)
                tmp_df = pd.read_json(file_path, lines=True, compression="gzip")
                common_fields = list(set(tmp_df.columns) & set(fields))
                try:
                    df = transformer(tmp_df[common_fields])
                except TypeError:
                    logging.exception(
                        f"Unexpected format. Can't transform data for {prefix}"
                    )
                    continue

                if df.size:
                    dfs_for_day.append(df)

        day_df = preprocess_day(pd.concat(dfs_for_day))
        data_dfs.append(day_df)

        shutil.rmtree(path)

        dt = dt - datetime.timedelta(days=1)

    data_df = pd.concat(data_dfs)

    write_metric("downloaded_rows", data_df.shape[0])
    latency = time.time() - start_ts
    write_metric("download_time", latency, unit=Unit.SECONDS)
    return data_df


def pad_date(date_expr: int) -> str:
    return str(date_expr).zfill(2)


def s3_select(bucket_name: str, s3_object: str, fields: List[str]):
    logging.info(f"Fetching object {s3_object} from bucket {bucket_name}")
    fields = [f"s.{field}" for field in fields]
    field_str = ", ".join(fields)
    query = f"select {field_str} from s3object s"
    r = s3.select_object_content(
        Bucket=bucket_name,
        Key=s3_object,
        ExpressionType="SQL",
        Expression=query,
        InputSerialization={
            "CompressionType": "GZIP",
            "JSON": {"Type": "LINES"},
        },
        OutputSerialization={
            "JSON": {"RecordDelimiter": "\n"},
        },
    )
    return r


def event_stream_to_file(event_stream, filename):
    with open(filename, "wb") as f:
        # Iterate over events in the event stream as they come
        for event in event_stream["Payload"]:
            # If we received a records event, write the data to a file
            if "Records" in event:
                data = event["Records"]["Payload"]
                f.write(data)
