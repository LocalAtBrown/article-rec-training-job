import random
import json
import datetime
import logging
import time
import os
from typing import List, Callable

import pandas as pd
import boto3

from lib.config import config, ROOT_DIR
from lib.bucket import list_objects
from lib.metrics import write_metric, Unit

BUCKET = config.get("GA_DATA_BUCKET")
DAYS_OF_DATA = 3
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

    # only keep the first and last event for each client pageview
    transformed_df = transformed_df.sort_values(by="activity_time")
    first_df = transformed_df.drop_duplicates(
        subset=["client_id", "landing_page_path"], keep="first"
    )
    last_df = transformed_df.drop_duplicates(
        subset=["client_id", "landing_page_path"], keep="last"
    )

    return pd.concat([first_df, last_df]).drop_duplicates()


def fetch_data(
    days: int = DAYS_OF_DATA,
    fields: List[str] = FIELDS,
    transformer: Callable = transform_raw_data,
) -> pd.DataFrame:
    start_ts = time.time()
    dt = datetime.datetime.now()
    data_dfs = []

    for _ in range(days):
        month = pad_date(dt.month)
        day = pad_date(dt.day)
        prefix = f"enriched/good/{dt.year}/{month}/{day}"
        obj_keys = list_objects(BUCKET, prefix)
        for object_key in obj_keys:
            local_filename = "tmp.json"
            event_stream = s3_select(BUCKET, object_key, fields)

            try:
                event_stream_to_file(event_stream, local_filename)
                df = pd.read_json(local_filename, lines=True)
                df = transformer(df)
                data_dfs.append(df)
                os.remove(local_filename)
            except ValueError:
                logging.warning(f"{object_key} incorrectly formatted, ignored.")
                continue

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
