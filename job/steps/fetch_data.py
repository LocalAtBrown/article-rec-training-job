import datetime
import logging
import os
import random
import subprocess
import shutil
import time

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
from sites.sites import Sites

DAYS_OF_DATA = config.get("DAYS_OF_DATA")
s3 = boto3.client("s3")


def fetch_data(
    site: Site,
    experiment_dt: datetime.datetime = None,
    days: int = DAYS_OF_DATA,
) -> pd.DataFrame:
    start_ts = time.time()
    dt = experiment_dt or datetime.datetime.now()
    data_dfs = []
    path = "/downloads"
    fields = site.fields
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

                if site == Sites.PI and random.random() < 0.5:
                    continue
                file_path = os.path.join(full_path, filename)
                tmp_df = pd.read_json(file_path, lines=True, compression="gzip")

                common_fields = list(set(tmp_df.columns) & set(fields))
                try:
                    df = site.transform_raw_data(tmp_df[common_fields])

                except TypeError:
                    logging.exception(
                        f"Unexpected format. Can't transform data for {prefix}"
                    )
                    continue

                if df.size:
                    dfs_for_day.append(df)

        if dfs_for_day:
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
