import datetime
import logging
import os
import subprocess
import shutil
import time

import pandas as pd
from typing import List

from lib.metrics import write_metric, Unit
from job.helpers import chunk_name
from lib.events import Event
from job.steps import warehouse
from sites.site import get_bucket_name, Site

PATH = "/downloads"
THRESHOLD = 100000


def download_chunk(site: Site, dt: datetime.datetime):
    """
    Download the files for the date and hour of the given dt
    Returns an iterator of the downloaded filenames
    """
    if not os.path.isdir(PATH):
        os.makedirs(PATH)

    s3_path = f"s3://{get_bucket_name(site)}/enriched/good/{chunk_name(dt)}"
    s3_sync_cmd = f"aws s3 sync {s3_path} {PATH}".split(" ")
    logging.info(" ".join(s3_sync_cmd))
    subprocess.call(
        s3_sync_cmd,
        stdout=subprocess.DEVNULL,
    )

    for _, _, files in os.walk(PATH):
        return files


def aggregate_page_pings(df: pd.DataFrame):
    """
    Aggregate page pings before uploading to cut down on
    storage/upload time
    """
    pings = df[df["event_name"] == Event.PAGE_PING.value]
    grouped_df = (
        pings.groupby(["client_id", "landing_page_path", "session_date"])
        .agg({"event_name": "count", "activity_time": "min"})
        .reset_index()
        .rename(columns={"event_name": "ping_count"})
    )
    grouped_df["event_name"] = Event.PAGE_PING.value
    merged_df = df[df["event_name"] != Event.PAGE_PING.value].append(grouped_df)

    merged_df["ping_count"] = merged_df["ping_count"].astype("Int64")
    return merged_df


def fetch_transform_upload_chunks(
    site: Site,
    dts: List[datetime.datetime],
) -> pd.DataFrame:
    """
    1. Fetches events data from S3
    2. Transform/filter the data to standard schema
    3. Upload to Redshift snowplow table

    start_dt: the dt to start fetching data for
    hours: the number of hours to fetch for
    """

    # In order to balance performance and memory usage,
    # data fetching is done in batches of <BATCH_SIZE> rows.
    start_ts = time.time()
    downloaded_rows = 0
    written_events = 0

    dfs = []
    for dt in dts:
        s = time.time()
        filenames = download_chunk(site, dt)
        d = time.time()
        logging.info(f"Download: {d - s}s")
        for filename in filenames:
            file_path = os.path.join(PATH, filename)
            tmp_df = pd.read_json(file_path, lines=True, compression="gzip")
            downloaded_rows += len(tmp_df)

            df = site.transform_raw_data(tmp_df)
            df = aggregate_page_pings(df)
            if df.size:
                dfs.append(df)
        l = time.time() - d
        logging.info(f"Transform: {l}s")
        shutil.rmtree(PATH)

        total_rows = sum([len(df) for df in dfs])
        if total_rows > THRESHOLD:
            # We've hit the memory limit, push it to S3
            # Then continue from where we left off
            warehouse.write_events(site, dt, pd.concat(dfs))
            written_events += total_rows
            dfs = []

    if len(dfs):
        total_rows = sum([len(df) for df in dfs])
        warehouse.write_events(site, dts[-1], pd.concat(dfs))
        written_events += total_rows

    write_metric("downloaded_rows", downloaded_rows)
    write_metric("written_events", written_events)
    latency = time.time() - start_ts
    write_metric("download_time", latency, unit=Unit.SECONDS)
