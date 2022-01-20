import datetime
from collections import deque
import logging
import json
import os
import gzip
import subprocess
import shutil
import time

import pandas as pd
from typing import List, Set

from lib.metrics import write_metric, Unit
from job.helpers import chunk_name
from lib.events import Event
from job.steps import warehouse
from sites.site import get_bucket_name, Site

PATH = "/downloads"
MEM_THRESHOLD = 100000


def download_chunk(site: Site, dt: datetime.datetime):
    """
    Download the files for the date and hour of the given dt
    Returns a generator of downloaded filenames
    """
    path = os.path.join(PATH, chunk_name(dt))
    if not os.path.isdir(path):
        os.makedirs(path)

    s3_path = f"s3://{get_bucket_name(site)}/enriched/good/{chunk_name(dt)}"
    s3_sync_cmd = f"aws s3 sync {s3_path} {path}".split(" ")
    logging.info(" ".join(s3_sync_cmd))
    return subprocess.Popen(
        s3_sync_cmd,
        stdout=subprocess.DEVNULL,
    )


def fast_read(path: str, fields: Set[str]) -> pd.DataFrame:
    """
    Read the gzipped file path into a df with the given fields
    This is 2x as fast as pd.read_json(gzip=True) for some reason
    """
    with gzip.open(path, 'rt') as f:
        records = deque()
        for line in f:
            l = json.loads(line)
            try:
                records.appendleft({k: l[k] for k in fields})
            except:
                # believe it or not, this happens sometimes
                logging.warning(f"Could not parse row! Filename {path}, path {l.get('page_path_urk')}")
    return pd.DataFrame.from_records(records)


def transform_chunk(site: Site, dt: datetime.datetime) -> List[pd.DataFrame]:
    path = os.path.join(PATH, chunk_name(dt))
    filenames = None
    for _, _, files in os.walk(path):
        filenames = files
        break

    dfs = []

    for filename in filenames:
        file_path = os.path.join(path, filename)

        df = fast_read(file_path, site.fields)
        df = site.transform_raw_data(df)
        df = aggregate_page_pings(df)

        if df.size:
            dfs.append(df)

    return dfs


def aggregate_page_pings(df: pd.DataFrame):
    """
    Aggregate page pings before uploading to cut down on
    storage/upload time
    """
    pings = df[df["event_name"] == Event.PAGE_PING.value]
    grouped_df = (
        pings.groupby(["client_id", "landing_page_path", "session_date"])
        .agg({"event_name": "count", "activity_time": "first"})
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
) -> None:
    """
    1. Fetches events data from S3
    2. Transform/filter the data to standard schema
    3. Upload to Redshift snowplow table

    start_dt: the dt to start fetching data for
    dts: list of the dts to fetch for
    """

    # In order to balance performance and memory usage,
    # data fetching is done in batches of <BATCH_SIZE> rows.
    start_ts = time.time()
    downloaded_rows = 0
    written_events = 0

    dfs = []

    # Take advantage of async download_chunk
    # Launch N_CONCURRENT_DOWNLOAD processes first.
    N_CONCURRENT_DOWNLOAD = 4
    processes = {}
    for dt in dts[0:N_CONCURRENT_DOWNLOAD]:
        processes[dt] = download_chunk(site, dt)

    # in the loop, start the next download process
    # then wait for the current dt's download to finish
    # before starting the transform
    for dt in dts:
        processes[dt].wait()
        if len(processes) < len(dts):
            next_dt = dts[len(processes)]
            processes[next_dt] = download_chunk(site, next_dt)

        dfs.extend(transform_chunk(site, dt))
        # Delete the data as soon as it's used
        shutil.rmtree(os.path.join(PATH, chunk_name(dt)))

        total_rows = sum([len(df) for df in dfs])
        if total_rows > MEM_THRESHOLD:
            # We've hit the memory limit, push it to S3
            # Then continue from where we left off
            warehouse.write_events(site, dt, pd.concat(dfs))
            written_events += total_rows
            dfs = []

    if len(dfs):
        total_rows = sum([len(df) for df in dfs])
        warehouse.write_events(site, dts[-1], pd.concat(dfs))
        written_events += total_rows

    downloaded_rows += sum([len(df) for df in dfs])
    shutil.rmtree(PATH)

    write_metric("downloaded_rows", downloaded_rows)
    write_metric("written_events", written_events)
    latency = time.time() - start_ts
    write_metric("download_time", latency, unit=Unit.SECONDS)
