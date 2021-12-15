import datetime
import logging
import numpy as np
import pandas as pd

from typing import List, Tuple, Optional
from sites.site import Site
from job.helpers import decay_fn
from concurrent.futures import ThreadPoolExecutor


def preprocess_day(df: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocess a day's worth of raw Snowplow data

    :param df: Raw dataFrame of activities collected from Snowplow for one day
    :return: df with initial preprocessing steps completed
    """
    df = time_activities(df)
    return df


def extract_external_id(site: Site, path: str) -> str:
    return site.extract_external_id(path)


def extract_external_ids(site: Site, landing_page_paths: List[str]) -> pd.DataFrame:
    """
    :param data_df: DataFrame of activities collected from Snowplow.
        * Requisite fields: "landing_page_path" (str)
    :return: data_df with "external_id" column added
    """
    futures_list = []
    results = []
    good_paths = []

    with ThreadPoolExecutor(max_workers=20) as executor:
        for path in landing_page_paths:
            future = executor.submit(extract_external_id, site, path=path)
            futures_list.append((path, future))

        for (path, future) in futures_list:
            try:
                result = future.result(timeout=60)
                results.append(result)
                good_paths.append(path)
            except:
                pass

    df_data = {"landing_page_path": good_paths, "external_id": results}
    external_id_df = pd.DataFrame(df_data)
    external_id_df = external_id_df.dropna(subset=["external_id"])
    external_id_df["external_id"] = external_id_df["external_id"].astype(object)

    return external_id_df


def time_decay(
    data_df: pd.DataFrame,
    experiment_date: datetime.date,
    half_life: float,
    date_col="session_date",
    duration_col="duration",
) -> pd.DataFrame:
    """
    Applies basic exponential decay based on the difference between the "date" column
    and the current date argument to the dwell time
    """
    decay_factor = decay_fn(experiment_date, data_df[date_col], half_life)
    data_df[duration_col] *= decay_factor
    return data_df


def time_activities(activity_df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute dwell times from df of heartbeat page_ping activities
    Returns one row per page_view event, with dwell time attached.
    Note this method drops singleton rows (eg, pageviews that lasted for less than a heartbeat)

    :param activity_df: Cleaned DataFrame of activities
        * Requisite fields: "session_date" (datetime.date), "client_id" (str), "landing_page_path" (str), "activity_time" (datetime.datetime)

    :return: DataFrame of activities with associated dwell times
        * Requisite fields: "duration" (float), "session_date" (datetime.date), "client_id" (str), "landing_page_path" (str)
    """
    sorted_df = activity_df.sort_values(by=["client_id", "activity_time"])
    compare_columns = ["client_id", "landing_page_path"]
    # Assign a group ID to each group "session"
    # A group "session" is defined as a consecutive run of client_id, landing_page_path pairs
    sorted_df["group"] = (
        (sorted_df[compare_columns] != sorted_df[compare_columns].shift(1))
        .any(axis=1)
        .cumsum()
    )

    # Now, take the first and last rows from each session
    minmax_df = sorted_df.groupby("group", as_index=False).nth([0, -1]).copy()

    # Compute dwell time for each activity (diff with row before and flip the sign)
    minmax_df["duration"] = minmax_df["activity_time"].diff(-1) * -1

    # Remove the last row of each group
    return minmax_df[minmax_df.groupby("group").cumcount(ascending=False) > 0]
