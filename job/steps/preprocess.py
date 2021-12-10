import datetime
import logging
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pdb

from datetime import timezone
from itertools import product
from progressbar import ProgressBar
from typing import List, Tuple, Optional
from lib.config import config, ROOT_DIR
from lib.bucket import save_outputs
from sites.site import Site
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

def decay_fn(experiment_date:datetime.date, df_column:pd.Series, half_life:float):
    """ half life decay a column value"""
    return 0.5 ** (
        (experiment_date - df_column).dt.days / half_life
    )

def time_decay(
    data_df: pd.DataFrame, experiment_date: datetime.date, half_life: float
) -> pd.DataFrame:
    """
    Applies basic exponential decay based on the difference between the "date" column
    and the current date argument to the dwell time
    """
    decay_factor = decay_fn(experiment_date, data_df["session_date"], half_life)
    data_df["duration"] *= decay_factor
    
    return data_df

def filter_flyby_users(data_df: pd.DataFrame) -> pd.DataFrame:
    """
    :param data_df: DataFrame of activities collected from Snowplow.
        * Requisite fields: "session_date" (datetime.date), "client_id" (str),
            "event_action" (str), "event_category" (str)
    :return: data_df with flyby users removed
    """
    if "external_id" in data_df.columns:
        unique_df = data_df.drop_duplicates(["client_id", "external_id"])
    else:
        unique_df = data_df
    valid_ids = (
        unique_df.groupby("client_id").filter(lambda x: len(x) > 1).client_id.unique()
    )
    filtered_df = data_df[data_df.client_id.isin(valid_ids)]
    return filtered_df


def filter_sparse_articles(data_df: pd.DataFrame) -> pd.DataFrame:
    """
    :param data_df: DataFrame of activities collected from Snowplow
        * Requisite fields: "session_date" (datetime.date), "client_id" (str), "external_id" (str),
            "event_action" (str), "event_category" (str)
    :return: data_df with sparse articles removed
    """
    unique_df = data_df.drop_duplicates(["client_id", "external_id"])
    valid_articles = (
        unique_df.groupby("external_id")
        .filter(lambda x: len(x) > 1)
        .external_id.unique()
    )
    filtered_df = data_df[data_df.external_id.isin(valid_articles)]
    return filtered_df


def filter_articles(data_df: pd.DataFrame) -> pd.DataFrame:
    """
    :param data_df: DataFrame of activities collected from Snowplow
        * Requisite fields: "session_date" (datetime.date), "client_id" (str), "external_id" (str),
            "event_action" (str), "event_category" (str)
    :return: data_df with flyby users and sparse articles removed
    """
    filtered_df = data_df
    prev_len = 0

    # Loop until convergence
    while len(filtered_df) != prev_len:
        prev_len = len(filtered_df)
        filtered_df = filter_sparse_articles(filtered_df)
        filtered_df = filter_flyby_users(filtered_df)

    return filtered_df


def common_preprocessing(data_df: pd.DataFrame) -> pd.DataFrame:
    """
    :param data_df: DataFrame of activities collected from Snowplow
        * Requisite fields: "session_date" (datetime.date), "client_id" (str), "external_id" (str),
            "event_action" (str), "event_category" (str)
    :return:
    """
    logging.info("Preprocessing: setting datatypes...")
    clean_df = fix_dtypes(data_df)
    logging.info("Preprocessing: calculating dwell time...")
    sorted_df = time_activities(clean_df)
    logging.info("Preprocessing: filtering activities...")
    filtered_df = filter_activities(sorted_df)
    logging.info("Preprocessing: filtering flyby users and articles...")
    filtered_df = filter_articles(filtered_df)
    return filtered_df



def fix_dtypes(activity_df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans event and datetime columns of DataFrame of collected Snowplow activities

    :param activity_df: DataFrame of activities collected from Snowplow using job.py
        * Requisite fields: "session_date" (datetime.date), "client_id" (str), "external_id" (str),
            "event_action" (str), "event_category" (str)

    :return: DataFrame of activities with associated dwell times
        * Requisite fields: "session_date" (datetime.date), "client_id" (str), "external_id" (str)
            "event_action" (str), "event_category" (str)
    """
    clean_df = activity_df.copy()
    clean_df["event_category"] = activity_df["event_category"].fillna(
        "snowplow_amp_page_ping"
    )
    clean_df["event_action"] = activity_df["event_action"].fillna("impression")
    clean_df["session_date"] = pd.to_datetime(clean_df["session_date"])
    clean_df["activity_time"] = pd.to_datetime(clean_df["activity_time"])

    return clean_df


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


def label_activities(activity_df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute and add dwell and conversion times to activity DataFrame

    :param activity_df: Cleaned DataFrame of activities
        * Requisite fields:
            "duration" (float),
            "session_date" (datetime.date),
            "client_id" (str),
            "external_id" (str)

    :return: DataFrame of activities with associated next conversion times.
        * Requisite fields:
            "duration" (float),
            "session_date" (datetime.date),
            "client_id" (str),
            "external_id" (str),
            "converted" (int),
            "conversion_time" (datetime.datetime),
            "time_to_conversion" (datetime.timedelta)
    """
    labeled_df = activity_df.set_index("client_id")

    # Add time of next conversion event to each activity preceding a conversion
    labeled_df["conversion_time"] = (
        labeled_df[["activity_time"]]
        .where(labeled_df["event_action"] == "newsletter signup")
        .groupby("client_id")["activity_time"]
        .bfill()
    )

    labeled_df["time_to_conversion"] = labeled_df.conversion_time.sub(
        labeled_df.activity_time
    )
    labeled_df.loc[labeled_df.conversion_time.isna(), "time_to_conversion"] = np.nan

    # Set "converted" boolean flag to true to each activity following a conversion
    labeled_df["converted"] = (
        (labeled_df[["event_action"]] == "newsletter signup")
        .where(labeled_df.event_action == "newsletter signup")
        .groupby("client_id")["event_action"]
        .ffill()
        .fillna(0)
    )

    labeled_df = labeled_df.reset_index()
    return labeled_df


def filter_activities(
    activity_df: pd.DataFrame,
    max_duration: float = 10.0,
    min_dwell_time: float = 1.0,
    output_figure: bool = False,
) -> pd.DataFrame:
    """
    Filters out activities that are longer than a predetermined time

    :param activity_df: DataFrame of Snowplow activities with associated dwell times
        * Requisite fields: "duration" (float), "session_date" (datetime.date), "client_id" (str), "external_id" (str)
    :param max_duration: Pre-determined optimal activity duration threshold (in minutes)
    :param min_dwell_time: Pre-determined optimal dwell_time threshold (in minutes)
    :return: DataFrame of Snowplow activities with invalid dwell times replaced by NaN values.
        * Requisite fields: "duration", "session_date", "client_id", "external_id"
    """

    filtered_df = activity_df.copy()
    filtered_df.loc[
        filtered_df["duration"].dt.total_seconds() / 60 > max_duration, "duration"
    ] = np.nan

    if output_figure and config.get("SAVE_FIGURES"):
        t = pd.activity_df.duration.dt.total_seconds().dropna() / 60

        _ = plt.hist(t, bins=np.logspace(np.log(0.001), np.log(100.0), 100), log=True)
        _ = plt.xscale("log")
        _ = plt.axvline(x=max_duration, c="red")
        plt.savefig(f"{ROOT_DIR}/../outputs/activity_time_filter.png")

    dwell_times = filtered_df.groupby("client_id")["duration"].sum()
    valid_clients = (
        dwell_times[dwell_times.dt.total_seconds() / 60 >= min_dwell_time]
        .reset_index()
        .client_id.unique()
    )

    filtered_df = filtered_df[
        filtered_df.duration.notna() & (filtered_df.duration > datetime.timedelta(0))
    ]
    filtered_df = filtered_df[filtered_df.client_id.isin(valid_clients)]
    return filtered_df
