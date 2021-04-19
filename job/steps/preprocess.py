import datetime
import logging
from itertools import product

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from progressbar import ProgressBar

from lib.config import config, ROOT_DIR


def common_preprocessing(data_df: pd.DataFrame) -> pd.DataFrame:
    """
    :param data_df: DataFrame of activities collected from Google Analytics using job.py
        * Requisite fields: "session_date" (datetime.date), "client_id" (str), "external_id" (str),
            "event_action" (str), "event_category" (str)
    :param date_list:
    :param external_id_col:
    :param half_life:
    :return:
    """
    clean_df = fix_dtypes(data_df)
    sorted_df = time_activities(clean_df)
    filtered_df = filter_activities(sorted_df)
    return filtered_df


def model_preprocessing(
    prepared_df: pd.DataFrame,
    date_list: list = [],
    external_id_col: str = "external_id",
    half_life: float = 10.0,
) -> pd.DataFrame:
    """
    Format clickstream Google Analytics data into user-item matrix for training.

    :param prepared_df: DataFrame of activities collected from Google Analytics using job.py
        * Requisite fields: "session_date" (datetime.date), "client_id" (str), "external_id" (str),
            "event_action" (str), "event_category" (str), "duration" (timedelta)
    :param date_list: List of datetimes to forcefully include in all aggregates
    :param external_id_col: Name of column being used to denote articles
    :param half_life: Desired half life of time spent in days
    :return: DataFrame with one row for each user at each date of interest, and one column for each article
    """
    time_df = aggregate_time(
        prepared_df, date_list=date_list, external_id_col=external_id_col
    )
    exp_time_df = time_decay(time_df, half_life=half_life)

    return exp_time_df


def _add_dummies(
    activity_df: pd.DataFrame,
    date_list: [datetime.date] = [],
    external_id_col: str = "external_id",
):
    """
    :param activity_df: DataFrame of Google Analytics activities with associated dwell times
    :param date_list: List of dates to forcefully include in all aggregates
    :param external_id_col: Name of column being used to denote articles

    :return: DataFrame of Google Analytics with dummy rows for each user and date of interest included
    """
    filtered_df = activity_df.copy()
    filtered_df = filtered_df.rename(columns={external_id_col: "external_id"})
    filtered_df = (
        filtered_df
        # Adding dummy rows to ensure each article ID from original activity_df is included
        .append(
            [
                {
                    "client_id": filtered_df.client_id.iloc[0],
                    "duration": pd.to_timedelta(0.0),
                    "external_id": external_id,
                    "session_date": pd.to_datetime(datetime_obj),
                }
                for datetime_obj, external_id in product(
                    date_list, activity_df[external_id_col].unique()
                )
            ]
        )
        # Adding dummy rows to ensure each client ID from original activity_df is included
        .append(
            [
                {
                    "client_id": client_id,
                    "duration": pd.to_timedelta(0.0),
                    "external_id": filtered_df.external_id.iloc[0],
                    "session_date": pd.to_datetime(datetime_obj),
                }
                for datetime_obj, client_id in product(
                    date_list, activity_df.client_id.unique()
                )
            ]
        )
    )
    return filtered_df


def fix_dtypes(activity_df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans event and datetime columns of DataFrame of collected Google Analytics activities

    :param activity_df: DataFrame of activities collected from Google Analytics using job.py
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
    Compute and add dwell times to activity DataFrame

    :param activity_df: Cleaned DataFrame of activities
        * Requisite fields: "session_date" (datetime.date), "client_id" (str), "external_id" (str), "activity_time" (datetime.datetime)

    :return: DataFrame of activities with associated dwell times
        * Requisite fields: "duration" (float), "session_date" (datetime.date), "client_id" (str), "external_id" (str)
    """
    sorted_df = activity_df.copy().sort_values(by=["client_id", "activity_time"])
    sorted_df["activity_time"] = pd.to_datetime(sorted_df["activity_time"])

    # Compute dwell time for each activity (diff with row before and flip the sign)
    sorted_df["duration"] = sorted_df["activity_time"].diff(-1) * -1

    # Drop the last activity from each client
    client_bounds = ~sorted_df["client_id"].eq(sorted_df["client_id"].shift(-1))
    sorted_df.loc[client_bounds, "duration"] = np.nan
    sorted_df = sorted_df[~sorted_df.duration.isna()]

    return sorted_df


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

    :param activity_df: DataFrame of Google Analytics activities with associated dwell times
        * Requisite fields: "duration" (float), "session_date" (datetime.date), "client_id" (str), "external_id" (str)
    :param max_duration: Pre-determined optimal activity duration threshold (in minutes)
    :param min_dwell_time: Pre-determined optimal dwell_time threshold (in minutes)
    :return: DataFrame of Google Analytics activities with invalid dwell times replaced by NaN values.
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

    filtered_df = filtered_df[~filtered_df.duration.isna()]
    filtered_df = filtered_df[filtered_df.client_id.isin(valid_clients)]
    return filtered_df


def aggregate_conversion_times(
    activity_df: pd.DataFrame,
    date_list: [datetime.date] = [],
    start_time: datetime.datetime = None,
    end_time: datetime.datetime = None,
    external_id_col: str = "external_id",
) -> pd.DataFrame:
    """
    Aggregates activities into minimum time to conversion on interactions with each article.

    :param activity_df: DataFrame of Google Analytics activities with associated dwell times
        * Requisite fields: "duration" (float), "session_date" (datetime.date), "client_id" (str), "external_id" (str),
            "activity_time" (optional datetime.datetime)
    :param date_list: List of datetimes to forcefully include in all aggregates
    :param start_time: (optional) Only consider events happening after a specific start time
    :param end_time: (optional) Only consider events happening before a specific end time
    :param external_id_col: Name of column being used to denote articles
    :return: DataFrame of aggregated per day conversion dates with one row for each user at each date of interest,
        and one column for each article.
    """
    filtered_df = _add_dummies(
        activity_df, date_list=date_list, external_id_col=external_id_col
    )
    if start_time is not None:
        filtered_df = filtered_df[filtered_df.activity_time >= start_time]
    if end_time is not None:
        filtered_df = filtered_df[filtered_df.activity_time < end_time]
    conversion_df = (
        filtered_df.groupby(["external_id", "client_id", "session_date"])[
            "time_to_conversion"
        ]
        .min()
        .unstack(level=0)
        .sort_index()
        .fillna(0.0)
    )

    return conversion_df


def aggregate_time(
    activity_df: pd.DataFrame,
    date_list: [datetime.date] = [],
    start_time: datetime.datetime = None,
    end_time: datetime.datetime = None,
    external_id_col: str = "external_id",
) -> pd.DataFrame:
    """
    Aggregates activities into daily per-article total dwell time.

    :param activity_df: DataFrame of Google Analytics activities with associated dwell times
        * Requisite fields: "duration" (float), "session_date" (datetime.date), "client_id" (str), "external_id" (str)
    :param date_list: List of dates to forcefully include in all aggregates
    :return: DataFrame of aggregated per day dwell time with one row for each user at each date of interest,
        and one column for each article.
    """
    filtered_df = _add_dummies(
        activity_df, date_list=date_list, external_id_col=external_id_col
    )
    if start_time is not None:
        filtered_df = filtered_df[filtered_df.activity_time >= start_time]
    if end_time is not None:
        filtered_df = filtered_df[filtered_df.activity_time < end_time]
    filtered_df["duration_seconds"] = filtered_df.duration.dt.total_seconds()
    time_df = (
        filtered_df.groupby(["external_id", "client_id", "session_date"])[
            "duration_seconds"
        ]
        .sum()
        .unstack(level=0)
        .sort_index()
        .fillna(0.0)
    )

    return time_df


def aggregate_pageviews(
    activity_df: pd.DataFrame,
    date_list: [datetime.date] = [],
    start_time: datetime.datetime = None,
    end_time: datetime.datetime = None,
    external_id_col: str = "external_id",
) -> pd.DataFrame:
    """
    Aggregates activities into daily per-article total dwell time.

    :param activity_df: DataFrame of Google Analytics activities with associated dwell times
        * Requisite fields: "session_date" (datetime.date), "client_id" (str), "external_id" (str)
    :param date_list: List of dates to forcefully include in all aggregates
    :return: DataFrame of aggregated pageviews with one row for each user at each date of interest,
        and one column for each article.
    """
    dummy_time_df = activity_df.copy()
    dummy_time_df["duration"] = 1.0
    time_df = aggregate_time(
        activity_df=dummy_time_df,
        date_list=date_list,
        start_time=start_time,
        end_time=end_time,
        external_id_col=external_id_col,
    )
    pageview_df = (time_df > 0).astype(int)
    return pageview_df


def time_decay(
    time_df: pd.DataFrame,
    half_life: float = 10,
) -> pd.DataFrame:
    """
    Computes exponential decay sum of time_df observations with decay based on session_date

    :param time_df: DataFrame of aggregated per day dwell time statistics for each user
    :param half_life: Desired half life of time spent in days
    :return: DataFrame with one row for each user at each date of interest, and one column for each article
    """
    article_cols = time_df.columns
    exp_time_df = time_df.reset_index()

    # Apply exponential time decay
    user_changed = ~(
        exp_time_df.client_id.eq(exp_time_df.client_id.shift(1)).fillna(False)
    )
    date_delta = exp_time_df.session_date.diff().dt.days.fillna(0)
    dwell_times = np.nan_to_num(exp_time_df[article_cols])
    bar = ProgressBar(max_value=len(exp_time_df))
    for i in range(1, dwell_times.shape[0]):
        if user_changed.iloc[i]:
            continue
        dwell_times[i, :] += apply_decay(
            dwell_times[i - 1, :], date_delta.iloc[i], half_life
        )
        bar.update(i)

    exp_time_df = pd.DataFrame(
        data=dwell_times, index=time_df.index, columns=article_cols
    )
    return exp_time_df


def apply_decay(values: np.array, date_delta: int, half_life: float) -> np.array:
    """
    Computes exponential decay of value over date_delta, with a half life of half_life.
    Can be used for cumulative row-wise sums, by the principle that:

        exp(T3 - T1) = exp(T3 - T2) * exp(T2 - T1)

    :param values: (NumPy array of floats) values being decayed
    :param date_delta: (int) time span in days over which decay occurs
    :param half_life: (float) half life of decay
    :return: (NumPy array of floats) decayed values
    """
    # Decay factor should be ln(2) / lambda, where lambda is the desired half-life in days
    decay_constant = np.log(2) / half_life
    decayed_values = values * np.exp(-date_delta * decay_constant)
    return decayed_values