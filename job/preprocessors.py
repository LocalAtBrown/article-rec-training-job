import json
import datetime
import logging
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from itertools import product

from lib.config import config, ROOT_DIR
from lib.bucket import s3_download

BUCKET_NAME = config.get("GA_DATA_BUCKET")


def fetch_latest_data() -> pd.DataFrame:
    # TODO remove hardcoded s3_object when live data is available
    latest_data_key = "ting.zhang/90_day_sessions_2020_09_10_2020_09_13.json"
    data_filepath = f"{ROOT_DIR}/tmp/data.json"
    s3_download(BUCKET_NAME, latest_data_key, data_filepath)
    with open(data_filepath) as f:
        sessions_dict = json.load(f)

    return flatten_raw_data(sessions_dict)


def flatten_raw_data(sessions_dict: dict) -> pd.DataFrame:
    rows = [
        {
            "client_id": client_id,
            "session_id": session["sessionId"],
            "device_category": session["deviceCategory"],
            "platform": session["platform"],
            "data_source": session["dataSource"],
            "session_date": session["sessionDate"],
            "activity_time": activity["activityTime"],
            "source": activity["source"],
            "medium": activity["medium"],
            "channel_grouping": activity["channelGrouping"],
            "campaign": activity["campaign"],
            "keyword": activity["keyword"],
            "hostname": activity["hostname"],
            "landing_page_path": activity["landingPagePath"],
            "activity_type": activity["activityType"],
            **get_type_specific_fields(activity),
        }
        for client_id, sessions in sessions_dict.items()
        for session in sessions
        for activity in session["activities"]
    ]

    return pd.DataFrame(rows)


def get_type_specific_fields(activity: dict) -> dict:
    if activity["activityType"] == "EVENT":
        return {
            "event_category": activity["event"]["eventCategory"],
            "event_action": activity["event"]["eventAction"],
            "page_path": activity["landingPagePath"],
        }
    elif activity["activityType"] == "PAGEVIEW":
        return {
            "event_category": "pageview",
            "event_action": "pageview",
            "page_path": activity["pageview"]["pagePath"],
        }
    else:
        logging.info(f"Couldn't find activity field for type: {activity['activityType']}")
        return {
            "event_category": None,
            "event_action": None,
            "page_path": None,
        }


def fix_dtypes(activity_df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans event and datetime columns of DataFrame of collected Google Analytics activities

        activity_df: DataFrame of activities collected from Google Analytics using job.py

        returns: DataFrame with formatted datetimes and clean event columns
    """
    clean_df = activity_df.copy()
    clean_df['event_category'] = activity_df['event_category'].fillna('pageview')
    clean_df['event_action'] = activity_df['event_action'].fillna('pageview')
    clean_df['session_date'] = pd.to_datetime(clean_df['session_date'])
    clean_df['activity_time'] = pd.to_datetime(clean_df['activity_time'])

    return clean_df


def time_activities(activity_df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute and add dwell times to activity DataFrame

        activity_df: Cleaned DataFrame of activities

        returns: DataFrame of activities with associated dwell times
    """
    sorted_df = activity_df.copy().sort_values(by=['client_id', 'activity_time'])
    sorted_df['activity_time'] = pd.to_datetime(sorted_df['activity_time'])

    # Compute dwell time for each activity (diff with row before and flip the sign)
    sorted_df['time_spent'] = pd.to_numeric(sorted_df['activity_time'].diff(-1) * -1)

    # Drop the last activity from each client
    client_bounds = ~sorted_df['client_id'].eq(sorted_df['client_id'].shift(-1))
    sorted_df.loc[client_bounds, 'time_spent'] = np.nan
    sorted_df = sorted_df[~sorted_df.time_spent.isna()]

    return sorted_df


def label_activities(activity_df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute and add dwell and conversion times to activity DataFrame

        activity_df: Cleaned DataFrame of activities

        returns: DataFrame of activities with associated next conversion times
    """
    labeled_df = activity_df.copy()

    # Add time of next conversion event to each activity preceding a conversion
    labeled_df.loc[:, 'conversion_date'] = pd.to_datetime(
        labeled_df['session_date']
            .where(labeled_df['event_action'] == 'conversion')
    )
    labeled_df.loc[:, 'conversion_date'] = (
        labeled_df
            .groupby('client_id')['conversion_date']
            .bfill()
    )

    # Set "converted" boolean flag to true to each activity following a conversion
    labeled_df.loc[:, 'converted'] = (
        (labeled_df.event_action == 'conversion')
            .where(labeled_df.event_action == 'conversion')
    )
    labeled_df.loc[:, 'converted'] = (
        labeled_df
            .groupby('client_id')['converted']
            .ffill()
            .fillna(0)
    )

    return labeled_df


def filter_activities(
        activity_df: pd.DataFrame,
        threshold_minutes: float = 10,
        output_figure: bool = False
    ) -> pd.DataFrame:
    """
    Filters out activities that are longer than a predetermined time

        activity_df: DataFrame of Google Analytics activities with associated dwell times
        threshold: Pre-determined optimal dwell time threshold (in minutes)

        returns: DataFrame of Google Analytics activities with invalid dwell times replaced by NaN values
    """

    filtered_df = activity_df.copy()
    filtered_df.loc[
        pd.to_timedelta(filtered_df['time_spent']).dt.total_seconds() > threshold_minutes * 60,
        'time_spent'
    ] = np.nan

    if output_figure and config.get("SAVE_FIGURES"):
        t = pd.to_timedelta(activity_df.time_spent).dt.seconds.dropna() / 60

        _ = plt.hist(t, bins=np.logspace(np.log(0.001), np.log(100.0), 100), log=True)
        _ = plt.xscale('log')
        _ = plt.axvline(x=threshold_minutes, c='red')
        plt.savefig(f'{ROOT_DIR}/../outputs/activity_time_filter.png')

    filtered_df = filtered_df[~filtered_df.time_spent.isna()]
    return filtered_df


def aggregate_time(
        activity_df: pd.DataFrame,
        date_list: [datetime.date] = [],
        start_time: datetime.datetime = None,
        end_time: datetime.datetime = None,
        external_id_col: str = 'external_id'
    ) -> pd.DataFrame:
    """
    Aggregates activities into daily per-article total dwell time.

        activity_df: DataFrame of Google Analytics activities with associated dwell times
        datetime_list: List of datetimes to forcefully include in all aggregates
        returns: DataFrame of aggregated per day dwell time statistics for each user
    """
    filtered_df = activity_df.copy()
    filtered_df = filtered_df.rename(columns={external_id_col: 'external_id'})
    if start_time is not None:
        filtered_df = filtered_df[filtered_df.activity_time >= start_time]
    if end_time is not None:
        filtered_df = filtered_df[filtered_df.activity_time < end_time]
    time_df = (
        filtered_df
            # Adding dummy rows to ensure each article ID from original activity_df is included
            .append(
            [
                {
                    'client_id': filtered_df.client_id.iloc[0],
                    'time_spent': 0.0,
                    'external_id': external_id,
                    'session_date': pd.to_datetime(datetime_obj)
                }
                for datetime_obj, external_id
                in product(date_list, activity_df[external_id_col].unique())
            ]
        )
            # Adding dummy rows to ensure each client ID from original activity_df is included
            .append(
            [
                {
                    'client_id': client_id,
                    'time_spent': 0.0,
                    'external_id': filtered_df.external_id.iloc[0],
                    'session_date': pd.to_datetime(datetime_obj)
                }
                for datetime_obj, client_id
                in product(date_list, activity_df.client_id.unique())
            ]
        )
            .groupby(['external_id', 'client_id', 'session_date'])['time_spent']
            .sum()
            .unstack(level=0)
            .sort_index()
            .fillna(0.0)
    )

    return time_df


def time_decay(
        time_df: pd.DataFrame,
        half_life: float = 10,
    ) -> pd.DataFrame:
    """
    Computes exponential decay sum of time_df observations with decay based on session_date

        time_df: DataFrame of aggregated per day dwell time statistics for each user
        half_life: Desired half life of time spent in days
    """
    article_cols = time_df.columns
    exp_time_df = time_df.reset_index()

    for client_id, group_df in exp_time_df.groupby('client_id'):

        # Apply exponential time decay
        date_delta = group_df.session_date.diff().dt.days.fillna(0)
        dwell_times = np.nan_to_num(group_df[article_cols])
        for i in range(1, dwell_times.shape[0]):
            dwell_times[i, :] += apply_decay(dwell_times[i - 1,:], date_delta.iloc[i], half_life)

        exp_time_df.loc[exp_time_df.client_id == client_id, article_cols] = dwell_times

    exp_time_df = exp_time_df.set_index(['client_id', 'session_date'])
    return exp_time_df


def apply_decay(
        values: np.array,
        date_delta: int,
        half_life: float
    ) -> float:
    """
    Computes exponential decay of value over date_delta, with a half life of half_life.
    Can be used for cumulative row-wise sums, by the principle that:

        exp(T3 - T1) = exp(T3 - T2) * exp(T2 - T1)

        value (float): value being decayed
        date_delta (int): time span in days over which decay occurs
        half_life (float): half life of decay
    """
    # Decay factor should be ln(2) / lambda, where lambda is the desired half-life in days
    decay_constant = np.log(2) / half_life
    decayed_values = values * np.exp(-date_delta * decay_constant)
    return decayed_values
