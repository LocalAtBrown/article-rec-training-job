import logging
import numpy as np
import pandas as pd
import pytz

from datetime import datetime, timedelta
from job.steps.fetch_data import fetch_data, FIELDS
from job.steps.preprocess import fix_dtypes, time_activities, aggregate_time, filter_activities
from job.steps.scrape_metadata import scrape_metadata
from lib.metrics import write_metric, Unit
from sites.sites import Sites


def evaluate_module(days=1):
    """
    Evaluate module over the last "days" days.
    :param days: number of days to evaluate module over
    """

    evaluation_fields = FIELDS + [
        "unstruct_event_com_washingtoncitypaper_recommendation_flow_1",
        "contexts_io_localnewslab_model_1",
        "contexts_io_localnewslab_article_recommendation_1"
    ]
    # Only capture the last
    data_df = fetch_data(fields=evaluation_fields, days=days + 1, transformer=transform_evaluation_data)
    data_df = data_df.sort_values(by=['client_id', 'activity_time'])
    data_df['last_click_source'] = data_df.groupby(['client_id'])['last_click_source'].ffill()
    data_df['last_click_target'] = data_df.groupby(['client_id'])['last_click_target'].ffill()
    data_df = data_df[data_df.activity_time > datetime.now().astimezone(pytz.utc) - timedelta(days=days)]
    article_df = scrape_metadata(
        Sites.WCP, list(data_df.landing_page_path.unique())
    )
    data_df = data_df.join(article_df, on="landing_page_path")

    event_counts = get_event_counts(data_df)
    ctr_df = get_ctr_df(event_counts)
    dwell_time_df = get_dwell_time_df(data_df)
    logging.info("Writing CTR metrics.")
    for row in ctr_df.itertuples():
        write_metric(f"ctr_{row.model_type}", row.ctr * 100, Unit.PERCENT)
    logging.info("Writing dwell time metrics.")
    for row in dwell_time_df.itertuples():
        write_metric(f"mean_dwell_time_{row.model_type}", row.mean_dwell_time, Unit.SECONDS)


def transform_evaluation_data(df: pd.DataFrame) -> pd.DataFrame:
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
    - event_name (e.g. "recommendation_flow" for seen and click events)
    - event_action (e.g. "widget_seen" or "widget_click")
    - model_id (if "recommendation_flow" event, id of model used for recommendation)
    - model_type (if "recommendation_flow" event, type of model used for recommendation)
    - last_click_source (the model_type used to recommend this click)
    - last_click_target (the recommended article external_id that was clicked on)
    """
    transformed_df = pd.DataFrame()
    transformed_df["client_id"] = df.contexts_dev_amp_snowplow_amp_id_1.apply(
        lambda x: x[0]["ampClientId"]
    )
    transformed_df["activity_time"] = pd.to_datetime(df.collector_tstamp)
    transformed_df["session_date"] = transformed_df.activity_time.dt.date
    transformed_df["landing_page_path"] = df.page_urlpath
    transformed_df["event_name"] = df.event_name if "event_name" in df else np.nan
    transformed_df["event_action"] = df.apply(_extract_event_action, axis=1)
    transformed_df["model_id"] = df.apply(_extract_model_id, axis=1)
    transformed_df["model_type"] = df.apply(_extract_model_type, axis=1)
    if 'unstruct_event_dev_amp_snowplow_amp_page_ping_1' in df.columns:
        transformed_df["duration_seconds"] = df.unstruct_event_dev_amp_snowplow_amp_page_ping_1.apply(
            lambda x: x['totalEngagedTime'] if not type(x) == float else 0
        )
    else:
        transformed_df["duration_seconds"] = 0
    transformed_df['last_click_target'] = df.apply(_get_target, axis=1)
    transformed_df['last_click_source'] = transformed_df.apply(_get_source, axis=1)

    return transformed_df


def _get_source(row):
    return row.model_type if row.event_action == 'widget_click' else np.nan


def _get_target(row):
    if ('unstruct_event_com_washingtoncitypaper_recommendation_flow_1' in row) and \
            (type(row.unstruct_event_com_washingtoncitypaper_recommendation_flow_1) != float) and \
            (row.unstruct_event_com_washingtoncitypaper_recommendation_flow_1['step_name'] == 'widget_click'):
        return row.contexts_io_localnewslab_article_recommendation_1[0]['articleId']
    else:
        return np.nan


def _extract_model_type(row: pd.Series) -> str:
    if ('contexts_io_localnewslab_model_1' in row) and not \
            (type(row.contexts_io_localnewslab_model_1) == float):
        return row.contexts_io_localnewslab_model_1[0]['type']
    else:
        return np.nan


def _extract_model_id(row: pd.Series) -> str:
    if ('contexts_io_localnewslab_model_1' in row) and not \
            (type(row.contexts_io_localnewslab_model_1) == float):

        model = row.contexts_io_localnewslab_model_1[0]
        return model.get('id') or model.get('type')
    else:
        return np.nan


def _extract_event_action(row: pd.Series) -> str:
    if ('unstruct_event_com_washingtoncitypaper_recommendation_flow_1' in row) and not \
            (type(row.unstruct_event_com_washingtoncitypaper_recommendation_flow_1) == float):
        return row.unstruct_event_com_washingtoncitypaper_recommendation_flow_1['step_name']
    elif ('event_name' not in row):
        return np.nan
    else:
        return row.event_name


def get_event_counts(data_df):
    event_counts = (
        data_df
        [['event_name', 'event_action', 'model_type', 'model_id']]
        .fillna('')
        .value_counts()
        .reset_index()
        .rename(columns={0:'client_id'})
    )
    return event_counts


def get_ctr_df(event_counts):
    ctr_df = pd.DataFrame()
    for model_type in event_counts.model_type.unique():
        if type(model_type) == float or not model_type:
            continue
        total_seen, total_clicked, ctr = get_ctr(event_counts, model_type)
        ctr_df = ctr_df.append({
            'model_type': model_type,
            'total_clicked': total_clicked,
            'total_seen': total_seen,
            'ctr': ctr
        }, ignore_index=True)

    total_seen, total_clicked, ctr = get_ctr(event_counts)
    ctr_df = ctr_df.append({
        'model_type': model_type,
        'total_clicked': total_clicked,
        'total_seen': total_seen,
        'ctr': ctr
    }, ignore_index=True)

    return ctr_df


def get_ctr(event_counts, model_type=None):
    if model_type is not None:
        model_event_counts = event_counts[event_counts.model_type == model_type]
    else:
        model_event_counts = event_counts
    total_seen = model_event_counts[model_event_counts.event_action == 'widget_seen'].client_id.sum()
    total_clicked = model_event_counts[model_event_counts.event_action == 'widget_click'].client_id.sum()
    ctr = total_clicked / total_seen
    return total_seen, total_clicked, ctr


def get_dwell_time_df(data_df):
    data_df['event_category'] = np.nan
    dwell_time_df = pd.DataFrame()

    for model_type in data_df.model_type.unique():
        if type(model_type) == float or not model_type:
            continue

        mean_dwell_time = get_mean_dwell_time(data_df, model_type=model_type)
        dwell_time_df = dwell_time_df.append({
            'model_type': model_type,
            'mean_dwell_time': mean_dwell_time
        }, ignore_index=True)

    mean_dwell_time = get_mean_dwell_time(data_df, model_type=None)
    dwell_time_df = dwell_time_df.append({
        'model_type': 'overall',
        'mean_dwell_time': mean_dwell_time
    }, ignore_index=True)
    return dwell_time_df


def get_mean_dwell_time(data_df, model_type=None):
    clean_df = fix_dtypes(data_df)
    filtered_df = filter_activities(data_df)
    if model_type is not None:
        clicked_df = filtered_df[
            (filtered_df.last_click_target == filtered_df.external_id) &
            (filtered_df.last_click_source == model_type)
        ]
    else:
        clicked_df = filtered_df

    sorted_df = time_activities(clicked_df)
    time_df = aggregate_time(sorted_df)
    if len(time_df) > 0:
        time_series = time_df.stack().reset_index()
        # When index is reset, column name for aggregated time defaults to "0"
        mean_dwell_time = time_series[0][time_series[0] > 0].mean()
    else:
        mean_dwell_time = 0.0
    return mean_dwell_time


