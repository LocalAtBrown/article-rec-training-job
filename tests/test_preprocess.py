import json
import pytest
import warnings

from job.preprocessors import *
from lib.config import ROOT_DIR

EXPERIMENT_DATE = pd.to_datetime(datetime.date.today())


@pytest.fixture(scope='module')
def activity_df():
    return pd.read_csv(f"{ROOT_DIR}/tests/data/activities.csv")

@pytest.fixture(scope='module')
def snowplow_df():
    df = pd.read_csv(f"{ROOT_DIR}/tests/data/snowplow_activities.csv")
    # Read as Python dict (expected format)
    df['contexts_dev_amp_snowplow_amp_id_1'] = df.contexts_dev_amp_snowplow_amp_id_1.apply(eval)
    return df

def _test_fix_dtypes(df):
    clean_df = fix_dtypes(df)
    assert all(~clean_df.event_category.isna())
    assert all(~clean_df.event_action.isna())
    assert all((clean_df.activity_type == 'PAGEVIEW') == (clean_df.event_category == 'pageview'))
    assert all((clean_df.activity_type == 'PAGEVIEW') == (clean_df.event_action == 'pageview'))
    assert clean_df.session_date.dtype == '<M8[ns]'
    assert clean_df.activity_time.dtype == 'datetime64[ns, UTC]'
    assert all(clean_df.external_id.isna() == (clean_df.landing_page_path == '(not set)'))
    return clean_df


def _test_time_activities(clean_df):
    sorted_df = time_activities(clean_df)
    assert sorted_df.duration.dtype == '<m8[ns]'
    client_first_visit = clean_df.groupby('client_id').activity_time.min()
    client_last_visit = clean_df.groupby('client_id').activity_time.max()
    client_duration = client_last_visit - client_first_visit
    total_duration = client_duration.sum()
    assert sorted_df.duration.sum() == total_duration
    return sorted_df


def _test_label_activities(sorted_df):
    labeled_df = label_activities(sorted_df)
    converted = set(sorted_df[sorted_df.event_action == 'newsletter signup'].client_id.unique())
    assert set(labeled_df[labeled_df.converted == 1.0].client_id.unique()) == converted
    assert set(labeled_df[~labeled_df.conversion_time.isna()].client_id.unique()) == converted
    assert set(labeled_df[~labeled_df.time_to_conversion.isna()].client_id.unique()) == converted
    conversion_events = labeled_df[labeled_df.event_action == 'newsletter signup']
    assert (conversion_events.converted == 1.0).all()
    assert (conversion_events.conversion_time == conversion_events.activity_time).all()
    assert (conversion_events.time_to_conversion == pd.to_timedelta(0.0)).all()
    regular_events = labeled_df[labeled_df.event_action != 'newsletter signup']
    assert len(regular_events.converted.unique()) == 2
    assert (regular_events.activity_time <
            regular_events.conversion_time)[~regular_events.conversion_time.isna()].all()
    assert (regular_events.time_to_conversion != pd.to_timedelta(0)).all()
    return labeled_df


def _test_filter_activities(labeled_df):
    durations = labeled_df[~labeled_df.duration.isna()].duration.dt.total_seconds() / 60
    for duration in durations.describe().loc[['25%', '50%', '75%']]:
        filtered_df = filter_activities(labeled_df, max_duration=duration, min_dwell_time=0)
        assert filtered_df.duration.max().total_seconds() / 60 <= duration
        dwell_times = filtered_df.groupby('client_id').duration.sum()
        for dwell_time in dwell_times:
            # Since cutoff is a pandas quartile, it should exactly correspond to a value in the DataFrame
            filtered_df = filter_activities(
                filtered_df,
                max_duration=duration,
                min_dwell_time=dwell_time.total_seconds() / 60
            )
            assert (filtered_df.groupby('client_id').duration.sum() >= dwell_time).all()

    filtered_df = filter_activities(labeled_df)
    return filtered_df


def _test_aggregate_conversion_times(filtered_df):
    conversion_df = aggregate_conversion_times(filtered_df, date_list=[EXPERIMENT_DATE], external_id_col='article_id')
    assert all(filtered_df.groupby('article_id').duration.sum() == conversion_df.sum())
    assert all([(client_id, EXPERIMENT_DATE) in conversion_df.index for client_id in filtered_df.client_id.unique()])
    return conversion_df


def _test_aggregate_pageviews(filtered_df):
    pageview_df = aggregate_pageviews(filtered_df, date_list=[EXPERIMENT_DATE], external_id_col='article_id')
    assert all(filtered_df.groupby('article_id').count() == pageview_df.sum())
    assert all([(client_id, EXPERIMENT_DATE) in pageview_df.index for client_id in filtered_df.client_id.unique()])
    return pageview_df


def _test_aggregate_time(filtered_df):
    time_df = aggregate_time(filtered_df, date_list=[EXPERIMENT_DATE], external_id_col='external_id')
    assert (time_df.dtypes == float).all()
    # Lower than floating point error
    assert all((
        filtered_df
        .groupby('external_id')
        .duration.sum()
        .dt.total_seconds() - time_df.sum()).abs() < 1e-12
    )
    assert all([(client_id, EXPERIMENT_DATE) in time_df.index for client_id in filtered_df.client_id.unique()])
    return time_df


def _test_time_decay(time_df):
    # A reader registers decayed time on an article if and only if reader registers some time.
    exp_time_df = time_decay(time_df, half_life=1)
    visited_articles = \
        (
            time_df
            .reset_index()
            .drop(columns='session_date')
            .groupby('client_id')
            .max() > 0
        )
    visited_decayed_articles = \
        (
            exp_time_df
            .reset_index()
            .drop(columns='session_date')
            .groupby('client_id')
            .max() > 0
        )
    assert all((visited_articles == visited_decayed_articles).all())
    # An infinite half life means there is no decay
    exp_time_df = time_decay(time_df, half_life=float('inf'))
    assert all((exp_time_df.reset_index(drop=True) ==
                time_df.reset_index().groupby(['client_id']).cumsum()).all())
    # A half life of 0 means dwell time is immediately decayed the day after
    warnings.filterwarnings("ignore", category=RuntimeWarning)
    exp_time_df = time_decay(time_df, half_life=0)
    warnings.resetwarnings()
    assert all((exp_time_df == time_df).all())

    return exp_time_df


def test_pipeline(activity_df):
    clean_df = _test_fix_dtypes(activity_df)
    sorted_df = _test_time_activities(clean_df)
    labeled_df = _test_label_activities(sorted_df)
    filtered_df = _test_filter_activities(labeled_df)
    time_df = _test_aggregate_time(filtered_df)
    exp_time_df = _test_time_decay(time_df)


def test_transform_raw_data(snowplow_df):
    df = transform_raw_data(snowplow_df)
    assert (df.session_date == pd.to_datetime('2021-02-11')).all()
    assert (df.event_category == 'snowplow_amp_page_ping').all()
    assert (df.event_action == 'impression').all()
    assert set(df.columns) == {'activity_time', 'session_date', 'client_id', 'landing_page_path', 'event_category', 'event_action'}
