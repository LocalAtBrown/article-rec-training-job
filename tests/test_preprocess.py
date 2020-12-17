import pytest
import warnings

from job.preprocessors import *
from lib.config import ROOT_DIR


@pytest.fixture(scope='module')
def activity_df():
    return pd.read_csv(f"{ROOT_DIR}/tests/data/activities.csv")


def _test_fix_dtypes(df):
    clean_df = fix_dtypes(df)
    assert all(~clean_df.event_category.isna())
    assert all(~clean_df.event_action.isna())
    assert all((clean_df.activity_type == 'PAGEVIEW') == (clean_df.event_category == 'pageview'))
    assert all((clean_df.activity_type == 'PAGEVIEW') == (clean_df.event_action == 'pageview'))
    assert clean_df.session_date.dtype == '<M8[ns]'
    assert clean_df.activity_time.dtype == 'datetime64[ns, UTC]'
    assert all(clean_df.page_title.isna() == (clean_df.landing_page_path == '(not set)'))
    return clean_df


def _test_time_activities(clean_df):
    sorted_df = time_activities(clean_df)
    client_first_visit = clean_df.groupby('client_id').activity_time.min()
    client_last_visit = clean_df.groupby('client_id').activity_time.max()
    client_time_spent = client_last_visit - client_first_visit
    total_time_spent = client_time_spent.sum()
    assert pd.to_timedelta(sorted_df.time_spent.sum()) == total_time_spent
    return sorted_df


def _test_label_activities(sorted_df):
    labeled_df = label_activities(sorted_df)
    converted = set(sorted_df[sorted_df.event_action == 'conversion'].client_id.unique())
    assert set(labeled_df[labeled_df.converted == 1.0].client_id.unique()) == converted
    assert set(labeled_df[~labeled_df.conversion_date.isna()].client_id.unique()) == converted
    return labeled_df


def _test_filter_activities(labeled_df):
    times_spent_minutes = (
        pd.to_timedelta(
            labeled_df[~labeled_df.time_spent.isna()]
            .time_spent
        ).dt.total_seconds() / 60
    )
    for threshold_minutes in times_spent_minutes.describe().loc[['25%', '50%', '75%']]:
        filtered_df = filter_activities(labeled_df, threshold_minutes=threshold_minutes)
        # Since cutoff is a pandas quartile, it should exactly correspond to a value in the DataFrame
        assert pd.to_timedelta(filtered_df.time_spent.max()).total_seconds() / 60 <= threshold_minutes

    filtered_df = filter_activities(labeled_df)
    return filtered_df


def _test_aggregate_time(filtered_df):
    EXPERIMENT_DATE = pd.to_datetime(datetime.datetime.today())
    time_df = aggregate_time(filtered_df, date_list=[EXPERIMENT_DATE], external_id_col='page_title')
    assert all(filtered_df.groupby('page_title').time_spent.sum() == time_df.sum())
    assert all([(client_id, EXPERIMENT_DATE) in time_df.index for client_id in filtered_df.client_id.unique()])
    return time_df


def _test_time_decay(time_df):
    # TODO: Test that time from other users isn't leaking
    # If a reader registers time on an article, should register at least some decayed time. And vice versa.
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
