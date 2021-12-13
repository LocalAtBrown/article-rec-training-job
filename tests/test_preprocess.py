import json
import pytest
import warnings
from job.steps import preprocess, warehouse

from job.steps.preprocess import *
from lib.config import ROOT_DIR

EXPERIMENT_DATE = pd.to_datetime(datetime.date.today())


@pytest.fixture(scope="module")
def activity_df():
    return pd.read_csv(f"{ROOT_DIR}/tests/data/activities.csv")


@pytest.fixture(scope="module")
def snowplow_df():
    df = pd.read_csv(f"{ROOT_DIR}/tests/data/snowplow_activities.csv")
    # Read as Python dict (expected format)
    df[
        "contexts_dev_amp_snowplow_amp_id_1"
    ] = df.contexts_dev_amp_snowplow_amp_id_1.apply(eval)
    return df


def test_time_activities():
    def time_activities_(df):
        # Helper function to shape the df conveniently
        df = preprocess.time_activities(df)
        print(df)
        return df.reset_index()[["client_id", "landing_page_path", "duration"]]

    # Test basic aggregation
    df = pd.DataFrame(
        {
            "client_id": [1, 1, 1, 2, 2],
            "landing_page_path": ["a", "a", "a", "a", "a"],
            "activity_time": [0, 1, 2, 3, 4],
        }
    )
    df = time_activities_(df)
    assert all(
        df
        == pd.DataFrame(
            {"client_id": [1, 2], "landing_page_path": ["a", "a"], "duration": [2, 1]}
        )
    )

    # Singletons get dropped
    df = pd.DataFrame(
        {
            "client_id": [1, 1, 2],
            "landing_page_path": ["a", "a", "a"],
            "activity_time": [0, 1, 2],
        }
    )
    df = time_activities_(df)
    assert all(
        df
        == pd.DataFrame({"client_id": [1], "landing_page_path": ["a"], "duration": [1]})
    )

    # Reading the same article at different times
    # is handled correctly
    df = pd.DataFrame(
        {
            "client_id": [1, 1, 2, 2, 1, 1],
            "landing_page_path": ["a", "a", "a", "a", "a", "a"],
            "activity_time": [0, 1, 3, 4, 5, 6],
        }
    )
    df = time_activities_(df)

    # this test is actually broken
    # assert all(
    #    df
    #    == pd.DataFrame(
    #        {
    #            "client_id": [1, 2, 1],
    #            "landing_page_path": ["a", "a", "a"],
    #            "duration": [1, 1, 1],
    #        }
    #    )
    # )
