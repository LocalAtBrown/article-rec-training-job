import pytest

import pandas as pd

from sites.washington_city_paper import transform_raw_data
from lib.config import ROOT_DIR


@pytest.fixture(scope="module")
def snowplow_df():
    df = pd.read_csv(f"{ROOT_DIR}/tests/data/snowplow_activities.csv")
    # Read as Python dict (expected format)
    df[
        "contexts_dev_amp_snowplow_amp_id_1"
    ] = df.contexts_dev_amp_snowplow_amp_id_1.apply(eval)
    return df


def test_transform_raw_data(snowplow_df):
    df = transform_raw_data(snowplow_df)
    assert (df.session_date == pd.to_datetime("2021-02-11")).all()
    assert set(df.columns) == {
        "activity_time",
        "session_date",
        "client_id",
        "landing_page_path",
        "event_name",
    }
