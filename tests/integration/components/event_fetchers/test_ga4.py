from datetime import date

import pandas as pd
import pytest

from article_rec_training_job.components.event_fetchers.ga4 import BaseFetcher
from article_rec_training_job.shared.types.event_fetchers import OutputSchema


@pytest.fixture(scope="module")
def base_fetcher(client_bigquery) -> BaseFetcher:
    component = BaseFetcher(
        gcp_project_id="test",
        site_ga4_property_id="123456789",
        date_start=date(2023, 12, 17),
        date_end=date(2023, 12, 19),
    )
    component.bigquery_client = client_bigquery
    return component


def test_base_fetcher(base_fetcher):
    df, metrics = base_fetcher.fetch()

    assert df.shape == (7, 5)

    timestamps = df[OutputSchema.event_timestamp].astype(int).tolist()
    assert timestamps[0] == 1702858930813469
    assert timestamps[1] == 1702858930813469
    assert timestamps[2] == 1702890591786924
    assert timestamps[3] == 1702890591786924
    assert timestamps[4] == 1702999440236213
    assert timestamps[5] == 1702999440236213
    assert timestamps[6] == 1703004440236213

    assert df.iloc[0][OutputSchema.event_name] == "session_start"
    assert df.iloc[1][OutputSchema.event_name] == "page_view"
    assert df.iloc[2][OutputSchema.event_name] == "session_start"
    assert df.iloc[3][OutputSchema.event_name] == "page_view"
    assert df.iloc[4][OutputSchema.event_name] == "session_start"
    assert df.iloc[5][OutputSchema.event_name] == "page_view"
    assert df.iloc[6][OutputSchema.event_name] == "user_engagement"

    assert df.iloc[0][OutputSchema.page_url] == "https://testpage.com/2023/12/dummy-article/"
    assert df.iloc[1][OutputSchema.page_url] == "https://testpage.com/2023/12/dummy-article/"
    assert df.iloc[2][OutputSchema.page_url] == "https://testpage.com/2023/12/dummy-article/"
    assert df.iloc[3][OutputSchema.page_url] == "https://testpage.com/2023/12/dummy-article/"
    assert df.iloc[4][OutputSchema.page_url] == "https://testpage.com/2023/12/dummy-article/"
    assert df.iloc[5][OutputSchema.page_url] == "https://testpage.com/2023/12/dummy-article/"
    assert df.iloc[6][OutputSchema.page_url] == "https://testpage.com/2023/12/dummy-article/"

    assert df.iloc[0][OutputSchema.engagement_time_msec] is pd.NA
    assert df.iloc[1][OutputSchema.engagement_time_msec] is pd.NA
    assert df.iloc[2][OutputSchema.engagement_time_msec] is pd.NA
    assert df.iloc[3][OutputSchema.engagement_time_msec] is pd.NA
    assert df.iloc[4][OutputSchema.engagement_time_msec] is pd.NA
    assert df.iloc[5][OutputSchema.engagement_time_msec] is pd.NA
    assert df.iloc[6][OutputSchema.engagement_time_msec] == 5000

    assert df.iloc[0][OutputSchema.user_id] == "123456789.1234567890"
    assert df.iloc[1][OutputSchema.user_id] == "123456789.1234567890"
    assert df.iloc[2][OutputSchema.user_id] == "123456789.1234567890"
    assert df.iloc[3][OutputSchema.user_id] == "123456789.1234567890"
    assert df.iloc[4][OutputSchema.user_id] == "123456789.1234567890"
    assert df.iloc[5][OutputSchema.user_id] == "123456789.1234567890"
    assert df.iloc[6][OutputSchema.user_id] == "123456789.1234567890"
