from datetime import datetime, timedelta
import unittest

import pandas as pd

from job.steps.save_defaults import calculate_default_recs
from job.steps.preprocess import common_preprocessing
from tests.base import BaseTest


def generate_row(client_id, external_id, delta_secs=0):
    return {
        "client_id": client_id,
        "external_id": external_id,
        "event_category": "snowplow_amp_page_ping",
        "event_action": "impression",
        "session_date": datetime.now().date(),
        "activity_time": datetime.now() + timedelta(seconds=delta_secs),
    }


class TestSaveDefaults(BaseTest):
    def test_calculate_default_recs(self) -> None:
        basit = "client.a"
        kai = "client.b"
        article_a = 123
        article_b = 456

        data = [
            # basit and kai both read article a for 2 minutes
            generate_row(basit, article_a),
            generate_row(basit, article_a, 120),
            generate_row(kai, article_a),
            generate_row(kai, article_a, 120),
            # kai read article b for 1 minute, basit read article b for 30 seconds
            generate_row(kai, article_b, 120),
            generate_row(kai, article_b, 150),
            generate_row(kai, article_b, 180),
            generate_row(basit, article_b, 120),
            generate_row(basit, article_b, 150),
        ]

        df = pd.DataFrame(data)
        prepared_df = common_preprocessing(df)
        top_times_per_view = calculate_default_recs(prepared_df)
        # article a should have 2 minutes per interaction, article b should have 45 seconds per interaction
        assert all(top_times_per_view.index == [article_a, article_b])
