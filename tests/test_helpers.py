import unittest
from datetime import datetime, timedelta

import pandas as pd

from job import helpers


def generate_row(client_id, external_id, delta_secs=0):
    return {
        "client_id": client_id,
        "external_id": external_id,
        "event_category": "snowplow_amp_page_ping",
        "event_action": "impression",
        "session_date": datetime.now().date(),
        "activity_time": datetime.now() + timedelta(seconds=delta_secs),
    }


class TestHelpers(unittest.TestCase):
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
        prepared_df = helpers.prepare_data(df)
        top_times_per_view = helpers.calculate_default_recs(prepared_df)
        # expect article a to be ranked higher than article b
        # article a should have 2 minutes per interaction, article b should have 45 seconds per interaction
        assert all(top_times_per_view.index == [article_a, article_b])
