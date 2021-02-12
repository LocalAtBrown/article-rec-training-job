import unittest
from datetime import datetime

import pandas as pd

from job import helpers


def generate_row(client_id, external_id):
    return {
        "client_id": client_id,
        "external_id": external_id,
        "event_category": "pageview",
        "event_action": "pageview",
        "session_date": datetime.now(),
        "activity_time": datetime.now(),
    }


class TestHelpers(unittest.TestCase):
    def test_calculate_default_recs(self) -> None:
        basit = "client.a"
        kai = "client.b"
        article_a = 123
        article_b = 456

        data = [
            # basit and kai both read article a once
            generate_row(basit, article_a),
            generate_row(kai, article_a),
            # while basit was on a date with jonathan, kai read article b three times
            generate_row(kai, article_b),
            generate_row(kai, article_b),
            generate_row(kai, article_b),
        ]

        df = pd.DataFrame(data)
        top_pageviews = helpers.calculate_default_recs(df)
        # expect article a to be ranked higher than article b
        assert all(top_pageviews.index == [article_a, article_b])
        # expect article a to have two unique pageviews, and article b to have one
        assert all(top_pageviews == [2, 1])
