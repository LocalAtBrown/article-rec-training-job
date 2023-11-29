from typing import Protocol

import pandas as pd


class EventFetcher(Protocol):
    def fetch(self) -> pd.DataFrame:
        ...

    def post_fetch(self) -> None:
        ...


def fetch_events(fetcher: EventFetcher) -> pd.DataFrame:
    df = fetcher.fetch()
    fetcher.post_fetch()
    return df
