import numpy as np
import pandas as pd

from datetime import datetime, timezone

from lib.bucket import save_outputs
from typing import List

from sites.sites import Sites
from sites.site import Site


def articles_to_df(articles: List["Article"]) -> pd.DataFrame:
    df_data = {
        "article_id": [a.id for a in articles],
        "external_id": [a.external_id for a in articles],
        "published_at": [a.published_at for a in articles],
        "landing_page_path": [a.path for a in articles],
        "site": [a.site for a in articles],
    }
    article_df = pd.DataFrame(df_data)
    return article_df


def decay_fn(
    experiment_date: datetime.date, df_column: pd.Series, half_life: float
) -> pd.Series:
    """ half life decay a pandas Series"""
    return 0.5 ** ((experiment_date - df_column).dt.days / half_life)


def get_site(site_name) -> Site:
    site = Sites.mapping.get(site_name)
    if site is None:
        raise Exception(f"Could not find site {site_name} in sites.py")
    return site
