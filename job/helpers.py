import numpy as np
import pandas as pd

from datetime import datetime, timezone

from lib.bucket import save_outputs
from typing import List

from sites.sites import Sites
from sites.site import Site


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
