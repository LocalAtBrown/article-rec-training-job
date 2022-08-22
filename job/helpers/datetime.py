from datetime import date, datetime

import pandas as pd


def pad_date(date_expr: int) -> str:
    return str(date_expr).zfill(2)


def chunk_name(dt: datetime) -> str:
    month = pad_date(dt.month)
    day = pad_date(dt.day)
    hour = pad_date(dt.hour)
    return f"{dt.year}/{month}/{day}/{hour}"


def time_decay(
    data_df: pd.DataFrame,
    experiment_date: date,
    half_life: float,
    date_col="session_date",
    duration_col="duration",
) -> pd.DataFrame:
    """
    Applies basic exponential decay based on the difference between the "date" column
    and the current date argument to the dwell time
    """
    decay_factor = decay_fn(experiment_date, data_df[date_col], half_life)
    data_df[duration_col] *= decay_factor
    return data_df


def decay_fn(experiment_date: date, df_column: pd.Series, half_life: float) -> pd.Series:
    """half life decay a pandas Series"""
    return 0.5 ** ((experiment_date - df_column).dt.days / half_life)
