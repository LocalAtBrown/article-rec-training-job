import logging
import datetime
import os

import pandas as pd
import numpy as np
import redshift_connector as rc

from sites.site import Site
from lib.config import config


def get_table(table):
    if os.environ.get("stage") == "prod":
        return table
    return "dev" + table


def set_dwell_seconds(df: pd.DataFrame):
    df["duration"] = (df["duration"] / np.timedelta64(1, "s")).astype("int")
    return df


def get_connection():
    return rc.connect(
        host=config.get("REDSHIFT_DB_HOST"),
        database=config.get("REDSHIFT_DB_NAME"),
        user=config.get("REDSHIFT_DB_USER"),
        password=config.get("REDSHIFT_DB_PASSWORD"),
    )


def update_dwell_times(df: pd.DataFrame, date: datetime.date, site: Site):
    """
    Update the data warehouse for the given date
    df columns: client_id, article_id, date, duration, site
    """

    df = df[df["session_date"] == np.datetime64(date)]
    df = df[["duration", "session_date", "client_id", "article_id", "site"]].copy()
    df = set_dwell_seconds(df)

    conn = get_connection()

    staging_table = get_table("staging")
    dwell_time_table = get_table("dwelltimes")

    with conn.cursor() as cursor:

        logging.info("Creating staging table...")
        cursor.execute(f"create temp table {staging_table} (like {dwell_time_table});")
        logging.info("Uploading today's data...")
        cursor.write_dataframe(
            df,
            staging_table,
        )
        logging.info("Deleting stale data...")
        cursor.execute(
            f"""
                delete from {dwell_time_table} where session_date = '{date}' and site = '{site.name}';
                """
        )
        logging.info("Merging staging table into main...")
        cursor.execute(
            f"""
                insert into {dwell_time_table}
                    select sum(duration) as duration, session_date, client_id, article_id, site 
                        from {staging_table} group by 2,3,4,5;
            """
        )
    conn.commit()
    conn.close()


def get_dwell_times(site: Site, days=28) -> pd.DataFrame:
    """
    Pulls the last {days} worth of data from the data warehouse and applies a series of
    filtering operations in SQL:
    - Removes articles with only one reader
    - Removes readers who only read one article
    - Removes interactions longer than 10 minutes (600 seconds)
    - Removes users who spent less than one total minute on the site
    """
    table = get_table("dwelltimes")
    query = f"""
    with article_agg as (
        select count(*) as num_users_per_article, article_id 
        from {table} group by article_id
    ),
    user_agg as (
        select count(*) as num_articles_per_user, sum(duration) as duration_per_user, client_id
        from {table} group by client_id
    )
    select {table}.article_id, {table}.client_id, session_date, duration from {table}
    join article_agg on article_agg.article_id = {table}.article_id
    join user_agg on user_agg.client_id = {table}.client_id
    where site = '{site.name}'
    -- fitler for session dates greater than `days` days ago
    and timestamp_cmp_date(dateadd(day, -{days-1}, current_date), session_date) != 1
    and num_users_per_article > 1
    and num_articles_per_user > 1
    and duration < 600
    and duration_per_user > 60
    """
    conn = get_connection()
    with conn.cursor() as cursor:
        cursor.execute(query)
        results = cursor.fetchall()
        return pd.DataFrame(
            results, columns=["article_id", "client_id", "session_date", "duration"]
        )
