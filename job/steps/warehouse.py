import numpy as np
import logging
import pandas as pd
import datetime
from sites.site import Site
from lib.config import config
import os
import redshift_connector as rc


def get_table(table):
    if os.environ.get("stage") == "prod":
        return table
    return "dev" + table


def set_dwell_seconds(df: pd.DataFrame):
    df["duration"] = (df["duration"] / np.timedelta64(1, "s")).astype("int")
    return df


def update_dwell_times(df: pd.DataFrame, date: datetime.date, site: Site):
    """
    Update the data warehouse for the given date
    df columns: client_id, article_id, date, duration, site
    """

    df = df[df["session_date"] == np.datetime64(date)]
    df = df[["duration", "session_date", "client_id", "article_id", "site"]].copy()
    df = set_dwell_seconds(df)

    conn = rc.connect(
        host=config.get("REDSHIFT_DB_HOST"),
        database=config.get("REDSHIFT_DB_NAME"),
        user=config.get("REDSHIFT_DB_USER"),
        password=config.get("REDSHIFT_DB_PASSWORD"),
    )
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
