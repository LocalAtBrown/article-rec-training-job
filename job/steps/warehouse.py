import logging
import datetime
from typing import List

import pandas as pd
import numpy as np
import redshift_connector as rc
import s3fs

from sites.site import Site
from lib.config import config
from lib.events import Event, PING_INTERVAL
from job.helpers import chunk_name
from enum import Enum


class Table(Enum):
    EVENTS = "events"
    DWELL_TIMES = "dwelltimes"
    ARTICLES = "articlerecdb.article"
    PATHS = "articlerecdb.paths"


def get_table(table: Table):
    return config.get("REDSHIFT_PREFIX") + table.value


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


def dts_to_chunk_name_sql(dts: List[datetime.datetime]) -> str:
    """Converts a list of dts to SQL-style array string of chunk names"""
    chunk_names = (chunk_name(dt) for dt in dts)
    chunk_names = [f"'{name}'" for name in set(chunk_names)]
    return f"({','.join(chunk_names)})"


def write_events(
    site: Site,
    start_dt: datetime.datetime,
    df: pd.DataFrame,
) -> None:

    logging.info(f"Uploading events data to Redshift...")

    df["site"] = site.name
    df["chunk_name"] = [chunk_name(dt) for dt in df["activity_time"]]
    # Convert chunk names to SQL array format
    chunk_names_str = dts_to_chunk_name_sql(list(df["activity_time"]))

    s3_name = chunk_name(start_dt)
    s3_path = (
        f"{config.get('REDSHIFT_CACHE_BUCKET')}/events/{site.name}/{s3_name}.tsv"
    )
    s3 = s3fs.S3FileSystem(anon=False)
    with s3.open(s3_path, "w") as f:
        df.to_csv(f, index=False, sep="\t")

    events_table = get_table(Table.EVENTS)
    staging = f"events_staging"
    conn = get_connection()

    with conn.cursor() as cursor:
        cursor.execute(f"create temp table {staging} (like {events_table})")
        cursor.execute(
            f"""
                delete from {events_table} 
                where chunk_name in {chunk_names_str}
                and site = '{site.name}'
            """
        )
        cursor.execute(
            f"""
                COPY {staging} FROM 's3://{s3_path}'
                CREDENTIALS 'aws_iam_role={config.get("REDSHIFT_SERVICE_ROLE")}'
                DELIMITER AS '\t'
                DATEFORMAT 'YYYY-MM-DD'
                IGNOREHEADER 1
                csv;
            """
        )
        cursor.execute(
            f"""
                INSERT INTO {events_table} 
                select * from {staging}
                """
        )
    conn.commit()
    conn.close()


def get_paths_to_update(site: Site, dts: List[datetime.datetime]) -> pd.DataFrame:
    """
    Fetches URLs from the events table that need to be scraped and added to the article DB

    Returns: df of landing_page_path
    """

    chunk_names_str = dts_to_chunk_name_sql(dts)
    events = get_table(Table.EVENTS)
    paths = get_table(Table.PATHS)
    articles = get_table(Table.ARTICLES)

    query = f"""
        with paths as (
            select distinct landing_page_path from {events}
            where chunk_name in {chunk_names_str}
            and site = '{site.name}'
        ),
        articles as (
            select 
                paths.landing_page_path, 
                a.published_at, 
                a.updated_at,
                p.external_id,
                p.exclude_reason
            from paths
            left join {paths} p
                on paths.landing_page_path = p.path
                and p.site = '{site.name}'
            left join {articles} a
                on p.external_id = a.external_id
                and p.site = a.site
        )
        select landing_page_path, external_id from articles
        where 
            -- scrape articles that aren't in the cache:
            -- re-scrape articles that meet these conditions:
            --    no publish date
            --    published within the last day
            exclude_reason is NULL and (
                external_id is NULL or
                (
                    published_at is NULL or
                    (published_at > current_date - 1 and 
                )
            )
    """
    connection = get_connection()
    with connection.cursor() as cursor:
        cursor.execute(query)
        results = cursor.fetchall()
        return pd.DataFrame(results, columns=["landing_page_path", "external_id"])


def update_dwell_times(site: Site, date: datetime.date):
    """
    Refresh the dwell times table for the given date
    using the existing events table
    """

    conn = get_connection()

    events_table = get_table(Table.EVENTS)
    dwell_time_table = get_table(Table.DWELL_TIMES)
    articles = get_table(Table.ARTICLES)

    conn = get_connection()
    with conn.cursor() as cursor:

        logging.info(f"Deleting stale data from {dwell_time_table}...")
        cursor.execute(
            f"""
                delete from {dwell_time_table} where session_date = '{date}' and site = '{site.name}';
                """
        )
        logging.info(f"Writing dwell time table {date}...")
        cursor.execute(
            f"""
            insert into {dwell_time_table}
            select 
                sum(s.ping_count) * {PING_INTERVAL} as duration, 
                s.session_date, 
                s.client_id, 
                a.id as article_id, 
                s.site,
                a.external_id,
                a.published_at
            from {events_table} s 
            join {articles} a on 
                a.path = s.landing_page_path
                and a.site = s.site
            where event_name = '{Event.PAGE_PING.value}'
                and session_date = '{date}'
            group by 2,3,4,5,6,7
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
    table = get_table(Table.DWELL_TIMES)
    article_table = get_table(Table.ARTICLES)
    query = f"""
    with article_agg as (
        select count(*) as num_users_per_article, article_id 
        from {table} 
        where {table}.site = '{site.name}'
            and timestamp_cmp_date(dateadd(day, -{days-1}, current_date), session_date) != 1
        group by article_id
    ),
    user_agg as (
        select count(*) as num_articles_per_user, sum(duration) as duration_per_user, client_id
        from {table} 
        where {table}.site = '{site.name}'
            and timestamp_cmp_date(dateadd(day, -{days-1}, current_date), session_date) != 1
        group by client_id
    )
    select 
        {table}.article_id, {table}.external_id, {table}.client_id, session_date, duration, cast({article_table}.published_at as date) as published_at
    from {table}
    join article_agg on article_agg.article_id = {table}.article_id
    join user_agg on user_agg.client_id = {table}.client_id
    join {article_table} on {article_table}.id = {table}.article_id
    where {table}.site = '{site.name}'
    -- filter for session dates greater than `days` days ago
    and timestamp_cmp_date(dateadd(day, -{days-1}, current_date), session_date) != 1
    and num_users_per_article > 5
    and num_articles_per_user > 2 
    and duration between 30 and 600
    and duration_per_user > 60
    """
    conn = get_connection()
    with conn.cursor() as cursor:
        cursor.execute(query)
        results = cursor.fetchall()
        return pd.DataFrame(
            results,
            columns=[
                "article_id",
                "external_id",
                "client_id",
                "session_date",
                "duration",
                "published_at",
            ],
        )


def get_default_recs(site: Site, days=7, limit=50):
    """
    Pull the articles that were the most popular in the last {days} days
    """
    table = get_table(Table.DWELL_TIMES)
    query = f"""
        with total_times as (
            select 
                article_id
                , max(published_at) as publish_date
                , sum(duration) as total_duration
                , count(distinct client_id) n_users 
            from {table} 
            where
                -- filter for session dates greater than `days` days ago
                timestamp_cmp_date(dateadd(day, -{days-1}, current_date), session_date) != 1
                and {table}.site = '{site.name}'
            group by article_id
        )
        select 
            article_id,
            publish_date,
            total_duration as score
        from total_times 
        order by 3 desc limit {limit}
    """
    conn = get_connection()
    with conn.cursor() as cursor:
        cursor.execute(query)
        results = cursor.fetchall()
        return pd.DataFrame(results, columns=["article_id", "publish_date", "score"])
