import asyncio
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from enum import StrEnum
from functools import cached_property
from typing import Protocol

import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from loguru import logger


class Column(StrEnum):
    # BigQuery columns
    EVENT_TIMESTAMP = "event_timestamp"
    EVENT_NAME = "event_name"
    EVENT_PARAMS = "event_params"
    USER_PSEUDO_ID = "user_pseudo_id"
    # Custom columns
    EVENT_PAGE_LOCATION = "event_page_location"
    EVENT_ENGAGEMENT_TIME_MSEC = "event_engagement_time_msec"


class Site(Protocol):
    ga4_property_id: str


@dataclass(frozen=True)
class GA4EventTable:
    table_id: str
    exists: bool


@dataclass
class GA4EventQuery:
    table: GA4EventTable
    statement: str
    executed: bool = False
    execution_uses_cache: bool = field(init=False)
    total_bytes_processed: int = field(init=False)
    total_bytes_billed: int = field(init=False)


def check_bigquery_table_exists(client: bigquery.Client, table_id: str) -> bool:
    """
    Checks if a BigQuery table exists.
    """
    try:
        client.get_table(table_id)
    except NotFound:
        return False
    else:
        return True


@dataclass
class GA4EventFetcher:
    gcp_project_id: str
    site: Site
    date_start: date
    date_end: date
    queries: list[GA4EventQuery] = field(init=False)

    @cached_property
    def dates(self) -> list[date]:
        """
        List of dates between date_start and date_end.
        """
        return [self.date_start + timedelta(days=i) for i in range((self.date_end - self.date_start).days + 1)]

    @cached_property
    def bigquery_client(self) -> bigquery.Client:
        """
        BigQuery client based on provided project ID.
        """
        return bigquery.Client(project=self.gcp_project_id)

    @cached_property
    def bigquery_job_config(self) -> bigquery.QueryJobConfig:
        """
        BigQuery job config.
        """
        return bigquery.QueryJobConfig(
            dry_run=False,
            use_query_cache=True,
        )

    @cached_property
    def bigquery_dataset_name(self) -> str:
        """
        Name of BigQuery dataset representing site.
        """
        return f"{self.gcp_project_id}.{self.site.ga4_property_id}"

    @cached_property
    def tables(self) -> list[GA4EventTable]:
        """
        List of tables to query.
        """
        return [self.construct_table_object(dt) for dt in self.dates]

    def construct_table_object(self, dt: date) -> GA4EventTable:
        """
        Constructs a table name for a single day of data.
        """
        date_str = dt.strftime("%Y%m%d")

        # If date is today, use the intraday table
        today = datetime.utcnow().date()
        if dt == today:
            table_id = f"{self.bigquery_dataset_name}.events_intraday_{date_str}"
            table_exists = check_bigquery_table_exists(self.bigquery_client, table_id)
            return GA4EventTable(table_id=table_id, exists=table_exists)

        # Else, use the historical table
        table_id = f"{self.bigquery_dataset_name}.events_{date_str}"
        if check_bigquery_table_exists(self.bigquery_client, table_id):
            return GA4EventTable(table_id=table_id, exists=True)

        # If that table doesn't exist, try the intraday table as a last resort.
        table_id = f"{self.bigquery_dataset_name}.events_intraday_{date_str}"
        table_exists = check_bigquery_table_exists(self.bigquery_client, table_id)
        return GA4EventTable(table_id=table_id, exists=table_exists)

    def construct_single_table_query(self, table: GA4EventTable) -> GA4EventQuery:
        """
        Constructs a query for a single table, which represents a single day of data.
        """
        statement = f"""
            SELECT
                {Column.EVENT_TIMESTAMP},
                {Column.EVENT_NAME},
                {Column.USER_PSEUDO_ID}
                (
                    SELECT
                        value.int_value
                    FROM
                        UNNEST({Column.EVENT_PARAMS})
                    WHERE
                        key = 'engagement_time_msec'
                )
                AS {Column.EVENT_ENGAGEMENT_TIME_MSEC},
                (
                    SELECT
                        value.string_value
                    FROM
                        UNNEST({Column.EVENT_PARAMS})
                    WHERE
                        key = 'page_location'
                )
                AS {Column.EVENT_PAGE_LOCATION}
            FROM
                `{table.table_id}`
            ORDER BY
                {Column.USER_PSEUDO_ID},
                {Column.EVENT_TIMESTAMP}
        """
        return GA4EventQuery(statement=statement)

    def fetch_single_table(self, table: GA4EventTable) -> tuple[pd.DataFrame, GA4EventQuery]:
        """
        Fetches data from a single table.
        """
        if not table.exists:
            logger.warning(f"Table {table.table_id} does not exist and will be skipped.")
            return pd.DataFrame(), GA4EventQuery(table=table, statement="")

        query = self.construct_single_table_query(table)
        job = self.bigquery_client.query(query.statement, job_config=self.bigquery_job_config)
        df = job.to_dataframe()

        # Update query object with job metadata in-place
        query.executed = True
        query.execution_uses_cache = job.cache_hit
        query.total_bytes_processed = job.total_bytes_processed
        query.total_bytes_billed = job.total_bytes_billed

        return df, query

    def fetch(self) -> pd.DataFrame:
        """
        Fetches data from BigQuery.
        """
        # We're using asyncio to concurrently fetch data from multiple tables:
        # https://github.com/googleapis/python-bigquery/issues/453
        async def fetch_async(tables: list[GA4EventTable]) -> list[tuple[pd.DataFrame, GA4EventQuery]]:
            return await asyncio.gather(*[self.fetch_single_table_async(table) for table in tables])

        async def fetch_single_table_async(table: GA4EventTable) -> tuple[pd.DataFrame, GA4EventQuery]:
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(None, self.fetch_single_table, table)

        # Fetch data from all tables
        results = asyncio.run(fetch_async(self.tables))
        dfs, self.queries = zip(*results)

        return pd.concat([pd.DataFrame(), *dfs])
