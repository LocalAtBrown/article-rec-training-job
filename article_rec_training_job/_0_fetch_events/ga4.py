import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import StrEnum
from functools import cached_property
from typing import Protocol

import pandas as pd
from google.cloud import bigquery


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


@dataclass
class GA4EventQuery:
    statement: str
    executed: bool = False
    execution_uses_cache: bool = field(init=False)
    total_bytes_processed: int = field(init=False)
    total_bytes_billed: int = field(init=False)


@dataclass
class GA4EventFetcher:
    gcp_project_id: str
    site: Site
    date_start: datetime
    date_end: datetime
    num_threads: int
    queries: list[GA4EventQuery] = field(init=False)

    @cached_property
    def dates(self) -> list[datetime]:
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

    def fetch(self) -> pd.DataFrame:
        """
        Fetches data from BigQuery.
        """

        async def fetch_async(self, queries) -> list[pd.DataFrame]:
            return await asyncio.gather(*[self.fetch_single_table_async(query) for query in queries])

        async def fetch_single_table_async(self, query: GA4EventQuery) -> pd.DataFrame:
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(None, self.fetch_single_table, query)

        self.queries = [self.construct_single_table_query(date) for date in self.dates]
        dfs = asyncio.run(fetch_async(self, self.queries))
        return pd.concat([pd.DataFrame(), *dfs])

    def fetch_single_table(self, query: GA4EventQuery) -> pd.DataFrame:
        """
        Fetches data from a single table.
        """
        job = self.bigquery_client.query(query.statement, job_config=self.bigquery_job_config)
        df = job.to_dataframe()

        query.executed = True
        query.execution_uses_cache = job.cache_hit
        query.total_bytes_processed = job.total_bytes_processed
        query.total_bytes_billed = job.total_bytes_billed
        return df

    def construct_single_table_query(self, date: datetime) -> GA4EventQuery:
        """
        Constructs a query for a single table, which represents a single day of data.
        """
        table_name = f"{self.gcp_project_id}.{self.site.ga4_property_id}.events_{date.strftime('%Y%m%d')}"
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
                `{table_name}`
            ORDER BY
                {Column.USER_PSEUDO_ID},
                {Column.EVENT_TIMESTAMP}
        """
        return GA4EventQuery(statement=statement)
