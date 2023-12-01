import asyncio
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from functools import cached_property
from typing import Final

import pandas as pd
import pandera as pa
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from loguru import logger
from pandera.typing import DataFrame

from article_rec_training_job.helpers.math import convert_bytes_to_human_readable
from article_rec_training_job.helpers.time import get_elapsed_time


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
    # Could turn this into async, but compared to actual query execution, this is not gonna be a bottleneck.
    try:
        client.get_table(table_id)
    except NotFound:
        return False
    else:
        return True


class OutputSchema(pa.DataFrameModel):
    """
    Pandera schema for GA4 event fetcher output.
    Callng .column_name returns the name of the column as a string (e.g. `OutputSchema.event_name = "event_name"`),
    which is useful for pandas DataFrame operations involving columns.
    """

    event_timestamp: int
    event_name: str
    user_pseudo_id: str
    event_engagement_time_msec: int = pa.Field(nullable=True)
    event_page_location: str


@dataclass
class BaseFetcher:
    """
    Base, self-contained, GA4 event fetcher component.
    """

    gcp_project_id: Final[str]
    site_ga4_property_id: Final[str]
    date_start: Final[date]
    date_end: Final[date]

    queries: list[GA4EventQuery] = field(init=False, repr=False)
    time_taken_to_construct_table_objects: float = field(init=False, repr=False)
    time_taken_to_fetch_events: float = field(init=False, repr=False)

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
    def bigquery_dataset_id(self) -> str:
        """
        Name of BigQuery dataset representing site.
        """
        return f"{self.gcp_project_id}.analytics_{self.site_ga4_property_id}"

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
            table_id = f"{self.bigquery_dataset_id}.events_intraday_{date_str}"
            table_exists = check_bigquery_table_exists(self.bigquery_client, table_id)
            return GA4EventTable(table_id=table_id, exists=table_exists)

        # Else, use the historical table
        table_id = f"{self.bigquery_dataset_id}.events_{date_str}"
        if check_bigquery_table_exists(self.bigquery_client, table_id):
            return GA4EventTable(table_id=table_id, exists=True)

        # If that table doesn't exist, try the intraday table as a last resort.
        table_id = f"{self.bigquery_dataset_id}.events_intraday_{date_str}"
        table_exists = check_bigquery_table_exists(self.bigquery_client, table_id)
        return GA4EventTable(table_id=table_id, exists=table_exists)

    def construct_single_table_query(self, table: GA4EventTable) -> GA4EventQuery:
        """
        Constructs a query for a single table, which represents a single day of data.
        """
        statement = f"""
            SELECT
                event_timestamp,
                event_name,
                user_pseudo_id,
                (
                    SELECT
                        value.int_value
                    FROM
                        UNNEST(event_params)
                    WHERE
                        key = 'engagement_time_msec'
                )
                AS event_engagement_time_msec,
                (
                    SELECT
                        value.string_value
                    FROM
                        UNNEST(event_params)
                    WHERE
                        key = 'page_location'
                )
                AS event_page_location
            FROM
                `{table.table_id}`
            ORDER BY
                user_pseudo_id,
                event_timestamp
        """
        return GA4EventQuery(table=table, statement=statement)

    def fetch_single_table(self, table: GA4EventTable) -> tuple[pd.DataFrame, GA4EventQuery]:
        """
        Fetches data from a single table.
        """
        if not table.exists:
            logger.warning(f"Table {table.table_id} does not exist and will be skipped.")
            return pd.DataFrame(), GA4EventQuery(table=table, statement="")

        query = self.construct_single_table_query(table)
        logger.info(f"Executing query for table {table.table_id}...")
        job = self.bigquery_client.query(query.statement, job_config=self.bigquery_job_config)
        df = job.to_dataframe()

        # Update query object with job metadata in-place
        query.executed = True
        query.execution_uses_cache = job.cache_hit
        query.total_bytes_processed = job.total_bytes_processed
        query.total_bytes_billed = job.total_bytes_billed

        return df, query

    @pa.check_types
    def fetch(self) -> DataFrame[OutputSchema]:
        """
        Fetches data from BigQuery.
        """

        @get_elapsed_time
        def construct_all_table_objects() -> list[GA4EventTable]:
            return self.tables

        # We're using asyncio to concurrently fetch data from multiple tables:
        # https://github.com/googleapis/python-bigquery/issues/453
        @get_elapsed_time
        def fetch_all_tables(tables: list[GA4EventTable]) -> list[tuple[pd.DataFrame, GA4EventQuery]]:
            return asyncio.run(fetch_all_tables_async(tables))

        async def fetch_all_tables_async(tables: list[GA4EventTable]) -> list[tuple[pd.DataFrame, GA4EventQuery]]:
            return await asyncio.gather(*[fetch_single_table_async(table) for table in tables])

        async def fetch_single_table_async(table: GA4EventTable) -> tuple[pd.DataFrame, GA4EventQuery]:
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(None, self.fetch_single_table, table)

        # Calling self.tables will trigger construction of table objects
        self.time_taken_to_construct_table_objects, tables = construct_all_table_objects()

        # Fetch data from all tables
        self.time_taken_to_fetch_events, results = fetch_all_tables(tables)
        dfs, self.queries = zip(*results)  # type: ignore

        return pd.concat([pd.DataFrame(), *dfs])

    def post_fetch(self) -> None:
        """
        Post-fetch actions. Base fetcher only logs metrics to stdout.
        """
        num_tables_exist = sum([table.exists for table in self.tables])
        num_queries_executed = sum([query.executed for query in self.queries])
        num_queries_use_cache = sum([query.execution_uses_cache for query in self.queries])
        total_bytes_processed = sum([query.total_bytes_processed for query in self.queries])
        total_bytes_billed = sum([query.total_bytes_billed for query in self.queries])

        logger.info(f"Table object construction took {self.time_taken_to_construct_table_objects:.3f} seconds")
        logger.info(f"Event fetch took {self.time_taken_to_fetch_events:.3f} seconds")
        logger.info(f"{num_tables_exist} tables out of {len(self.tables)} exist")
        logger.info(f"{num_queries_executed} queries out of {len(self.queries)} were executed")
        logger.info(f"{num_queries_use_cache} queries out of {len(self.queries)} used cache")
        logger.info(f"Total bytes processed: {convert_bytes_to_human_readable(total_bytes_processed)}")
        logger.info(f"Total bytes billed: {convert_bytes_to_human_readable(total_bytes_billed)}")


class FetcherWithCloudWatchReporting(BaseFetcher):
    """
    Base fetcher with CloudWatch reporting.
    """

    def post_fetch(self) -> None:
        super().post_fetch()
        self.log_metrics()

    def log_metrics(self) -> None:
        """
        Logs metrics to CloudWatch.
        """
        raise NotImplementedError
