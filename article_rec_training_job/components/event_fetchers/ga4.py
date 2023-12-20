import asyncio
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from functools import cached_property
from typing import final

import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from loguru import logger

from article_rec_training_job.shared.helpers.time import get_elapsed_time
from article_rec_training_job.shared.types.event_fetchers import (
    Metrics,
    OutputDataFrame,
    OutputSchema,
)


@dataclass(frozen=True)
class GA4EventTable:
    table_id: str
    exists: bool


@dataclass
class GA4EventQuery:
    table: GA4EventTable
    statement: str
    executed: bool = False
    execution_used_cache: bool = field(init=False)
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


@dataclass
class BaseFetcher:
    """
    Base, self-contained, GA4 event fetcher component.
    """

    gcp_project_id: str
    site_ga4_property_id: str
    date_start: date
    date_end: date

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
                {OutputSchema.event_timestamp},
                {OutputSchema.event_name},
                user_pseudo_id AS {OutputSchema.user_id},
                (
                    SELECT
                        value.int_value
                    FROM
                        UNNEST(event_params)
                    WHERE
                        key = 'engagement_time_msec'
                )
                AS {OutputSchema.engagement_time_msec},
                (
                    SELECT
                        value.string_value
                    FROM
                        UNNEST(event_params)
                    WHERE
                        key = 'page_location'
                )
                AS {OutputSchema.page_url}
            FROM
                `{table.table_id}`
            ORDER BY
                {OutputSchema.user_id},
                {OutputSchema.event_timestamp}
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
        query.execution_used_cache = job.cache_hit or False
        query.total_bytes_processed = job.total_bytes_processed
        query.total_bytes_billed = job.total_bytes_billed

        return df, query

    @staticmethod
    def transform_df_to_required_schema(df: pd.DataFrame) -> OutputDataFrame:
        """
        Transforms DataFrame of queried events to required schema.
        """
        # Convert event_timestamp int to pandas datetime
        df[OutputSchema.event_timestamp] = pd.to_datetime(df[OutputSchema.event_timestamp], unit="ns", utc=True)

        return df.pipe(OutputDataFrame)  # pipe to appease mypy, task will do the actual schema validation

    @final
    def fetch(self) -> tuple[OutputDataFrame, Metrics]:
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

        # Concatenate all dataframes (to an empty DataFrame to prevent error if no data were fetched)
        df = pd.concat([pd.DataFrame(), *dfs])

        # Convert DataFrame to required schema
        df = self.transform_df_to_required_schema(df)

        # Construct metrics object
        metrics = Metrics(
            num_tables_exist=sum([table.exists for table in tables]),
            num_queries_executed=sum([query.executed for query in self.queries]),
            num_queries_used_cache=sum([query.execution_used_cache for query in self.queries]),
            time_taken_to_find_tables=self.time_taken_to_construct_table_objects,
            time_taken_to_fetch_events=self.time_taken_to_fetch_events,
            total_bytes_fetched=sum([query.total_bytes_processed for query in self.queries]),
            total_bytes_billed=sum([query.total_bytes_billed for query in self.queries]),
            total_rows_fetched=len(df),
        )

        return df, metrics
