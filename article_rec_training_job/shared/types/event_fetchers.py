from typing import Annotated, TypeAlias

import pandera as pa
from pandas import DatetimeTZDtype
from pandera.typing import DataFrame, Series


class OutputSchema(pa.DataFrameModel):
    """
    Pandera schema for GA4 event fetcher output.
    Callng .column_name returns the name of the column as a string (e.g. `OutputSchema.event_name = "event_name"`),
    which is useful for pandas DataFrame operations involving columns.
    """

    event_timestamp: Series[Annotated[DatetimeTZDtype, "ns", "utc"]]
    event_name: str
    user_id: str
    engagement_time_msec: int = pa.Field(nullable=True)
    page_url: str


OutputDataFrame: TypeAlias = DataFrame[OutputSchema]
