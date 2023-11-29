from enum import StrEnum


class Column(StrEnum):
    # BigQuery columns
    EVENT_TIMESTAMP = "event_timestamp"
    EVENT_NAME = "event_name"
    EVENT_PARAMS = "event_params"
    USER_PSEUDO_ID = "user_pseudo_id"
    # Custom columns
    EVENT_PAGE_LOCATION = "event_page_location"
    EVENT_ENGAGEMENT_TIME_MSEC = "event_engagement_time_msec"
