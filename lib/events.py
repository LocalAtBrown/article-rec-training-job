from enum import Enum


# Snowplow Events
class Event(Enum):
    PAGE_PING = "page_ping"
    PAGE_VIEW = "page_view"
    RECOMMENDATION_FLOW = "recommendation_flow"


PING_INTERVAL = 10
