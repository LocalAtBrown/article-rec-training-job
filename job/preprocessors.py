import json

import pandas as pd

from lib.config import config
from lib.bucket import s3_download

BUCKET_NAME = config.get("GA_DATA_BUCKET")


def fetch_latest_data() -> pd.DataFrame:
    # TODO remove hardcoded s3_object when live data is available
    latest_data_key = "ting.zhang/90_day_sessions_2020_09_10_2020_09_13.json"
    data_filepath = "/app/tmp/data.json"
    s3_download(BUCKET_NAME, latest_data_key, data_filepath)
    with open(data_filepath) as f:
        sessions_dict = json.load(f)
    import pdb

    pdb.set_trace()
    return flatten_raw_data(sessions_dict)


def flatten_raw_data(sessions_dict: dict) -> pd.DataFrame:
    rows = [
        {
            "client_id": client_id,
            "session_id": session["sessionId"],
            "device_category": session["deviceCategory"],
            "platform": session["platform"],
            "data_source": session["dataSource"],
            "session_date": session["sessionDate"],
            "activity_time": activity["activityTime"],
            "source": activity["source"],
            "medium": activity["medium"],
            "channel_grouping": activity["channelGrouping"],
            "campaign": activity["campaign"],
            "keyword": activity["keyword"],
            "hostname": activity["hostname"],
            "landing_page_path": activity["landingPagePath"],
            "activity_type": activity["activityType"],
            **get_type_specific_fields(activity),
        }
        for client_id, sessions in sessions_dict.items()
        for session in sessions
        for activity in session["activities"]
    ]

    return pd.DataFrame(rows)


def get_type_specific_fields(activity: dict) -> dict:
    if activity["activityType"] == "EVENT":
        return {
            "event_category": activity["event"]["eventCategory"],
            "event_action": activity["event"]["eventAction"],
            "page_path": activity["landingPagePath"],
        }
    elif activity["activityType"] == "PAGEVIEW":
        return {
            "event_category": "pageview",
            "event_action": "pageview",
            "page_path": activity["pageview"]["pagePath"],
        }
    else:
        logging.info(f"Couldn't find activity field for type: {activity['activityType']}")
        return {
            "event_category": None,
            "event_action": None,
            "page_path": None,
        }
