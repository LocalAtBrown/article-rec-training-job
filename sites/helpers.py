from typing import Dict
import requests as req

from retrying import retry

def transform_data_google_tag_manager(df: pd.DataFrame) -> pd.DataFrame:
    """
        requires a dataframe with the following fields:
                - domain_userid
                    - collector_tstamp
                        - page_urlpath
    returns a dataframe with the following fields:
        - client_id
            - session_date
                - activity_time
                    - landing_page_path
                        - event_category (conversions, newsletter sign-ups TK)
                            - event_action (conversions, newsletter sign-ups TK)
    """
    transformed_df = pd.DataFrame()
    transformed_df["client_id"] = df['domain_userid']
    transformed_df["activity_time"] = pd.to_datetime(df.collector_tstamp)
    transformed_df["session_date"] = pd.to_datetime(transformed_df.activity_time.dt.date)
    transformed_df["landing_page_path"] = df.page_urlpath
    transformed_df["event_category"] = "snowplow_amp_page_ping"
    transformed_df["event_category"] = transformed_df["event_category"].astype("category")
    transformed_df["event_action"] = "impression"
    transformed_df["event_action"] = transformed_df["event_action"].astype("category")

    return transformed_df
class ArticleScrapingError(Exception):
    pass

class ArticleFetchError(Exception):
    pass

@retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000)
def safe_get(url: str, headers: Dict[str, str] = None) -> str:
    TIMEOUT_SECONDS = 30
    default_headers = {"User-Agent": "article-rec-training-job/1.0.0"}
    if headers:
        default_headers.update(headers)
    page = req.get(url, timeout=TIMEOUT_SECONDS, headers=default_headers)
    return page
