import pandas as pd


GOOGLE_TAG_MANAGER_RAW_FIELDS = {
    "collector_tstamp",
    "domain_userid",
    "event_name",
    "page_urlpath",
}


def transform_data_google_tag_manager(df: pd.DataFrame) -> pd.DataFrame:
    """
        requires a dataframe with the following fields:
            - domain_userid
            - collector_tstamp
            - page_urlpath
            - event_name
    returns a dataframe with the following fields:
        - client_id
            - session_date
                - activity_time
                    - landing_page_path
                        - event_category (conversions, newsletter sign-ups TK)
                            - event_action (conversions, newsletter sign-ups TK)
    """
    transformed_df = pd.DataFrame()
    transformed_df["client_id"] = df["domain_userid"]
    transformed_df["activity_time"] = pd.to_datetime(df.collector_tstamp).dt.round("1s")
    transformed_df["session_date"] = pd.to_datetime(transformed_df.activity_time.dt.date)
    transformed_df["landing_page_path"] = df.page_urlpath
    transformed_df["event_name"] = df.event_name
    transformed_df["event_name"] = transformed_df["event_name"].astype("category")

    return transformed_df
