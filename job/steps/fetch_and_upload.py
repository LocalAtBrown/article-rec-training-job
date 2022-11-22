from datetime import datetime, timedelta
from job.steps import fetch_data, scrape_metadata
from job.helpers import warehouse
from sites.templates.site import Site
from lib.config import config


def fetch_and_upload_data(site: Site, dt: datetime, hours=config.get("HOURS_OF_DATA")):
    """
    1. Upload transformed events data to Redshift
    2. Update article metadata
    3. Update dwell times table
    """
    dts = [dt - timedelta(hours=i) for i in range(hours)]

    fetch_data.fetch_transform_upload_chunks(site, dts)
    scrape_metadata.scrape_upload_metadata(site, dts)

    for date in set([dt.date() for dt in dts]):
        warehouse.update_dwell_times(site, date)