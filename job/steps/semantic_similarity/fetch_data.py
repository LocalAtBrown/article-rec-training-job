from datetime import datetime

from sites.site import Site


def run(site: Site, start_dt: datetime, end_dt: datetime):
    """
    Main script.
    """
    data = site.bulk_fetch(start_date=start_dt.date(), end_date=end_dt.date())
    return data
