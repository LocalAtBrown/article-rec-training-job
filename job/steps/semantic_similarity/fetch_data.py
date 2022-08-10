import pandas as pd

from sites.site import Site


def run(site: Site, interactions_data: pd.DataFrame):
    """
    Main script.
    """
    assert isinstance(interactions_data, pd.DataFrame)
    pass
