from sites.site import Site
from sites.sites import Sites


def get_site(site_name) -> Site:
    site = Sites.mapping.get(site_name)
    if site is None:
        raise Exception(f"Could not find site {site_name} in sites.py")
    return site
