from collections import namedtuple

from sites.washington_city_paper import WCP_SITE


class Sites:
    WCP = WCP_SITE

    mapping = {WCP_SITE.name: WCP_SITE}
