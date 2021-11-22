from collections import namedtuple

from sites.washington_city_paper import WCP_SITE
from sites.texas_tribune import TT_SITE

class Sites:
    WCP = WCP_SITE
    TT = TT_SITE
    mapping = {WCP_SITE.name: WCP_SITE, TT_SITE.name:TT_SITE}
