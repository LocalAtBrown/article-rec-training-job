from sites.washington_city_paper import WCP_SITE
from sites.texas_tribune import TT_SITE
from sites.philadelphia_inquirer import PI_SITE

class Sites:
    WCP = WCP_SITE
    TT = TT_SITE
    PI = PI_SITE
    mapping = { WCP_SITE.name: WCP_SITE, 
                PI_SITE.name: PI_SITE,
                TT_SITE.name:TT_SITE }
