from copy import deepcopy

import sites.philadelphia_inquirer as site
from tests.base import BaseTest


class TestPhiladelphiaInquirer(BaseTest):
    def setUp(self) -> None:
        res = {
            "headlines": {
                "meta_title": "Russia-Ukraine updates: Kremlin strikes Ukrainian city with suspected cluster munitions; peace talks end",
                "basic": "Russia strikes Ukrainian city with suspected cluster munitions as peace talks end",
            },
            "publish_date": "2022-03-01T18:37:44.908Z",
            "_id": "TCKTFPUVXJE5LJXG7THB4PLMKM",
            "canonical_url": "/news/nation-world/live/russia-ukraine-conflict-crisis-20220228.html",
        }
        self.res = deepcopy(res)
        super().setUp()

    def test_get_headline(self) -> None:
        title = site.get_headline(self.res)
        assert title == self.res["headlines"]["meta_title"]

    def test_get_headline__empty_meta_title(self) -> None:
        self.res["headlines"]["meta_title"] = ""
        title = site.get_headline(self.res)
        assert title == self.res["headlines"]["basic"]
