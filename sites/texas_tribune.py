from typing import Optional
import re
import logging
from urllib.parse import urlparse
from requests.models import Response

from bs4 import BeautifulSoup

from sites.helpers import safe_get, ArticleScrapingError, transform_data_google_tag_manager
from sites.site import Site
import pandas as pd

DOMAIN = "www.texastribune.org"
NAME = "texas-tribune"
FIELDS = [ "collector_tstamp", "page_urlpath", "domain_userid"]

# supported url path formats:
#- [`https://www.texastribune.org/2021/09/10/texas-abortion-law-ban-enforcement/?utm_campaign=trib-social&utm_content=1632982788&utm_medium=social&utm_source=twitter`](https://www.texastribune.org/2021/09/10/texas-abortion-law-ban-enforcement/?utm_campaign=trib-social&utm_content=1632982788&utm_medium=social&utm_source=twitter)

# /2021/09/10/texas-abortion-law-ban-enforcement/


def extract_external_id(path: str) -> int:
    # this is actually extracting the content ID for TT. but for naming convention I kept like this
    path=f"https://{DOMAIN}{path}"
    
    try:
        page = safe_get(path)
    except Exception as e:
        msg = f"Error fetching article url: {path}"
        logging.exception(msg)
        raise ArticleScrapingError(msg) from e
    soup = BeautifulSoup(page.text, features="html.parser")

    token = None
    html_content = soup.html
    matched = re.search(r"contentID: '\d+'", str(html_content))
    if matched:
        token = matched.group(0)
        contentID = token.split("\'")[1]
        return int(contentID)
    else:
        None

def scrape_title(res: Response) -> str:
    api_info = res.json()
    headers = api_info['headline']
    return headers


def scrape_published_at(res: Response) -> str:
    # example published_at: '2021-11-12T12:45:35-06:00'
    api_info=res.json()
    pub_date = api_info['pub_date']
    return pub_date

def scrape_path(page: Response) -> str:
    # there are times when older articles redirect to an alternate path, for ex:
    # https://washingtoncitypaper.com/food/article/20830693/awardwinning-chef-michel-richard-dies-at-age-68
    return urlparse(page.url).path


def scrape_article_metadata(page: Response, soup: BeautifulSoup) -> dict:
    logging.info(f"Scraping metadata from url: {page.url}")

    metadata = {}
    scraper_funcs = [
        ("title", scrape_title),
        ("published_at", scrape_published_at),
        ("path", scrape_path),
    ]

    for prop, func in scraper_funcs:
        try:
            val = func(page)
        except Exception as e:
            msg = f"Error scraping {prop} for article url: {page.url}"
            logging.exception(msg)
            raise ArticleScrapingError(msg) from e
        metadata[prop] = val

    return metadata


def validate_article(external_id: int) -> (Response, BeautifulSoup, Optional[str]):
    external_id = int(external_id)
    # hitting the api with 38319.0 is failing but hitting with 38319 is working

    url =f"https://{DOMAIN}/api/v2/articles/{external_id}"
    logging.info(f"Validating article url: {url}")

    try:
        page = safe_get(url)
    except Exception as e:
        msg = f"Error fetching article url: {url}"
        logging.exception(msg)
        raise ArticleScrapingError(msg) from e
    #soup = BeautifulSoup(page.text, features="html.parser")

    #error_msg = None
    
    #reason I am still sending three return values to match the return values in WCP. the scrape metadata expects three values to unpack in the function call

    return page, None, None


TT_SITE = Site(NAME, FIELDS, transform_data_google_tag_manager, extract_external_id, scrape_article_metadata, validate_article)
