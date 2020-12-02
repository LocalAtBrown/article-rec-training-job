import logging

from db.models.model import Type
from job.helpers import create_model, create_article, create_rec
from job.scraper import scrape_metadata


def rand_int():
    import random

    return random.randint(100000, 999999)


def run():
    logging.info("Running job...")

    model_id = create_model(type=Type.ARTICLE.value)

    page_path = "/article/194506/10-things-you-didnt-know-about-steakumm/"
    metadata = scrape_metadata(page_path)

    logging.info(f"scraped metadata {metadata}")
