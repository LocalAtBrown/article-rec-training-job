import datetime
import logging

from scipy.spatial import distance

from db.mappings.model import Type
from db.helpers import create_model
from job.helpers import find_or_create_articles, format_ga
from job import preprocessors
from job import models
from lib.config import MAX_RECS
from sites.sites import Sites


def run():
    logging.info("Running job...")

    model_id = create_model(type=Type.ARTICLE.value)
    logging.info(f"Created model with id {model_id}")
    ga_df = preprocessors.fetch_latest_data()
    article_df = find_or_create_articles(Sites.WCP, list(ga_df["page_path"].unique()))
    ga_df = ga_df.join(article_df, on='page_path')

    EXPERIMENT_DATE = datetime.date.today()
    # Hyperparameters derived using optimize_ga_pipeline.ipynb notebook in google-analytics-exploration
    formatted_df = format_ga(ga_df, date_list=[EXPERIMENT_DATE], half_life=59.631698)
    model = models.train_model(
        X=formatted_df,
        reg=2.319952,
        n_components=130,
        epochs=2
    )
    # External IDs to map articles back to
    article_names = formatted_df.columns
    user_names = formatted_df.index

    vector_distance = distance.cdist(model.item_vectors, model.item_vectors, metric='cosine')
    vector_order = vector_distance.argsort()
    for i, order in enumerate(vector_order):
        source_article = article_names[i]
        # First entry is the article itself, so skip it
        for j in order[1:MAX_RECS]:
            recommended_article = article_names[j]

    logging.info(f"Successfully trained model on {len(article_df)} inputs.")
