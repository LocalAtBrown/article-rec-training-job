import logging
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
from sentence_transformers import SentenceTransformer
from sklearn.preprocessing import normalize

from db.mappings.model import ModelType
from job.strategies.templates.strategy import Strategy
from lib.config import config

# TODO: When adding functions to job.py in the future, maybe move these job.py and pass to respective functions as a
#  parameter?


class SemanticSimilarity(Strategy):
    """
    Semantic-Similarity site configs and methods.
    """

    def __init__(self):
        super().__init__(model_type=ModelType.SEMANTIC_SIMILARITY)

        # Other attributes
        self.article_data: Optional[List[Dict[str, str]]] = None
        self.interactions_data: Optional[pd.DataFrame] = None
        self.PRETRAINED_MODEL_NAME = config.get("SS_ENCODER")

    def fetch_data(self, interactions_data: pd.DataFrame = None):
        """
        Fetch data of all articles included in the interactions table.
        """
        self.interactions_data = interactions_data
        # Grab unique external IDs from Interactions table
        # https://stackoverflow.com/questions/46839277/series-unique-vs-list-of-set-performance
        external_ids: List[str] = list(set(interactions_data["external_id"]))

        # Fetch article text data using said IDs
        data = self.site.bulk_fetch_by_external_id(external_ids)

        # Validation: check if all external IDs have been fetched
        num_to_fetch = len(external_ids)
        num_fetched = len(data)
        if num_to_fetch != num_fetched:
            logging.warning(
                f"No. articles fetched ({num_fetched}) doesn't match no. articles" f" to fetch ({num_to_fetch})."
            )

        self.article_data = data

    def preprocess_data(self):
        """
        Preprocess fetched data into a DataFrame ready for training. Steps include:
        - Get a text representation for each article. The rule for creating such representations
        may differ between sites.
        - Merge fetched data with article metadata (LNL DB article ID, etc.)
        """
        # Create text representation of each article
        data = [
            {"external_id": article["external_id"], "text": self.site.get_article_text(article)}
            for article in self.article_data
        ]
        df_data = pd.DataFrame(data, dtype=str)

        # Extract article metadata (LNL DB article ID, categorial item ID, publish date) from interactions_data
        df_metadata = self.interactions_data[["article_id", "external_id", "published_at"]].dropna().drop_duplicates()

        # Merge article metadata with article text representations; the result is a DataFrame with 4 columns:
        # article_id, external_id, published_at and text. TODO: The schema for df will only include these 4 columns,
        #  which means we would benefit from type-enforcing it, maybe with something like pandera (
        #  https://stackoverflow.com/questions/61386477/type-hints-for-a-pandas-dataframe-with-mixed-dtypes)?
        df = df_metadata.merge(df_data, how="inner", on="external_id")

        self.train_data = df

    def generate_embeddings(self) -> None:
        """
        Given article data DataFrame with a text column, create article-level text embeddings.
        """
        texts = self.train_data["text"].tolist()
        model = SentenceTransformer(self.PRETRAINED_MODEL_NAME, device="cpu")

        self.train_embeddings = normalize(model.encode(texts, convert_to_numpy=True), axis=1, norm="l2")
        # TODO: Not doing time decay on SS for time being
        self.decays = np.ones(self.train_embeddings.shape[0])
