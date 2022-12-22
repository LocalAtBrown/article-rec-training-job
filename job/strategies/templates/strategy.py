from __future__ import annotations

import logging
import time
from abc import ABCMeta, abstractmethod
from datetime import datetime
from typing import TYPE_CHECKING, List, Optional

import numpy as np
import pandas as pd

from db.helpers import create_model, refresh_db, set_current_model
from db.mappings.model import ModelType
from db.mappings.recommendation import Rec
from job.helpers.itertools import batch
from job.helpers.knn import KNN, map_neighbors_to_recommendations
from lib.config import config

if TYPE_CHECKING:
    from sites.templates.site import Site


class Strategy(metaclass=ABCMeta):
    """
    Superclass that defines the methods for each one of the recommendation strategies (collaborative filtering,
    popularity, semantic similarity).
    """

    MAX_RECS = config.get("MAX_RECS")

    @abstractmethod
    def __init__(self, model_type: ModelType) -> None:
        """
        Constructor that needs to be defined by subclasses
        """
        self.model_type = model_type
        self.site: Optional[Site] = None
        self.experiment_time: datetime = None

        # Attributes to be defined in subclasses; listing them here to keep track
        self.decays: Optional[np.ndarray] = None
        self.train_embeddings: Optional[np.ndarray] = None
        self.train_data: Optional[pd.DataFrame] = None
        self.recommendations: Optional[List[Rec]] = None

    def set_experiment_time(self, experiment_time: datetime) -> None:
        """
        Set experiment time post-init but pre-fetch_data and other methods
        """
        self.experiment_time = experiment_time

    def set_site(self, site: Site) -> None:
        """
        Set site post-init but pre-fetch_data and other methods
        """
        self.site = site

    def prepare(self, site: Site, experiment_time: datetime) -> None:
        """
        Prepare instance post-init but pre-fetch_data and other methods
        """
        self.set_site(site)
        self.set_experiment_time(experiment_time)

    @abstractmethod
    def fetch_data(self, interactions_data: pd.DataFrame = None) -> None:
        """
        Fetch data from the data warehouse
        """
        pass

    @abstractmethod
    def preprocess_data(self) -> None:
        """
        Preprocess fetched data into a DataFrame ready for training.
        """
        pass

    @abstractmethod
    def generate_embeddings(self) -> None:
        """
        Given DataFrame with training data, create embeddings.
        """
        pass

    def generate_recommendations(self) -> None:
        """
        Run article-level embeddings through a KNN and create recs from resulting neighbors.
        """
        searcher = KNN(self.train_embeddings, self.decays)

        logging.info("Calculating KNN...")
        start_ts = time.time()

        # MAX_RECS + 1 because an article's top match is always itself
        similarities, nearest_indices = searcher.get_similar_indices(self.MAX_RECS + 1)

        logging.info(f"Total latency to find K-Nearest Neighbors: {time.time() - start_ts}")

        self.recommendations = map_neighbors_to_recommendations(
            model_item_ids=self.train_data["external_id"].factorize()[0],
            external_ids=self.train_data["external_id"].values,
            article_ids=self.train_data["article_id"].values,
            model_item_neighbor_indices=nearest_indices,
            similarities=similarities,
        )

    @refresh_db
    def save_recommendations(self) -> None:
        """
        Save generated recommendations to database.
        """
        start_ts = time.time()
        # Create new model object in DB
        model_id = create_model(type=self.model_type.value, site=self.site.name)
        logging.info(f"Created model with id {model_id}")
        for rec in self.recommendations:
            rec.model_id = model_id

        logging.info(f"Writing {len(self.recommendations)} recommendations...")
        # Insert a small delay to avoid overwhelming the DB
        for rec_batch in batch(self.recommendations, n=50):
            Rec.bulk_create(rec_batch)
            time.sleep(0.05)

        logging.info(f"Updating model objects in DB")
        set_current_model(model_id, self.model_type, self.site.name)

        latency = time.time() - start_ts
        # TODO: Unlike save_predictions in CF, not writing metrics to CloudWatch for now
        logging.info(f"rec_creation_time: {latency}s")
        logging.info(f"recs_creation_total: {len(self.recommendations)}")
