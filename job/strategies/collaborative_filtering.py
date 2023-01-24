import datetime
import logging
import time
from copy import deepcopy
from typing import Any, Callable, List, Optional, Set, Tuple, TypedDict

import numpy as np
import pandas as pd
import torch
from sklearn.preprocessing import normalize
from spotlight.cross_validation import random_train_test_split
from spotlight.evaluation import mrr_score
from spotlight.factorization.explicit import ExplicitFactorizationModel
from spotlight.factorization.implicit import ImplicitFactorizationModel
from spotlight.interactions import Interactions

from db.mappings.model import ModelType
from db.mappings.recommendation import Rec
from job.helpers.datetime import decay_fn
from job.helpers.knn import KNN
from job.strategies.templates.strategy import Strategy
from lib.config import config
from lib.metrics import Unit, write_metric

MAX_RECS = config.get("MAX_RECS")


def _spotlight_transform(prepared_df: pd.DataFrame, batch_size: int, random_seed: int, **kwargs: Any) -> pd.DataFrame:
    """Transform data for Spotlight
    :prepared_df: Dataframe with user-article interactions
    :return: (prepared_df)
    """
    prepared_df = prepared_df.dropna()

    # If DataFrame length divides batch_size with a remainder of 1, Spotlight's BilinearNet inside IMF will
    # throw an IndexError (see https://github.com/maciejkula/spotlight/issues/107) that in the past was
    # responsible for a few failed job runs (see https://github.com/LocalAtBrown/article-rec-training-job/pull/140).
    #
    # Until this Spotlight bug is fixed, as a short-term measure, we randomly remove 1 entry from the
    # DataFrame corresponding to the article with the highest view count so as to minimally affect performance.
    #
    # There's a also a test for this: test_article_recommendations_spotlight_batchsize() in tests/test_helpers.py
    num_interactions = prepared_df.shape[0]
    if num_interactions % batch_size == 1:
        # External ID of most read article
        id_article_most_read = prepared_df[["external_id"]].groupby("external_id").size().idxmax()
        # Randomly chooses an index among interactions involving most interacted article
        index_to_drop = np.random.default_rng(random_seed).choice(
            prepared_df[prepared_df["external_id"] == id_article_most_read].index, size=1, replace=False, shuffle=False
        )
        # Drop row with said index
        prepared_df = prepared_df.drop(index=index_to_drop)
        logging.warning(
            f"Found {num_interactions} reader-article interactions, which leaves a remainder of 1 when divided by a batch size of {batch_size} and would trigger a Spotlight bug. "
            + f"To prevent this, 1 random interaction corresponding to external ID {id_article_most_read} has been dropped."
        )

    prepared_df["published_at"] = pd.to_datetime(prepared_df["published_at"])
    prepared_df["session_date"] = pd.to_datetime(prepared_df["session_date"])
    prepared_df["session_date"] = prepared_df["session_date"].dt.date
    prepared_df["external_id"] = prepared_df["external_id"].astype("category")
    prepared_df["item_id"] = prepared_df["external_id"].cat.codes
    prepared_df["user_id"] = prepared_df["client_id"].factorize()[0]
    prepared_df["timestamp"] = prepared_df["session_date"].factorize()[0] + 1

    return prepared_df


def train_model(X: pd.DataFrame, params: dict, experiment_time: datetime.datetime) -> Tuple[np.ndarray, pd.DataFrame]:
    """Train spotlight model

    X: pandas dataframe of interactions
    params: Hyperparameters
    experiment_time: time to benchmark decay against

    return: (embeddings: Spotlight model embeddings for each unique article,
            dates_df: pd.DataFrame with columns:
                external_item_ids: Publisher unique article IDs,
                internal_ids: Spotlight article IDs,
                article_ids: LNL DB article IDs,
                date_decays: Decay factors for each article [0,1]
            )
    """
    model = Trainer(X, experiment_time, _spotlight_transform, params)
    model.fit()
    return model.model_embeddings, model.model_dates_df


def map_nearest(
    spotlight_id: int,
    nearest_indices: np.ndarray,
    distances: np.ndarray,
    article_ids: np.ndarray,
) -> Tuple[np.ndarray, np.ndarray]:
    """Map the K nearest neighbors indexes to the map LNL DB article_id, also get the distances"""
    return (article_ids[nearest_indices[spotlight_id][1:]], distances[spotlight_id][1:])


def get_recommendations(X: pd.DataFrame, params: dict, dt: datetime.datetime) -> List[Rec]:
    logging.info("Starting model training...")
    embeddings, df = train_model(X, params, dt)

    start_ts = time.time()

    logging.info("Calcuating KNN...")
    # Use KNN similarity to calculate score of each recommendation
    knn_index = KNN(embeddings, df["date_decays"].values)
    similarities, nearest_indices = knn_index.get_similar_indices(MAX_RECS + 1)

    knn_latency = time.time() - start_ts
    logging.info(f"Total latency to find K-Nearest Neighbors: {knn_latency}")

    spotlight_ids = df["item_id"].values
    external_item_ids = df["external_id"].values
    article_ids = df["article_id"].values
    recs = []

    for i in spotlight_ids:
        source_external_id = external_item_ids[i]
        recommendations = map_nearest(i, nearest_indices, similarities, article_ids)

        recs += [
            Rec(
                source_entity_id=source_external_id,
                recommended_article_id=recommended_item_id,
                score=similarity,
            )
            for (recommended_item_id, similarity) in zip(*recommendations)
        ]
    return recs


class ScrapeConfig(TypedDict):
    """
    Scraping config.
    """

    concurrent_requests: int
    requests_per_second: int


class TrainParamsCF(TypedDict):
    """
    Training params for collaborative filtering.
    """

    hl: int
    embedding_dim: int
    epochs: int
    tune: bool
    tune_params: List[str]
    tune_ranges: List[List[int]]
    model: str
    loss: str


class Trainer:
    def __init__(
        self,
        warehouse_df: pd.DataFrame,
        experiment_time: pd.datetime,
        warehouse_transform: Optional[Callable],
        params: Optional[dict] = None,
    ):
        """Trainer class to perform ML tasks supporting Spotlight model training
        :warehouse_df: Data from Redshift Warehouse to train on
        :experiment_time: Base timeframe to train against
        :warehouse_transform: Function to transform warehouse data into format for Spotlight Interactions data object
        :params: Dictionary of parameters and hyperparameters
        Note: params and hparams should be separated out in the future
        """
        super().__init__()

        self.params: dict = {
            "hl": 10,
            "epochs": 2,
            "embedding_dim": 350,
            "batch_size": 256,
            "model": "EMF",
            "tune": False,
            "random_seed": 42,
            "loss": "pointwise",
        }
        self._update_params({"random_state": np.random.RandomState(self.params["random_seed"])})
        self._update_params(params)
        self.experiment_time = pd.to_datetime(experiment_time)

        if warehouse_transform:
            warehouse_df = warehouse_transform(warehouse_df, **self.params)

        self.spotlight_dataset = self._generate_interactions(warehouse_df)
        self.dates_df = self._generate_datedecays(warehouse_df)

        self._generate_model()

    def _update_params(self, params: Optional[dict] = None) -> None:
        """Update the internal params"""
        if isinstance(params, dict):
            self.params.update(params)

    def _generate_datedecays(self, prepared_df: pd.DataFrame) -> pd.DataFrame:
        """Build columns with date decay and external id"""
        dates_df = prepared_df[["published_at", "external_id", "item_id", "article_id"]].drop_duplicates()

        dates_df["date_decays"] = decay_fn(self.experiment_time, dates_df["published_at"], self.params["hl"])
        return dates_df

    def _generate_interactions(self, warehouse_df: pd.DataFrame) -> Interactions:
        """Generate an Interactions object"""
        return Interactions(
            user_ids=warehouse_df["user_id"].values,
            item_ids=warehouse_df["item_id"].values,
            ratings=warehouse_df["duration"].values,
            timestamps=warehouse_df["timestamp"].values,
        )

    def _generate_model(self) -> None:
        """Initialize model of the Trainer"""
        if self.params["model"] == "IMF":
            self.model = ImplicitFactorizationModel(
                n_iter=self.params["epochs"],
                loss=self.params["loss"],
                random_state=self.params["random_state"],
                embedding_dim=self.params["embedding_dim"],
                batch_size=self.params["batch_size"],
            )
        elif self.params["model"] == "EMF":
            self.model = ExplicitFactorizationModel(
                n_iter=self.params["epochs"],
                loss=self.params["loss"],
                random_state=self.params["random_state"],
                embedding_dim=self.params["embedding_dim"],
                batch_size=self.params["batch_size"],
            )

    def _normalize_embeddings(self, embedding_matrix: np.ndarray) -> np.ndarray:
        """l2 normalize all embeddings along row dimension of matrix"""
        return normalize(embedding_matrix, axis=1, norm="l2")

    def _generate_normalized_embeddings(self) -> np.ndarray:
        """Get l2 normalized embeddings from Spotlight model for all spotlight_ids"""
        spotlight_ids = self.dates_df["item_id"].values
        return self._normalize_embeddings(
            np.array(
                [self.model._net.item_embeddings(torch.tensor([i], dtype=torch.int32)).tolist()[0] for i in spotlight_ids]
            )
        )

    def _fit(self, training_dataset: Interactions) -> None:
        """Fit the spotlight model to an Interactions dataset
        :training_dataset: Spotlight Interactions object"""
        self.model.fit(training_dataset, verbose=True)

    def _tune(self) -> None:
        """Perform grid seach over tune_params and tune_ranges lists
        tune_params: Hyperparameters to tune
        tune_ranges: Range of values to tune over. Third item in the list is the step

        Model will be evaluated by MRR on a test set.
        Best hyperparameters will be used to train the model
        """
        if "tune_params" not in self.params or "tune_ranges" not in self.params:
            logging.info("Tuning cannot be performed without range and parameter")
            return

        if len(self.params["tune_params"]) != len(self.params["tune_ranges"]):
            logging.info("Must have same number of parameters as ranges")
            return

        train, test = random_train_test_split(self.spotlight_dataset)
        best_mrr = -float("inf")
        best_params = deepcopy(self.params)
        logging.info(f"Starting hyperparameter tuning job on {self.params['tune_params']}")

        for i, tune_param in enumerate(self.params["tune_params"]):
            for tune_val in range(*self.params["tune_ranges"][i]):
                self.params[tune_param] = tune_val
                for j, tune_param2 in enumerate(self.params["tune_params"][i + 1 :], i + 1):
                    for tune_val2 in range(*self.params["tune_ranges"][j]):
                        self.params[tune_param2] = tune_val2

                        self._generate_model()
                        self._fit(train)
                        mrr_val = np.mean(mrr_score(self.model, test))
                        logging.info(f"Tested hyperparameters: {self.params} MRR: {mrr_val}")

                        if mrr_val > best_mrr:
                            best_mrr = mrr_val
                            best_params = deepcopy(self.params)

        logging.info(f"Final hyperparameters: {best_params} MRR: {best_mrr}")
        self._update_params(best_params)
        self._generate_model()
        self._fit(self.spotlight_dataset)

    def fit(self) -> None:
        """Fit a Spotlight model with or without grid tuning"""
        start_ts = time.time()
        if self.params["tune"]:
            self._tune()
        else:
            self._fit(self.spotlight_dataset)
        latency = time.time() - start_ts
        write_metric("model_training_time", latency, unit=Unit.SECONDS)

    @property
    def model_dates_df(self) -> pd.DataFrame:
        """Return dataframe with date decay values for each article"""
        return self.dates_df

    @property
    def model_embeddings(self) -> np.ndarray:
        """Return article embeddings"""
        return self._generate_normalized_embeddings()


class CollaborativeFiltering(Strategy):
    """
    Collaborative-filtering site configs and methods.
    """

    def __init__(
        self, snowplow_fields: Set[str], scrape_config: ScrapeConfig, training_params: TrainParamsCF, max_article_age
    ):
        super().__init__(model_type=ModelType.ARTICLE)

        # Parameters
        self.snowplow_fields: Set[str] = snowplow_fields
        self.scrape_config: ScrapeConfig = scrape_config
        self.training_params: TrainParamsCF = training_params
        # this is a number of years; will grab dwell time data for any article within the past X years
        self.max_article_age: int = max_article_age

    def fetch_data(self, interactions_data: pd.DataFrame = None):
        self.train_data = interactions_data

    def preprocess_data(self):
        pass

    def generate_embeddings(self):
        model = Trainer(self.train_data, self.experiment_time, _spotlight_transform, self.training_params)
        model.fit()
        self.train_embeddings = model.model_embeddings
        self.article_data = model.model_dates_df
