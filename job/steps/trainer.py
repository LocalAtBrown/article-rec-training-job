import logging
import time
from copy import deepcopy
from typing import Callable, Optional

import numpy as np
import pandas as pd
import torch
from sklearn.preprocessing import normalize
from spotlight.cross_validation import random_train_test_split
from spotlight.evaluation import mrr_score
from spotlight.factorization.explicit import ExplicitFactorizationModel
from spotlight.factorization.implicit import ImplicitFactorizationModel
from spotlight.interactions import Interactions

from job.helpers import decay_fn
from lib.metrics import Unit, write_metric


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
