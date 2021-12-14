import logging
import time
from copy import deepcopy

import numpy as np
import pandas as pd
from typing import Optional, Callable
from sklearn.preprocessing import normalize
import torch
from spotlight.factorization.implicit import ImplicitFactorizationModel
from spotlight.sequence.implicit import ImplicitSequenceModel
from spotlight.interactions import Interactions
from spotlight.cross_validation import random_train_test_split
from spotlight.evaluation import mrr_score

from lib.metrics import write_metric, Unit
from job.helpers import decay_fn

DEFAULT_PARAMS = {
        "hl": 10,
        "epochs": 2,
        "embedding_dim": 350,
        "model": "IMF",
        "tune": False
        }

class Trainer:
    def __init__(self, warehouse_df:pd.DataFrame,
                        experiment_time:pd.datetime, 
                        warehouse_transform:Optional[Callable],
                        params:Optional[dict]=None
                        ):
        """
            params and hparams should be separated out in the future
        """
        super().__init__()
        self.params = self._update_params(params)
        self.experiment_time = pd.to_datetime(experiment_time)

        if warehouse_transform:
            warehouse_df = warehouse_transform(warehouse_df)

        self.spotlight_dataset = self._generate_interactions(warehouse_df)
        self.dates_df = self._generate_datedecays(warehouse_df)

        self._generate_model()
   
    def _update_params(self, params:None) -> dict:
        _params = deepcopy(DEFAULT_PARAMS)
        _params.update(params)
        return _params

    def _generate_datedecays(self, prepared_df:pd.DataFrame)-> pd.DataFrame:
        """ Build columns with date decay and external id""" 
        dates_df = prepared_df[["published_at", "external_id", "item_id", "article_id"]].drop_duplicates()

        dates_df["date_decays"] = decay_fn(self.experiment_time, dates_df["published_at"], self.params["hl"]) 
        return dates_df

    def _generate_interactions(self, warehouse_df:pd.DataFrame)-> Interactions:
        """ Generate an Interactions object"""
        return Interactions(user_ids=warehouse_df['user_id'].values,
                           item_ids=warehouse_df['item_id'].values,
                           ratings=warehouse_df['duration'].values,
                           timestamps=warehouse_df['timestamp'].values)

    def _generate_model(self)-> None:
        if self.params["model"] == "IMF":
            self.model = ImplicitFactorizationModel(n_iter=self.params["epochs"], 
                                                embedding_dim=self.params["embedding_dim"])
        elif self.params["model"] == "Sequential":
            self.model = ImplicitSequenceModel(n_iter=self.params["epochs"],
                                                embedding_dim=self.params["embedding_dim"])
    
    def _normalize_embeddings(self, embedding_matrix:np.ndarray) -> np.ndarray:
        """l2 normalize all embeddings along row dimension of matrix"""
        return normalize(embedding_matrix, axis=1, norm='l2')

    def _generate_normalized_embeddings(self) -> None:
        """ Get l2 normalized embeddings from Spotlight model for all spotlight_ids"""
        spotlight_ids = self.dates_df["item_id"].values
        return self._normalize_embeddings(np.array([self.model._net.item_embeddings(torch.tensor([i], dtype=torch.int32)).tolist()[0] for i in spotlight_ids]))

    def _fit(self, training_dataset:Interactions) -> None:
        self.model.fit(training_dataset, verbose=True)

    def _tune(self) -> None:
        train, test = random_train_test_split(self.spotlight_dataset) 
        best_mrr = -float('inf')
        best_params = deepcopy(self.params) 

        for tune_val in range(self.params[""]):
            self.params[self.params["tune_parameter"]] = tune_val
            self._fit(train)
            mrr_val = mrr_score(test) 
            # write metric

            if mrr_val > best_mrr:
                best_mrr = mrr_val
                best_params = deepcopy(self.params)
                # write metric
        
        # write findings
        self._update_params(best_params)
        self._fit(self.spotlight_dataset)


    def fit(self) -> None:
        start_ts = time.time()
        if self.params["tune"]:
            self._tune() 
        else:
            self._fit(self.spotlight_dataset)
        latency = time.time() - start_ts
        write_metric("model_training_time", latency, unit=Unit.SECONDS)

    def get_dates_df(self) -> pd.DataFrame:
        return self.dates_df

    def get_embeddings(self) -> np.ndarray:
        return self._generate_normalized_embeddings()
