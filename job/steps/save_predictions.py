import logging
import time
from typing import List

import numpy as np
from sklearn.neighbors import NearestNeighbors
from sklearn.preprocessing import normalize
import torch
from spotlight.factorization.implicit import ImplicitFactorizationModel

from db.helpers import create_rec
from db.mappings.recommendation import Rec
from lib.config import config
from lib.metrics import write_metric, Unit

MAX_RECS = config.get("MAX_RECS")

def normalize_embeddings(embedding_matrix:np.ndarray) -> np.ndarray:
    """l1 normalize all embeddings along row dimension of matrix"""
    return normalize(embedding_matrix, axis=1, norm='l1')


def get_model_embeddings(model, spotlight_ids:np.ndarray) -> np.ndarray:
    """ Get l1 normalized embeddings from Spotlight model for all spotlight_ids"""
    return normalize_embeddings(np.array([model._net.item_embeddings(torch.tensor([i], dtype=torch.int32)).tolist()[0] for i in spotlight_ids]))

def weighted_cosine(a:np.ndarray, b:np.ndarray) -> float:
    # dot product of a and b (cosine similarity) scaled by the time_decay of b
    # note: embeddings are already l1 normalized (not including decay constant in last index)
    # :a embedding vector for article. last entry is decay constant.
    # :b embedding vector for article. last entry is decay constant.
    # :return [0,1] distance score, where 0 is closer
    if np.array_equal(a, b): return 0
    decay_constant = b[-1]
    return  (1 - (decay_constant * (a[:-1] @ b[:-1])))

def get_similarities(embeddings:np.ndarray, date_decays:np.ndarray, n_recs:int) -> (np.ndarray, np.ndarray):
    """ Get K nearest neighbors for each article"""
    weighted_embeddings = np.hstack([embeddings, np.expand_dims(date_decays, axis=1)]) 
    nbrs = NearestNeighbors(n_neighbors=n_recs, 
                            metric=weighted_cosine,
                            algorithm='brute').fit(weighted_embeddings) 

    return nbrs.kneighbors(weighted_embeddings)

def get_nearest(spotlight_id:int, nearest_indices:np.ndarray, distances:np.ndarray, article_ids:np.ndarray) -> (np.ndarray, np.ndarray):
    """ Map the K nearest neighbors indexes to the map LNL DB article_id, also get the distances """
    return (article_ids[nearest_indices[spotlight_id - 1][1:]], distances[spotlight_id - 1][1:])

def save_predictions(model:ImplicitFactorizationModel, model_id:int, 
                    spotlight_ids:np.ndarray, 
                    external_item_ids:np.ndarray, 
                    article_ids:np.ndarray,
                    date_decays:np.ndarray) -> None:
    """Save predictions to the db
    
    :model: Spotlight model  
    :model_id: unique id of model 
    :spotlight_ids: Spotlight 1-indexed item IDs
    :external_item_ids: Unique article ID from publisher
    :article_ids: DB article_ids
    :date_decays: decay factors for each article [0,1]
    """
    start_ts = time.time()
    embeddings = get_model_embeddings(model, spotlight_ids)
    distances, nearest_indices = get_similarities(embeddings, date_decays, MAX_RECS + 1)
    knn_latency = time.time() - start_ts
    logging.info(f"Total latency for KNN: {knn_latency}")
    to_save = []

    for i in spotlight_ids:
        source_external_id = external_item_ids[i - 1]
        recommendations = get_nearest(i, nearest_indices, distances, article_ids)

        to_save += [Rec(source_entity_id=source_external_id,
                            model_id=model_id,
                            recommended_article_id=recommended_item_id,
                            score= 1 - distance) for (recommended_item_id, distance) in zip(*recommendations)]

        if len(to_save) % 1000 == 0:
            logging.info(f"Created {len(to_save)} recommendations...")

    Rec.bulk_create(to_save, batch_size=50)
    latency = time.time() - start_ts
    write_metric("rec_creation_time", latency, unit=Unit.SECONDS)
    write_metric("rec_creation_total", len(to_save))
