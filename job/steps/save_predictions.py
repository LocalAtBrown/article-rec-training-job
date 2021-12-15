import logging
import time
from typing import List

import numpy as np
from scipy.spatial import distance
from sklearn.preprocessing import normalize
import torch
from spotlight.factorization.implicit import ImplicitFactorizationModel

from db.helpers import create_rec
from db.mappings.recommendation import Rec
from lib.config import config
from lib.metrics import write_metric, Unit

MAX_RECS = config.get("MAX_RECS")

def normalize_embeddings(embedding_matrix:np.ndarray) -> np.ndarray:
    """l2 normalize all embeddings along row dimension of matrix"""
    return normalize(embedding_matrix, axis=1, norm='l2')


def get_model_embeddings(model, spotlight_ids:np.ndarray) -> np.ndarray:
    """ Get l2 normalized embeddings from Spotlight model for all spotlight_ids"""
    return normalize_embeddings(np.array([model._net.item_embeddings(torch.tensor([i], dtype=torch.int32)).tolist()[0] for i in spotlight_ids]))

def get_cosines(embeddings:np.ndarray):
    """get [0,1] normalized cosine similarity for all vectors"""
    similarities = distance.cdist(
        embeddings, embeddings, metric="cosine")
    return ((((similarities - 1) * -1) + 1) / 2)

def get_similarities(embeddings:np.ndarray, date_decays:np.ndarray, n_recs:int):
    """ Get most similar articles"""
    similarities = get_cosines(embeddings)
    decayed_matrix = apply_decay(similarities, date_decays)
    return get_similar_indices(decayed_matrix, n_recs)

def apply_decay(embedding_matrix, decays_by_index):
    """ multiply every column by its corresponding decay weight. set the diagonal equal to 1. every row's index corresponds to that recommendation."""
    decayed_matrix = embedding_matrix * decays_by_index.reshape((1,decays_by_index.size))
    np.fill_diagonal(decayed_matrix, 1.0)
    return decayed_matrix

def get_similar_indices(decayed_matrix, n_recs):
    """ Get the nearest n_rec indices; get the corresponding weights"""
    sorted_recs = decayed_matrix.argsort()[:, ::-1][:, :n_recs]
    return (np.array([decayed_matrix[i][v] for i,v in enumerate(sorted_recs)]), sorted_recs)

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
    logging.info(f"Total latency to find K-Nearest Neighbors: {knn_latency}")
    to_save = []

    for i in spotlight_ids:
        source_external_id = external_item_ids[i - 1]
        recommendations = get_nearest(i, nearest_indices, distances, article_ids)

        to_save += [Rec(source_entity_id=source_external_id,
                            model_id=model_id,
                            recommended_article_id=recommended_item_id,
                            score= similarities) for (recommended_item_id, similarities) in zip(*recommendations)]

        if len(to_save) % 1000 == 0:
            logging.info(f"Created {len(to_save)} recommendations...")

    Rec.bulk_create(to_save, batch_size=50)
    latency = time.time() - start_ts
    write_metric("rec_creation_time", latency, unit=Unit.SECONDS)
    write_metric("rec_creation_total", len(to_save))
