import logging
import time
from typing import List
import numpy as np
from sklearn.neighbors import NearestNeighbors
import torch

from db.helpers import create_rec
from db.mappings.recommendation import Rec
from lib.config import config
from lib.metrics import write_metric, Unit

MAX_RECS = config.get("MAX_RECS")

def get_model_embeddings(model, spotlight_ids:np.ndarray):
    """ Get embeddings from Spotlight model for all spotlight_ids"""
    return np.array([model._net.item_embeddings(torch.tensor([i], dtype=torch.int32)).tolist()[0] for i in spotlight_ids])


def get_similarities(embeddings:np.ndarray, n_recs:int):
    """ Get K nearest neighbors for each article"""
    nbrs = NearestNeighbors(n_neighbors=n_recs, 
                            metric='cosine',
                            algorithm='brute').fit(embeddings) 

    return nbrs.kneighbors(embeddings)

def get_nearest(spotlight_id:int, nearest_indices:np.ndarray, distances:np.ndarray, article_ids:np.ndarray):
    """ Map the K nearest neighbors indexes to the map LNL DB article_id, also get the distances """
    return (article_ids[nearest_indices[spotlight_id - 1][1:]], distances[spotlight_id - 1][1:])

def save_predictions(model, model_id:int, 
                    spotlight_ids:np.ndarray, 
                    external_item_ids:np.ndarray, 
                    article_ids:np.ndarray):
    """Save predictions to the db
    
    :model: Spotlight model  
    :model_id: unique id of model 
    :spotlight_ids: Spotlight 1-indexed item IDs
    :external_item_ids: Unique article ID from publisher
    :article_ids: DB article_ids
    """
    start_ts = time.time()
    embeddings = get_model_embeddings(model, spotlight_ids)
    distances, nearest_indices = get_similarities(embeddings, MAX_RECS + 1)
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
