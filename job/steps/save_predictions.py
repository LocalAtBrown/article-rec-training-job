import logging
import time
from typing import List

import numpy as np

from db.helpers import create_rec
from db.mappings.recommendation import Rec
from db.mappings.base import db_proxy
from lib.config import config
from lib.metrics import write_metric, Unit
from job.steps.knn import KNN

MAX_RECS = config.get("MAX_RECS")

def map_nearest(spotlight_id:int, nearest_indices:np.ndarray, distances:np.ndarray, article_ids:np.ndarray) -> (np.ndarray, np.ndarray):
    """ Map the K nearest neighbors indexes to the map LNL DB article_id, also get the distances """
    return (article_ids[nearest_indices[spotlight_id][1:]], distances[spotlight_id][1:])

def save_predictions(embeddings:np.ndarray, model_id:int, 
                    spotlight_ids:np.ndarray, 
                    external_item_ids:np.ndarray, 
                    article_ids:np.ndarray,
                    date_decays:np.ndarray) -> None:
    """Save predictions to the db
    
    :embeddings: article embeddings  
    :model_id: unique id of model 
    :spotlight_ids: Spotlight 0-indexed item IDs
    :exteral_item_ids: Unique article ID from publisher
    :article_ids: DB article_ids
    :date_decays: decay factors for each article [0,1]
    """
    logging.info(f"Finding nearest neighbors")
    start_ts = time.time()
    knn_index = KNN(embeddings)
    knn_index.decay_embeddings(date_decays)
    distances, nearest_indices = knn_index.get_similar_indices(MAX_RECS + 1)
    knn_latency = time.time() - start_ts
    logging.info(f"Total latency to find K-Nearest Neighbors: {knn_latency}")
    to_save = []

    for i in spotlight_ids:
        source_external_id = external_item_ids[i]
        recommendations = map_nearest(i, nearest_indices, distances, article_ids)

        to_save += [Rec(source_entity_id=source_external_id,
                            model_id=model_id,
                            recommended_article_id=recommended_item_id,
                            score= similarities) for (recommended_item_id, similarities) in zip(*recommendations)]

        if len(to_save) % 1000 == 0:
            logging.info(f"Created {len(to_save)} recommendations...")
    
    db_proxy.close() 
    db_proxy.connect()
    Rec.bulk_create(to_save, batch_size=50)
    latency = time.time() - start_ts
    write_metric("rec_creation_time", latency, unit=Unit.SECONDS)
    write_metric("rec_creation_total", len(to_save))
