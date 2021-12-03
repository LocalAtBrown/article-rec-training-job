import logging
import time
from typing import List

from db.helpers import create_rec
from lib.config import config
from lib.metrics import write_metric, Unit

from sklearn.neighbors import NearestNeighbors
import torch

MAX_RECS = config.get("MAX_RECS")

def get_nearest(i, indices, distances, article_ids):
    return (article_ids[indices[i - 1][1:]], distances[i - 1][1:])

def save_predictions(model, model_id, internal_ids, external_item_ids, article_ids):
    """
    """
    # get the embedding for each article
    
    start_ts = time.time()
    embeddings = np.array([model._net.item_embeddings(torch.tensor([i], dtype=torch.int32)).tolist()[0] for i in internal_ids])
    nbrs = NearestNeighbors(n_neighbors=MAX_RECS + 1, 
                            metric='cosine',
                            algorithm='brute').fit(embeddings) 
    distances, indices = nbrs.kneigbors(embeddings)

    to_save = []

    for i in internal_ids:
        source_db_external_id = external_item_ids[i - 1]
        recommendations = get_nearest(i, indices, distances, article_ids)

        to_save += [Rec(source_entity_id=source_db_external_id,
                            model_id=model_id,
                            recommended_article_id=recommended_item_id,
                            score= 1 - score) for (recommended_item_id, score) in zip(*recommendations)]

        if len(to_save) % 1000 == 0:
            logging.info(f"Created {created_recs} recommendations...")

    Rec.bulk_create(to_save, batch_size=50)
    latency = time.time() - start_ts
    write_metric("rec_creation_time", latency, unit=Unit.SECONDS)
    write_metric("rec_creation_total", created_recs)
