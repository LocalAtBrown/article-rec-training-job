import logging
import time
from typing import List

from db.helpers import create_model, refresh_db, set_current_model
from db.mappings.model import ModelType
from db.mappings.recommendation import Rec
from job.helpers import batch
from lib.metrics import Unit, write_metric
from sites.site import Site


@refresh_db
def save_predictions(
    site: Site,
    recs: List[Rec],
) -> None:
    """
    Save predictions to the db
    """
    start_ts = time.time()
    # Create new model object in DB
    model_id = create_model(type=ModelType.ARTICLE.value, site=site.name)
    logging.info(f"Created model with id {model_id}")
    for rec in recs:
        rec.model_id = model_id

    logging.info(f"Writing {len(recs)} recommendations...")
    # Insert a small delay to avoid overwhelming the DB
    for rec_batch in batch(recs, n=50):
        Rec.bulk_create(rec_batch)
        time.sleep(0.05)

    logging.info(f"Updating model objects in DB")
    set_current_model(model_id, ModelType.ARTICLE, site.name)

    latency = time.time() - start_ts
    write_metric("rec_creation_time", latency, unit=Unit.SECONDS)
    write_metric("rec_creation_total", len(recs))
