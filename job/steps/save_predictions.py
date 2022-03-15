from datetime import datetime, timezone, timedelta
import logging
import time
from typing import List
from job.helpers import batch

from db.helpers import (
    create_model,
    refresh_db,
    set_current_model,
    delete_models,
)
from db.mappings.model import Model, Status, Type
from db.mappings.recommendation import Rec
from lib.metrics import write_metric, Unit
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
    model_id = create_model(type=Type.ARTICLE.value, site=site.name)
    logging.info(f"Created model with id {model_id}")
    for rec in recs:
        rec.model_id = model_id

    # Insert a small delay to avoid overwhelming the DB
    for rec_batch in batch(recs, n=50):
        Rec.bulk_create(rec_batch)
        time.sleep(0.05)

    # Update model objects in DB
    set_current_model(model_id, Type.ARTICLE.value, site.name)
    delete_old_models()

    latency = time.time() - start_ts
    write_metric("rec_creation_time", latency, unit=Unit.SECONDS)
    write_metric("rec_creation_total", len(recs))


def delete_old_models() -> None:
    TTL_DAYS = 14
    ttl_days_ago = datetime.now(timezone.utc) - timedelta(days=TTL_DAYS)

    query = Model.select().where(
        (Model.created_at < ttl_days_ago) & (Model.status != Status.CURRENT.value)
    )
    model_ids = [x.id for x in query]

    BATCH_SIZE = 2
    logging.info(
        f"found {len(model_ids)} models to delete, deleting a max of {BATCH_SIZE}..."
    )

    # prevent a large deletion that may slow other queries
    if len(model_ids) > BATCH_SIZE:
        model_ids = model_ids[:BATCH_SIZE]

    # associated recommendations will also be deleted
    delete_models(model_ids)

    logging.info(f"deleted model_ids: {model_ids}")
