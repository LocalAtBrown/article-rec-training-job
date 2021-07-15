from datetime import datetime, timezone, timedelta
import logging

from db.mappings.model import Model, Status


def delete_old_models() -> None:
    # delete models older than one month
    TTL_DAYS = 30
    ttl_days_ago = datetime.now(timezone.utc) - timedelta(days=TTL_DAYS)

    query = Model.select().where(
        (Model.created_at < ttl_days_ago) & (Model.status != Status.CURRENT.value)
    )
    model_ids = [x.id for x in query]

    BATCH_SIZE = 10
    logging.info(
        f"found {len(model_ids)} models to delete, deleting a max of {BATCH_SIZE}..."
    )

    # prevent a large deletion that may slow other queries
    if len(model_ids) > BATCH_SIZE:
        model_ids = model_ids[:BATCH_SIZE]

    # associated recommendations will also be deleted
    dq = Model.delete().where(Model.id.in_(model_ids))
    dq.execute()
    logging.info(f"deleted model_ids: {model_ids}")
