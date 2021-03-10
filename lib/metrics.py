from typing import Dict

import boto3

from lib.config import config, STAGE, REGION

client = boto3.client("cloudwatch", REGION)
SERVICE = config.get("SERVICE")


def write(
    name: str,
    value: float,
    unit: str = "Count",
    tags: Dict[str, str] = None,
) -> None:
    if STAGE == "local":
        logging.info("Skipping metric write")
        return
    default_tags = {"stage": STAGE}
    if tags:
        default_tags.update(tags)
    formatted_tags = [{"Name": k, "Value": v} for k, v in default_tags.items()]

    client.put_metric_data(
        Namespace=SERVICE,
        MetricData=[
            {
                "MetricName": name,
                "Dimensions": formatted_tags,
                "Value": value,
                "Unit": unit,
            },
        ],
    )
