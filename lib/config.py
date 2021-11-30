import os
import json
from pathlib import Path
import logging
from typing import Any

import boto3
from botocore.exceptions import NoCredentialsError

STAGE = os.environ["STAGE"]
REGION = os.getenv("REGION", "us-east-1")
ROOT_DIR = str(Path(__file__).parent.parent.resolve())
INPUT_FILEPATH = f"{ROOT_DIR}/env.json"
CLIENT = boto3.client("ssm", REGION)


class Config:
    def __init__(self):
        self._config = self.load_env()
        # override site name on dev and prod with env var specified in cdk
        self._config["SITE_NAME"] = os.getenv("SITE_NAME", self.get("SITE_NAME"))

    def get_secret(self, secret_key: str) -> Any:
        res = CLIENT.get_parameter(Name=secret_key, WithDecryption=True)
        val = res["Parameter"]["Value"]
        try:
            val = json.loads(val)
        except json.decoder.JSONDecodeError:
            # expect this error for strings
            pass
        return val

    def is_secret(self, val: str) -> bool:
        if not isinstance(val, str):
            return False

        if val.startswith("/prod") or val.startswith("/dev"):
            return True

    def get(self, var_name: str) -> Any:
        try:
            val = self._config[var_name]
        except KeyError:
            raise TypeError(f"Variable {var_name} not found in config")

        return val

    def load_env(self):
        """
        Loads the following parameters from "env.json":
        * `LOG_LEVEL` (str): log level above which logs will be displayed (_e.g. "INFO"_)
        * `SERVICE` (str):  name of the service ("article-rec-training-job")
        * `DB_NAME` (str): [AWS SSM](https://docs.aws.amazon.com/systems-manager/latest/userguide/ssm-agent.html) path to the database name secret (_e.g. "/dev/article-rec-db/name"_)
        * `DB_PASSWORD` (str): path to the database password secret (_e.g. "/dev/database/password"_)
        * `DB_USER` (str): path to the database user secret (_e.g. "/dev/database/user"_)
        * `DB_HOST` (str): path to the database host screw (_e.g. "/dev/database/host"_)
        * `SITE NAME` (str): default site name, override with environment variable SITE (_e.g. "washington-city-paper"_)
        * `SAVE_FIGURES` (boolean):  whether or not to output a figure at the filter users step (_e.g. false_)
        * `DISPLAY_PROGRESS` (boolean): whether or not to display training progress in logs (_e.g. false_)
        * `MAX_RECS` (int): How many recommendations to save to the database for each article (_e.g. 20_)
        * `DAYS_OF_DATA` (int): How many days worth of data to fetch and train on (_e.g. 28_)
        """
        with open(INPUT_FILEPATH) as json_file:
            env_vars = json.load(json_file)

        config = {}
        stage_env = env_vars.get("default", {})
        stage_env.update(env_vars[STAGE])

        for var_name, val in stage_env.items():
            if self.is_secret(val):
                try:
                    val = self.get_secret(val)
                except NoCredentialsError:
                    # alright if github action test workflow does not have aws credentials
                    logging.warning(
                        f"AWS credentials missing. Can't fetch secret: '{val}'"
                    )

            config[var_name] = val

        return config


config = Config()
