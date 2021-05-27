from functools import wraps

import numpy as np
import pandas as pd

from job.steps.train_model import ImplicitMF
from lib.bucket import upload_to_s3, ARTIFACT_BUCKET
from lib.config import ROOT_DIR


def save_outputs(filename):
    def save_outputs_decorator(func):
        @wraps(func)
        def save_outputs_wrapper(*args, **kwargs):
            filepath = f"{ROOT_DIR}/tmp/{filename}"
            result = func(*args, **kwargs)
            if type(result) == np.array:
                np.save(filepath, result.item_vectors)
                upload_to_s3(filepath, bucket=ARTIFACT_BUCKET)
            elif type(result) == pd.DataFrame:
                result.to_csv(filepath)
                upload_to_s3(filepath, bucket=ARTIFACT_BUCKET)
            elif type(result) == ImplicitMF:
                np.save(filepath, result.item_vectors)
                upload_to_s3(filepath, bucket=ARTIFACT_BUCKET)
            else:
                raise NotImplementedError
            return result
        return save_outputs_wrapper
    return save_outputs_decorator