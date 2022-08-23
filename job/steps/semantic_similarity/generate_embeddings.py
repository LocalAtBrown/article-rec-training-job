import numpy as np
import pandas as pd
from sentence_transformers import SentenceTransformer


def run(article_data: pd.DataFrame, pretrained_model_name: str) -> np.ndarray:
    """
    Given article data DataFrame with a text column, create article-level text embeddings.
    """
    texts = article_data["text"].tolist()
    model = SentenceTransformer(pretrained_model_name, device="cpu")

    return model.encode(texts, convert_to_numpy=True)
