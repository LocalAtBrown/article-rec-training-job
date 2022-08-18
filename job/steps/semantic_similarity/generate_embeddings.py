from typing import Any, Dict, List

import numpy as np
from sentence_transformers import SentenceTransformer

from sites.site import Site


def get_article_text_representations(site: Site, data: List[Dict[str, Any]]) -> List[str]:
    return [site.get_article_text(article) for article in data]


def create_model(pretrained_model_name: str) -> SentenceTransformer:
    return SentenceTransformer(pretrained_model_name, device="cpu")


def run(site: Site, data: List[Dict[str, Any]], pretrained_model_name: str) -> np.ndarray:
    """
    Given fetched article data with at least one text field, create article-level text embeddings.
    """
    texts = get_article_text_representations(site, data)
    model = create_model(pretrained_model_name, convert_to_numpy=True)

    return model.encode(texts)
