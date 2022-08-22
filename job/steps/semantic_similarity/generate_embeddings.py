from typing import Any, Dict, List

import numpy as np
from sentence_transformers import SentenceTransformer

from sites.site import Site


def run(site: Site, data: List[Dict[str, Any]], pretrained_model_name: str) -> np.ndarray:
    """
    Given fetched article data with at least one text field, create article-level text embeddings.
    """
    texts = [site.get_article_text(article) for article in data]
    model = SentenceTransformer(pretrained_model_name, device="cpu")

    return model.encode(texts, convert_to_numpy=True)
