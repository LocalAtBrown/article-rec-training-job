import logging
import numpy as np
import scipy.sparse as sparse
import time

from scipy.sparse import csr_matrix
from sklearn.preprocessing import MinMaxScaler

from job.steps.implicit_mf import ImplicitMF
from lib.config import config
from lib.bucket import save_outputs

DISPLAY_PROGRESS = config.get("DISPLAY_PROGRESS")


def load_matrix(pageview_df, num_users, num_items):
    """
    #TODO: We should rewrite this without the iterrows and use it to load the df directly into sparse matrix format

    :param pageview_df:
    :param num_users:
    :param num_items:
    :return:
    """
    t0 = time.time()
    counts = sparse.dok_matrix((num_users, num_items), dtype=float)
    total = 0.0
    num_zeros = num_users * num_items
    users = {}
    items = {}
    for i, line in pageview_df.iterrows():
        user, item, count = line
        users[user] = len(users) - 1
        items[item] = len(items) - 1
        count = float(count)
        if len(users) >= num_users:
            continue
        if len(items) >= num_items:
            continue
        if count != 0:
            counts[users[user], items[item]] = count
            total += count
            num_zeros -= 1
        if i % 100000 == 0:
            logging.info("loaded %i counts..." % i)
    alpha = num_zeros / total
    logging.info("alpha %.2f" % alpha)
    counts *= alpha
    counts = counts.tocsr()
    t1 = time.time()
    logging.info("Finished loading matrix in %f seconds" % (t1 - t0))
    return counts, users


@save_outputs("model_item_vectors.npy")
def train_model(X: np.array, reg: float, n_components: int, epochs: int) -> ImplicitMF:
    """
    #TODO: If necessary, skip the numpy array conversion step, and load directly into sparse matrix.
    :param X:
    :param reg:
    :param n_components:
    :param epochs:
    :return:
    """
    X_log = np.log(1 + X)
    X_scaler = MinMaxScaler()
    X_scaled = X_scaler.fit_transform(X_log)

    # Hyperparameters derived using optimize_ga_pipeline.ipynb
    model = ImplicitMF(
        counts=csr_matrix(X_scaled),
        reg_param=reg,
        num_factors=n_components,
        num_iterations=epochs,
    )
    model.train_model()
    return model
