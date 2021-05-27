import logging
import time

import numpy as np
from progressbar import ProgressBar
from scipy import sparse as sparse
from scipy.sparse.linalg import spsolve

from lib.config import config
from lib.metrics import write_metric, Unit


DISPLAY_PROGRESS = config.get("DISPLAY_PROGRESS")


class ImplicitMF:
    """
    Python implementation of implicit matrix factorization as outlined in
    "Collaborative Filtering for Implicit Feedback Datasets."

    Source: https://github.com/MrChrisJohnson/implicit-mf
    Paper: http://yifanhu.net/PUB/cf.pdf
    """

    def __init__(self, counts, num_factors=40, num_iterations=30, reg_param=0.8):
        self.counts = counts
        self.num_users = counts.shape[0]
        self.num_items = counts.shape[1]
        self.num_factors = num_factors
        self.num_iterations = num_iterations
        self.reg_param = reg_param

    def train_model(self):
        start_ts = time.time()
        self.user_vectors = np.random.normal(size=(self.num_users, self.num_factors))
        self.item_vectors = np.random.normal(size=(self.num_items, self.num_factors))

        for i in range(self.num_iterations):
            t0 = time.time()
            logging.info("Solving for user vectors...")
            self.user_vectors = self.iteration(
                True, sparse.csr_matrix(self.item_vectors)
            )
            logging.info("Solving for item vectors...")
            self.item_vectors = self.iteration(
                False, sparse.csr_matrix(self.user_vectors)
            )
            t1 = time.time()
            logging.info("iteration %i finished in %f seconds" % (i + 1, t1 - t0))

        latency = time.time() - start_ts
        write_metric("model_training_time", latency, unit=Unit.SECONDS)

    def iteration(self, user, fixed_vecs):
        num_solve = self.num_users if user else self.num_items
        num_fixed = fixed_vecs.shape[0]
        YTY = fixed_vecs.T.dot(fixed_vecs)
        eye = sparse.eye(num_fixed)
        lambda_eye = self.reg_param * sparse.eye(self.num_factors)
        solve_vecs = np.zeros((num_solve, self.num_factors))

        t = time.time()
        bar = ProgressBar(max_value=num_solve)
        for i in range(num_solve):
            if DISPLAY_PROGRESS:
                bar.update(i)
            if user:
                counts_i = self.counts[i].toarray()
            else:
                counts_i = self.counts[:, i].T.toarray()
            CuI = sparse.diags(counts_i, [0])
            pu = counts_i.copy()
            pu[np.where(pu != 0)] = 1.0
            YTCuIY = fixed_vecs.T.dot(CuI).dot(fixed_vecs)
            YTCupu = fixed_vecs.T.dot(CuI + eye).dot(sparse.csr_matrix(pu).T)
            xu = spsolve(YTY + YTCuIY + lambda_eye, YTCupu)
            solve_vecs[i] = xu
            if i % 10000 == 0:
                logging.info("Solved %i vecs in %d seconds" % (i, time.time() - t))
                t = time.time()

        return solve_vecs