from article_rec_training_job.shared.helpers.itertools import batched


def test_batched():
    assert list(batched([1, 2, 3, 4, 5], 2)) == [(1, 2), (3, 4), (5,)]
