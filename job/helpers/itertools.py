def batch(iterable, n=1):
    total_length = len(iterable)
    for ndx in range(0, total_length, n):
        yield iterable[ndx : min(ndx + n, total_length)]
