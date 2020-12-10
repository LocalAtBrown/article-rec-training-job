import logging

import datadog

from lib.config import config


try:
    write_metrics = config.get("WRITE_METRICS")
except TypeError:
    write_metrics = True


def serialize_tags(tags):
    """serialize a dict of tags into a list of colon separated strings.

    this is the format for the datadog client's key-value tags.
    """
    return ["{}:{}".format(k, v) for k, v in tags.items() if v is not None]


class Metrics(datadog.DogStatsd):
    """Statsd client with a better tagging interface.

    Supports passing tags as a list of colon separated strings (this is the
    Datadog client's expected format), while also suporting tags passed as a
    dictionary.

    If write_metrics is False, metrics will only be logged.

    Usage:

        # tags as a dictionary...

            metrics = Metrics(
                namespace='foo',
                host='localhost',
                constant_tags={'foo': 'bar'},
            )
            metrics.incr('foo', tags={'baz': 'qux'})

        # tags as a list...

            metrics = Metrics(
                namespace='foo',
                host='localhost',
                constant_tags=['foo:bar'},
            )
            metrics.incr('foo', tags=['baz:qux'])
    """

    def __init__(self, *args, **kwargs):
        self.write_metrics = kwargs.pop("write_metrics", True)
        self.incr = self.increment
        self.decr = self.decrement

        constant_tags = kwargs.pop("constant_tags", {})
        if isinstance(constant_tags, dict):
            constant_tags = serialize_tags(constant_tags)

        super(Metrics, self).__init__(constant_tags=constant_tags, *args, **kwargs)

    def _report(self, metric, metric_type, value, tags, sample_rate):
        if isinstance(tags, dict):
            tags = serialize_tags(tags)

        tags.extend(self.constant_tags) if tags else self.constant_tags

        msg = "metric {metric}:{value} tags={tags}"
        logging.debug(msg.format(msg, metric=metric, value=value, tags=tags))

        if not self.write_metrics:
            return

        super(Metrics, self)._report(metric, metric_type, value, tags=tags, sample_rate=sample_rate)


metrics = Metrics(
    host="localhost",
    port=8125,
    write_metrics=write_metrics,
    constant_tags={"service": config.get("SERVICE"), "stage": config.get("STAGE")},
)
