import logging
from abc import ABCMeta


class SingletonABCMeta(ABCMeta):
    """
    A singleton definition to be used as a metaclass.
    A singleton pattern as defined here is where a class only ever has ONE instantiation of it.
    Here is a helpful wikipedia link: https://en.wikipedia.org/wiki/Singleton_pattern
    And stackoverflow: https://stackoverflow.com/questions/6760685/creating-a-singleton-in-python
    Inheriting from `ABCMeta` ensures behavior of both a singleton, defined here, and an abstract base
    class, defined by `ABCMeta`.

    For the reason that we decided to use the singleton pattern here, visit
    https://github.com/LocalAtBrown/article-rec-training-job/pull/149#discussion_r968885680
    """

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(SingletonABCMeta, cls).__call__(*args, **kwargs)
        else:
            logging.warning(f"You tried to instantiate an instance of {cls.__name__} but one already exists.")
        return cls._instances[cls]
