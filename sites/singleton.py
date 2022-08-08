class Singleton(type):
    """
    A singleton definition to be used as a metaclass.
    A singleton pattern as defined here is where a class only ever has ONE instantiation of it.
    Here is a helpful wikipedia link: https://en.wikipedia.org/wiki/Singleton_pattern
    And stackoverflow: https://stackoverflow.com/questions/6760685/creating-a-singleton-in-python
    """

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]
