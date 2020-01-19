from itertools import groupby
from operator import attrgetter
import random
from typing import List, Iterable, Callable, Dict, Any


class classproperty:
    def __init__(self, method=None):
        self.fget = method

    def __get__(self, instance, cls=None):
        return self.fget(cls)

    def getter(self, method):
        self.fget = method
        return self


def all_subclasses(cls):
    return set(cls.__subclasses__()).union(
        [s for c in cls.__subclasses__() for s in all_subclasses(c)]
    )


def shuffled(xs: Iterable) -> List:
    ys = list(xs)
    random.shuffle(ys)
    return ys


def index_by(attr: str) -> Callable[[Iterable[Any]], Dict[Any, List[Any]]]:
    keygetter = attrgetter(attr)

    def inner(items: Iterable[Any]) -> Dict[Any, List[Any]]:
        return {
            key: list(group) for key, group in groupby(sorted(items, key=keygetter), key=keygetter)
        }

    return inner
