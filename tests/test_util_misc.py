from typing import NamedTuple

from lansync.util.misc import index_by


def test_index_by():
    index_by_x = index_by("x")

    class X(NamedTuple):
        x: int

    items = [X(x=1), X(x=2), X(x=1)]

    assert index_by_x(items) == {1: [X(x=1), X(x=1)], 2: [X(x=2)]}
