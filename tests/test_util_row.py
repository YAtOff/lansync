import pytest

from lansync.util.row import Row


class Row1(Row):
    value_types = [int, str, bytes]


@pytest.mark.parametrize(
    "row,values",
    [
        (Row1.create("k", (1, "x", b"x")), (1, "x", b"x")),
        (Row1.create("k", (b"x", 1, "x")), (1, "x", b"x")),
        (Row1.create("k", (None, "x", b"x")), (None, "x", b"x")),
        (Row1.create("k", (b"x", None, 1)), (1, None, b"x")),
    ]
)
def test_row(row, values):
    assert tuple(row) == (row.key, *values)
