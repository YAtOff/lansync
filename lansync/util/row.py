from __future__ import annotations

from typing import List, Any, Iterable


class Row:
    value_types: List[type]
    key: str
    values: List[Any]

    def __init__(self, key: str, values: List[Any]):
        self.key = key
        self.values = values

    def __iter__(self):
        return iter((self.key, *self.values))

    @classmethod
    def create(cls, key: str, values: Iterable[Any]) -> Row:
        result_values = {t: None for t in cls.value_types}

        for value in values:
            value_type = next(
                (t for t in cls.value_types if isinstance(value, t)), None
            )
            if value_type:
                result_values[value_type] = value

        return cls(key, list(result_values.values()))
