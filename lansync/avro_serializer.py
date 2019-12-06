from io import BytesIO
import json
from pathlib import Path
from typing import Dict, Any

from fastavro import writer, reader, parse_schema  # type: ignore

from lansync.util.misc import classproperty


schema_registry: Dict[str, Any] = {}


def load_schema(name: str):
    if name in schema_registry:
        return schema_registry[name]

    schema_registry_root = Path(__file__).parent.parent / "schema"
    raw_schema = (schema_registry_root / name).with_suffix(".json").read_text()
    json_schema = json.loads(raw_schema)
    schema = parse_schema(json_schema)
    schema_registry[name] = schema
    return schema


class Serializer:
    def __init__(self, schema_name) -> None:
        self.schema = load_schema(schema_name)

    def load_record(self, data: bytes) -> Dict:
        buffer = BytesIO(data)
        return next(reader(buffer, self.schema))

    def dump_record(self, record: Dict) -> bytes:
        buffer = BytesIO()
        writer(buffer, self.schema, (record,))
        return buffer.getvalue()

    def load_record_from_file(self, fd) -> Dict:
        return next(reader(fd, self.schema))

    def dump_record_to_file(self, record: Dict, fd) -> None:
        writer(fd, self.schema, (record,))


class SerializerMixin:
    schema_name: str

    _serializer = None

    def as_record(self) -> Dict[str, Any]:
        raise NotImplementedError

    @classmethod
    def from_record(cls, record: Dict[str, Any]) -> Any:
        raise NotImplementedError

    @classproperty
    def serializer(cls) -> Serializer:
        if cls._serializer is None:
            cls._serializer = Serializer(cls.schema_name)
        return cls._serializer

    @classmethod
    def load(cls, data):
        return cls.from_record(cls.serializer.load_record(data))

    @classmethod
    def load_from_file(cls, fd):
        return cls.from_record(cls.serializer.load_record_from_file(fd))

    def dump(self) -> bytes:
        return self.serializer.dump_record(self.as_record())

    def dump_to_file(self, fd) -> None:
        self.serializer.dump_record_to_file(self.as_record(), fd)
