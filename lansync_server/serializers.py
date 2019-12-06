from marshmallow import Schema, fields


class NamespaceSerializer(Schema):
    name = fields.Str()


class NodeSerializer(Schema):
    namespace = fields.Nested(NamespaceSerializer, dump_only=True)
    key = fields.Str(dump_only=True)
    path = fields.Str()
    checksum = fields.Str()
    timestamp = fields.Str()
    parts = fields.List(fields.Str())
