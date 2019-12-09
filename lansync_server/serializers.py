from marshmallow import Schema, fields


class NamespaceSerializer(Schema):
    name = fields.Str()


class NodeEventSerializer(Schema):
    namespace = fields.Nested(NamespaceSerializer, dump_only=True)
    key = fields.Str()
    operation = fields.Str()
    sequence_number = fields.Integer(dump_only=True)
    path = fields.Str()
    timestamp = fields.Str()
    checksum = fields.Str(allow_none=True)
    parts = fields.List(fields.Str(), allow_none=True)
