from marshmallow import Schema, fields


class NodeChunkSerializer(Schema):
    hash = fields.Str()
    size = fields.Integer()
    offset = fields.Integer()


class NodeEventSerializer(Schema):
    key = fields.Str()
    operation = fields.Str()
    sequence_number = fields.Integer(allow_none=True)
    path = fields.Str()
    timestamp = fields.Str()
    checksum = fields.Str(allow_none=True)
    size = fields.Integer(allow_none=True)
    chunks = fields.List(
        fields.Nested(NodeChunkSerializer), allow_none=True
    )
    signature = fields.Str(allow_none=True)
