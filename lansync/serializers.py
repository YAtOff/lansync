from marshmallow import Schema, fields


class NodeEventSerializer(Schema):
    key = fields.Str()
    operation = fields.Str()
    sequence_number = fields.Integer(allow_none=True)
    path = fields.Str()
    timestamp = fields.Str()
    checksum = fields.Str(allow_none=True)
    parts = fields.List(fields.Str(), allow_none=True)
