import time
from marshmallow import Schema, fields, validate

SENSOR_TYPES = ['temperature', 'humidity']


class DeviceReadingSchema(Schema):
    type = fields.Str(required=True, validate=validate.OneOf(SENSOR_TYPES))
    value = fields.Int(required=True, validate=validate.Range(min=0, max=100))
    date_created = fields.Int(missing=int(time.time()))
