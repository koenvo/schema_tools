""" Map PySchema to ElasticSearch index definition
"""


from pyschema.types import Field, Boolean, Integer, Float
from pyschema.types import Bytes, Text, Enum, List, Map, SubRecord

# https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-types.html
# https://github.com/koenvo/pyschema/blob/master/pyschema_extensions/avro.py

Boolean.elasticsearch_type = "boolean"
Bytes.elasticsearch_type = "binary"
Text.elasticsearch_type = "string"



@Float.mixin
class FloatMixin:
    @property
    def elasticsearch_type(self):
        if self.size <= 4:
            return 'float'
        return 'double'


@Integer.mixin
class IntegerMixin:
    @property
    def elasticsearch_type(self):
        if self.size <= 4:
            return 'integer'
        return 'long'


@SubRecord.mixin
class SubRecordMixin:
    elasticsearch_type = 'object'

@Field.mixin
class FieldMixin:
    simplified_elasticsearch_schema()


def get_schema_dict(record):