def remove_elements(data, removelist):
    def recursive_remove(data, keys):
        if len(keys) == 1:
            data.pop(keys[0], None)
        else:
            key = keys.pop(0)
            if key in data:
                recursive_remove(data[key], keys)
                if not data[key]:  # Remove the key if the sub-dictionary is empty
                    data.pop(key)

    for item in removelist:
        keys = item.split('.')
        recursive_remove(data, keys)

    return data

# Example usage
data = {
    "a": 1,
    "b": {
        "sub_b1": 2,
        "sub_b2": 3
    },
    "c": {
        "sub_c1": 2,
        "sub_c2": {
            "sub_sub_c2_c1": 1,
            "sub_sub_c2_c2": 3
        }
    },
    "d": 3
}

# removelist = ["a", "b.sub_b1", "c.sub_c2.sub_sub_c2_c1"]
# result = remove_elements(data, removelist)
# print(result)

def flatten_data(data):
    flattened_data = {}

    for key, value in data.items():
        if key == "payload":
            flattened_data.update(value)
        elif key == "source_metadata":
            for sub_key, sub_value in value.items():
                flattened_data[f"source_metadata_{sub_key}"] = sub_value
        else:
            flattened_data[f"ingestion_meta_data_{key}"] = value

    return flattened_data

# Example usage
from datetime import datetime, timezone

data = {
    'uuid': 'b1ee962b-21b3-4662-b3a6-42d300000000',
    'read_timestamp': datetime(2024, 5, 22, 8, 40, 3, 542000, tzinfo=timezone.utc),
    'object': 'public_dwh_entity_role_properties',
    'sort_keys': [1716367203542, ''],
    'source_metadata': {
        'schema': 'public',
        'table': 'dwh_entity_role_properties',
        'is_deleted': False,
        'change_type': 'INSERT',
        'primary_keys': []
    },
    'payload': {
        'id': 39635,
        'public_identifier': 'dcbdcfd8-c994-40b5-a6eb-7d30dee66b55',
        'creation_timestamp': datetime(2023, 10, 17, 8, 15, 48, 620332, tzinfo=timezone.utc),
        'last_update_timestamp': datetime(2023, 10, 17, 8, 15, 48, 620335, tzinfo=timezone.utc),
        'property_type': 'SHAREHOLDER_PERCENTAGE',
        'property_value': '100.00'
    }
}

# result = flatten_data(data)
# print(result)
def flatten_schema(data):
    new_fields = []
    new_fields.append({"name": "ingestion_meta_data_processing_timestamp", "type": ["null", {"type": "long", "logicalType": "timestamp-micros"}]})
    for field in data["fields"]:
        field_name = field["name"]

        if field_name == "payload":
            for subfield in field["type"]["fields"]:
                new_fields.append(subfield)
        elif field_name == "source_metadata":
            for subfield in field["type"]["fields"]:
                subfield_name = f"source_metadata_{subfield['name']}"
                subfield["name"] = subfield_name
                new_fields.append(subfield)
        else:
            prefixed_name = f"ingestion_meta_data_{field_name}"
            field["name"] = prefixed_name
            new_fields.append(field)
    new_fields.append({"name": "row_hash",  "type": ["null", {"type": "bytes"}]})
    return {"fields": new_fields}

# Function to convert JSON schema to BigQuery schema
def json_to_bigquery_schema(json_schema):
    fields = []
    mapping = {
        'boolean': 'BOOL',
        'int': 'INT64',
        'long': 'INT64',
        'float': 'FLOAT64',
        'double': 'FLOAT64',
        'bytes': 'BYTES',
        'string': 'STRING',
        'date': 'TIMESTAMP'
    }
    def get_bq_type(field_type):
        if isinstance(field_type, dict):
            if field_type['type'] == 'array':
                return 'STRING'  # Arrays are represented as strings in BigQuery
            elif field_type['type'] == 'long' and field_type.get('logicalType') in ['timestamp-millis', 'timestamp-micros']:
                return 'TIMESTAMP'
            else:
                return mapping.get(field_type.get('type','string'), 'STRING')
        elif isinstance(field_type, list):
            for t in field_type:
                if isinstance(t, dict):
                    if t.get('logicalType') in ['timestamp-millis', 'timestamp-micros']:
                        return 'TIMESTAMP'
                    else: return mapping.get(t.get('type','string'), 'STRING') # Default to STRING if type not found
        else:
            return mapping.get(field_type, 'STRING')  # Default to STRING if type not found

    for field in json_schema['fields']:
        field_name = field['name']
        field_type = field['type']
        
        # Get the BigQuery type using the helper function
        bq_type = get_bq_type(field_type)
        
        fields.append({'name': field_name, 'type': bq_type, 'mode': 'NULLABLE'})
    
    return {'fields': fields}
# Example usage
data = {
    "fields": [
        {"name": "uuid", "type": "string"},
        {
            "name": "read_timestamp",
            "type": {"type": "long", "logicalType": "timestamp-millis"},
        },
        {
            "name": "source_timestamp",
            "type": {"type": "long", "logicalType": "timestamp-millis"},
        },
        {"name": "object", "type": "string"},
        {"name": "read_method", "type": "string"},
        {"name": "stream_name", "type": "string"},
        {"name": "schema_key", "type": "string"},
        {"name": "sort_keys", "type": {"type": "array", "items": ["string", "long"]}},
        {
            "name": "source_metadata",
            "type": {
                "type": "record",
                "name": "source_metadata",
                "fields": [
                    {"name": "schema", "type": "string"},
                    {"name": "table", "type": "string"},
                    {"name": "is_deleted", "type": ["null", "boolean"]},
                    {"name": "change_type", "type": ["null", "string"]},
                    {"name": "tx_id", "type": ["null", "long"]},
                    {"name": "lsn", "type": ["null", "string"]},
                    {
                        "name": "primary_keys",
                        "type": ["null", {"type": "array", "items": "string"}],
                    },
                ],
            },
        },
        {
            "name": "payload",
            "type": {
                "type": "record",
                "name": "payload",
                "fields": [
                    {"name": "id", "type": ["null", "int"]},
                    {
                        "name": "public_identifier",
                        "type": [
                            "null",
                            {"type": "string", "logicalType": "varchar", "length": 512},
                        ],
                    },
                    {
                        "name": "creation_timestamp",
                        "type": [
                            "null",
                            {"type": "long", "logicalType": "timestamp-micros"},
                        ],
                    },
                    {
                        "name": "last_update_timestamp",
                        "type": [
                            "null",
                            {"type": "long", "logicalType": "timestamp-micros"},
                        ],
                    },
                    {
                        "name": "entity_role_public_identifier",
                        "type": [
                            "null",
                            {"type": "string", "logicalType": "varchar", "length": 512},
                        ],
                    },
                    {
                        "name": "property_type",
                        "type": [
                            "null",
                            {"type": "string", "logicalType": "varchar", "length": 512},
                        ],
                    },
                    {
                        "name": "property_value",
                        "type": [
                            "null",
                            {"type": "string", "logicalType": "varchar", "length": 512},
                        ],
                    },
                ],
            },
        },
    ],
}

result = flatten_schema(data)

print(json_to_bigquery_schema(result))

