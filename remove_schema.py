import json
data = """
{
    "fields": [
      {
        "name": "uuid",
        "type": "string"
      },
      {
        "name": "read_timestamp",
        "type": {
          "logicalType": "timestamp-millis",
          "type": "long"
        }
      },
      {
        "name": "source_timestamp",
        "type": {
          "logicalType": "timestamp-millis",
          "type": "long"
        }
      },
      {
        "name": "object",
        "type": "string"
      },
      {
        "name": "read_method",
        "type": "string"
      },
      {
        "name": "source_metadata",
        "type": {
          "fields": [
            {
              "name": "schema",
              "type": "string"
            },
            {
              "name": "table",
              "type": "string"
            },
            {
              "name": "is_deleted",
              "type": [
                "null",
                "boolean"
              ]
            },
            {
              "name": "change_type",
              "type": [
                "null",
                "string"
              ]
            },
            {
              "name": "primary_keys",
              "type": [
                "null",
                {
                  "items": "string",
                  "type": "array"
                }
              ]
            }
          ],
          "name": "source_metadata",
          "type": "record"
        }
      },
      {
        "name": "payload",
        "type": {
          "fields": [
            {
              "name": "id",
              "type": [
                "null",
                "int"
              ]
            },
            {
              "name": "public_identifier",
              "type": [
                "null",
                {
                  "length": 512,
                  "logicalType": "varchar",
                  "type": "string"
                }
              ]
            },
            {
              "name": "creation_timestamp",
              "type": [
                "null",
                {
                  "logicalType": "timestamp-micros",
                  "type": "long"
                }
              ]
            },
            {
              "name": "last_update_timestamp",
              "type": [
                "null",
                {
                  "logicalType": "timestamp-micros",
                  "type": "long"
                }
              ]
            },
            {
              "name": "entity_role_public_identifier",
              "type": [
                "null",
                {
                  "length": 512,
                  "logicalType": "varchar",
                  "type": "string"
                }
              ]
            },
            {
              "name": "property_type",
              "type": [
                "null",
                {
                  "length": 512,
                  "logicalType": "varchar",
                  "type": "string"
                }
              ]
            },
            {
              "name": "property_value",
              "type": [
                "null",
                {
                  "length": 512,
                  "logicalType": "varchar",
                  "type": "string"
                }
              ]
            }
          ],
          "name": "payload",
          "type": "record"
        }
      }
    ],
    "name": "public_dwh_entity_role_properties",
    "type": "record"
  }
"""
ignore_fields =[
    'stream_name',
    'schema_key',
    'sort_keys',
    'source_metadata.tx_id',
    'source_metadata.lsn',
]
data_loaded = json.loads(data)
def clean_data(data, ignore_fields):
  """
  This function removes fields from a nested data structure based on an ignore list.

  Args:
      data: The input data structure (dictionary or list).
      ignore_fields: A list of strings representing the fields to ignore.

  Returns:
      A new data structure with the ignored fields removed.
  """

  def remove_fields_helper(data):
    """
    Recursive helper function to remove fields based on ignore list.

    Args:
        data: The data structure to be processed (dictionary or list).

    Returns:
        A new data structure with the ignored fields removed.
    """
    if isinstance(data, dict):
      return {key: remove_fields_helper(value) for key, value in data.items() if key not in ignore_fields}
    elif isinstance(data, list):
      return [remove_fields_helper(item) for item in data]
    else:
      return data

  # Use the helper function to clean the data
  return remove_fields_helper(data)
print(clean_data(data_loaded,ignore_fields))
        