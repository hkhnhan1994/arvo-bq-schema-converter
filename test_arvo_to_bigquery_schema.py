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
AVRO_TO_BIGQUERY_TYPES = {
            "record": "RECORD",
            "string": "STRING",
            "int": "INTEGER",
            "boolean": "BOOL",
            "double": "FLOAT",
            "float": "FLOAT",
            "long": "INT64",
            "bytes": "BYTES",
            "enum": "STRING",
            # logical types
            "decimal": "FLOAT",
            "uuid": "STRING",
            "date": "DATE",
            "time-millis": "TIME",
            "time-micros": "TIME",
            "timestamp-millis": "TIMESTAMP",
            "timestamp-micros": "TIMESTAMP",
            "varchar": "STRING",
        }
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
def convert_schema(avro_schema):
    """
    Convert an Avro schema to a BigQuery schema
    :param avro_schema: The Avro schema
    :return: The BigQuery schema
    """
    AVRO_TO_BIGQUERY_TYPES = {
    "record": "RECORD",
    "string": "STRING",
    "int": "INTEGER",
    "boolean": "BOOL",
    "double": "FLOAT",
    "float": "FLOAT",
    "long": "INT64",
    "bytes": "BYTES",
    "enum": "STRING",
    # logical types
    "decimal": "FLOAT",
    "uuid": "STRING",
    "date": "DATE",
    "time-millis": "TIME",
    "time-micros": "TIME",
    "timestamp-millis": "TIMESTAMP",
    "timestamp-micros": "TIMESTAMP",
    "varchar": "STRING",
    }
    def _convert_type(avro_type):
        """
        Convert an Avro type to a BigQuery type
        :param avro_type: The Avro type
        :return: The BigQuery type
        """
        mode = "NULLABLE"
        fields = ()

        if isinstance(avro_type, list):
            # list types are unions, one of them should be null; get the real type
            if len(avro_type) == 2:
                if avro_type[0] == "null":
                    avro_type = avro_type[1]
                elif avro_type[1] == "null":
                    avro_type = avro_type[0]
                else:
                    raise ReferenceError(
                        "One of the union fields should have type `null`"
                    )
            else:
                raise ReferenceError(
                    "A Union type can only consist of two types, "
                    "one of them should be `null`"
                )

        if isinstance(avro_type, dict):
            field_type, fields, mode = _convert_complex_type(avro_type)

        else:
            field_type = AVRO_TO_BIGQUERY_TYPES[avro_type]

        return field_type, mode, fields


    def _convert_complex_type(avro_type):
        """
        Convert a Avro complex type to a BigQuery type
        :param avro_type: The Avro type
        :return: The BigQuery type
        """
        fields = ()
        mode = "NULLABLE"

        if avro_type["type"] == "record":
            field_type = "RECORD"
            fields = tuple(map(lambda f: _convert_field(f), avro_type["fields"]))
        elif avro_type["type"] == "array":
            mode = "REPEATED"
            if "logicalType" in avro_type["items"]:
                field_type = AVRO_TO_BIGQUERY_TYPES[
                    avro_type["items"]["logicalType"]
                ]
            elif isinstance(avro_type["items"], dict):
                # complex array
                if avro_type["items"]["type"] == "enum":
                    field_type = AVRO_TO_BIGQUERY_TYPES[avro_type["items"]["type"]]
                else:
                    field_type = "RECORD"
                    fields = tuple(
                        map(
                            lambda f: _convert_field(f),
                            avro_type["items"]["fields"],
                        )
                    )
            else:
                # simple array
                field_type = AVRO_TO_BIGQUERY_TYPES[avro_type["items"]]
                # field_type = 'STRING'
        elif avro_type["type"] == "enum":
            field_type = AVRO_TO_BIGQUERY_TYPES[avro_type["type"]]
        elif avro_type["type"] == "map":
            field_type = "RECORD"
            mode = "REPEATED"
            # Create artificial fields to represent map in BQ
            key_field = {
                "name": "key",
                "type": "string",
                "doc": "Key for map avro field",
            }
            value_field = {
                "name": "value",
                "type": avro_type["values"],
                "doc": "Value for map avro field",
            }
            fields = tuple(
                map(lambda f: _convert_field(f), [key_field, value_field])
            )
        elif "logicalType" in avro_type:
            field_type = AVRO_TO_BIGQUERY_TYPES[avro_type["logicalType"]]
        elif avro_type["type"] in AVRO_TO_BIGQUERY_TYPES:
            field_type = AVRO_TO_BIGQUERY_TYPES[avro_type["type"]]
        else:
            raise ReferenceError(f"Unknown complex type {avro_type['type']}")
        return field_type, fields, mode


    def _convert_field(avro_field):
        """
        Convert an Avro field to a BigQuery field
        :param avro_field: The Avro field
        :return: The BigQuery field
        """

        if "logicalType" in avro_field:
            field_type, mode, fields = _convert_type(avro_field["logicalType"])
        else:
            field_type, mode, fields = _convert_type(avro_field["type"])

        return {
            "name": avro_field.get("name"),
            "type": field_type,
            "mode": mode,
            "fields":fields,
        }
    # bigquery.SchemaField(
    #         avro_field.get("name"),
    #         field_type,
    #         mode=mode,
    #         description=avro_field.get("doc"),
    #         fields=fields,
    #     )
    return tuple(map(lambda f: _convert_field(f), avro_schema["fields"]))
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
            new_fields.append({"name": "row_hash", "type": ["null", {"type": "bytes"}]})
            return {"fields": new_fields}
parsed = json.loads(data)
cleandata= clean_data(parsed,ignore_fields)
flatten_data= flatten_schema(cleandata)
result=convert_schema(flatten_data)
print(result)