import sys
from avro.datafile import DataFileReader
from avro.io import DatumReader
import json
file_name="/Users/hkhnhan/Documents/Code/test/datastream-postgres_datastream_cmd_test_datastream-postgres_datastream_cmd_test_Arvo_public_dwh_entity_role_properties_2024_05_22_06_09_7bb32d069d36f7728b1ca66f812bb4ff24413219_postgresql-backfill_1017065919_5_0.avro"
reader = DataFileReader(open(file_name, "rb"), DatumReader())

schema = reader.get_meta('avro.schema')

parsed = json.loads(schema)

print(json.dumps(parsed, indent=4, sort_keys=True))