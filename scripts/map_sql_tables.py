from chatto_transform.schema.schema_base import *

from chatto_transform.datastores.sqlalchemy_datastore import get_reflected_metadata, table_as_schema
from chatto_transform.config import config, mimic_config
import argparse
from sqlalchemy import create_engine

parser = argparse.ArgumentParser()

table_group = parser.add_mutually_exclusive_group()
table_group.add_argument('--table_names', nargs='+')
table_group.add_argument('--all_tables', action='store_true')
parser.add_argument('--schema')
parser.add_argument('--filename', required=True)

if __name__ == '__main__':
	args = parser.parse_args()
	engine = create_engine(mimic_config.mimic_psql_config)
	
	metadata = get_reflected_metadata(engine, schema_name=args.schema)
	if args.all_tables:
		table_names = metadata.tables.keys()
	else:
		table_names = args.table_names

	with open(config.data_dir+args.filename, 'w') as f:
		for table_name in sorted(table_names):
			print('getting reflected table for', table_name)
			table = metadata.tables[table_name]
			s = table_as_schema(table)
			f.write('{name}_schema = {repr}\n\n'.format(name=s.name, repr=repr(s)))
