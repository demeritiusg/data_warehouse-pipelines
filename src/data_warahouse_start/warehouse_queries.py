import configparser
from pathlib import Path

config = configparser.ConfigParser()
config.read_file(open(f"{Path(__file__).parents[0]}/config.cfg"))

warehouse_schema = config.get('WAREHOUSE', 'SCHEMA')

create_warehouse_schema = "CREATE SCHEMA IF NOT EXISTS {};".format(warehouse_schema)

drop_table = "DROP TABLE IF EXISTS {};".format(warehouse_schema)

create_table = """

""".format(warehouse_schema)

drop_warehouse_tables = [drop_authors_table]
create_warehouse_tables = [create_authors_table]