# переделать модуль в airflow dag

from connectors.connector_pg import ConnectorPG
from loaders.get_load_data_pg import GetLoadDataPG
from logger import Logging
from datetime import datetime
import yaml

with open('info_about_tables_pg.yaml', 'r') as file:
    about_tables = yaml.safe_load(file)

load_data = str(datetime.now().date())
load_hour = datetime.now().hour

log = Logging(filename=f'log/pg/load_data', load_data=load_data, load_hour=load_hour)


def task_get_data_to_tmp(loader):
    loader.get_data_to_df()
    if about_tables[info]['column_nested_json']:
        loader.unpacking_nested_json()

    loader.add_tech_columns()
    loader.df_to_tmp_json()


def task_tmp_to_table(loader):
    # чекаем наличие таблицы, если нет - создаем
    if not loader.check_table_exists():
        loader.data_type_definition()
        loader.create_table()
        loader.describe_table()

    loader.open_tmp_file()
    loader.insert_table()
    loader.check_added_data()
    connector.connection_close()
    loader.delete_tmp_file()


for info in about_tables:
    connector = ConnectorPG(log=log, dbname=about_tables[info]['in_database'])
    cursor = connector.cursor

    loader = GetLoadDataPG(info_object=about_tables[info],
                           load_data=load_data,
                           load_hour=load_hour,
                           log=log,
                           cursor=cursor)
    task_get_data_to_tmp(loader)
    task_tmp_to_table(loader)
