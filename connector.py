from io import StringIO
import requests
import psycopg2
import yaml
import logging
import pandas as pd
from datetime import datetime


class LoadingDataHTTPtoPG:

    def __init__(self, info_object):
        with open('settings.yaml', 'r') as file:
            settings = yaml.safe_load(file)

        self.user = settings['user']
        self.host = settings['host']
        self.password = settings['password']

        self.url = info_object['source_data']
        self.column_nested_json = info_object['column_nested_json']
        self.database = info_object['in_database']
        self.table_name = info_object['out_table_name']

        self.load_data = str(datetime.now().date())
        self.load_hour = datetime.now().hour

        self.dataframe = None
        self.data_type = dict()
        self.connection = None
        self.cursor = None

    def __logger__(self, filename):
        self.filename = f'{filename}{self.load_data}_{self.load_hour}h.log'
        logging.basicConfig(level=logging.INFO,
                            filename=self.filename,
                            filemode="a",
                            format="%(asctime)s %(levelname)s %(message)s")

    def get_data_to_pandas(self):
        response_status = 0
        try:
            response = requests.get(url=self.url)
            self.dataframe = pd.read_json(StringIO(response.text))
            response_status = response.status_code
            logging.info(f'response.status_code = {response_status}')
            assert response_status == 200
        except AssertionError:
            logging.error(f'response.status_code = {response_status}')

        if self.column_nested_json:

            for column in self.column_nested_json:
                sub_df = pd.json_normalize(self.dataframe[column], sep='_')
                self.dataframe = self.dataframe.join(sub_df, how='left')
                self.dataframe.pop(column)
        self.dataframe.fillna('NULL', inplace=True)

    def add_tech_columns(self):
        self.dataframe['load_data'] = self.load_data
        self.dataframe['load_hour'] = self.load_hour
        logging.info(f'DataFrame {self.table_name} shape - {self.dataframe.shape}')

    def data_type_definition(self):
        for column, value in zip(self.dataframe.columns, list(self.dataframe.iloc[0])):
            if column == 'load_data':
                self.data_type[column] = 'DATE'
            elif isinstance(value, bool):
                self.data_type[column] = 'BOOL'
            else:
                self.data_type[column] = 'TEXT'

    def connect_to_pg(self):
        try:
            self.connection = psycopg2.connect(database=self.database,
                                               user=self.user,
                                               host=self.host,
                                               password=self.password)
            self.connection.autocommit = True
            self.cursor = self.connection.cursor()
        except psycopg2.OperationalError as err:
            logging.error(f'{err}')
        else:
            logging.info(f'Connection to server at {self.host}, database {self.database} successfully')

    def delete_table(self):
        query = f'DELETE FROM {self.table_name};'
        try:
            self.cursor.execute(query)
        except psycopg2.errors.SyntaxError as err:
            logging.error(f'{err} Delete table {self.table_name} failed with query {query}')
        else:
            logging.info(f'Delete table {self.table_name} successfully')

        check_query = f'SELECT * FROM {self.table_name};'
        try:
            self.cursor.execute(check_query)
            res = self.cursor.fetchall()
            assert res == []
        except AssertionError:
            logging.error(f'Row in table {self.table_name} if exists - {res} row')
        except psycopg2.errors.SyntaxError as err:
            logging.error(f'{err} Execute row count failed with {check_query}')
        else:
            logging.info(f'No row in table {self.table_name} - {res}')

    def create_table(self):
        query = ''

        for col, type in self.data_type.items():
            query += f"{col} {type}, "

        query = f"""CREATE TABLE IF NOT EXISTS {self.table_name} ({query[:-2]})"""
        try:
            self.cursor.execute(query)
        except psycopg2.errors.SyntaxError as err:
            logging.error(f'{err} Create table {self.table_name} failed with query {query}')
        else:
            logging.info(f'Create table {self.table_name} successfully')

    def describe_table(self):
        query = f'''SELECT table_name, column_name, data_type ''' \
                f'''FROM information_schema.columns ''' \
                f'''WHERE table_name = '{self.table_name}';'''
        try:
            self.cursor.execute(query)
            res = self.cursor.fetchall()
        except psycopg2.errors.SyntaxError as err:
            logging.error(f'{err} Execute row count failed with {query}')

        else:
            logging.info(f"DDL table is:")
            for row in res:
                logging.info(f"{row} \n")

    def insert_table(self):
        insert_query = f"""INSERT INTO {self.table_name} ({', '.join(self.dataframe.columns)}) VALUES """

        for row in self.dataframe.values:
            insert_query += '('
            for val in row:
                if isinstance(val, bool) or val == "NULL":
                    insert_query += f"{val}, "
                else:
                    insert_query += "$$" + str(val) + "$$, "
            insert_query = insert_query[:-2] + "),"
        try:
            self.cursor.execute(insert_query[:-1])
        except psycopg2.errors.SyntaxError as err:
            logging.error(f'{err} Insert into table {self.table_name} failed with query {insert_query}')
        except psycopg2.errors.InvalidTextRepresentation as err:
            logging.error(f'{err} Insert into table {self.table_name} failed with query {insert_query}')
        else:
            logging.info(f'Insert in table {self.table_name} successfully')

    def check_added_data(self):
        query = f'''SELECT count(*) ''' \
                f'''FROM {self.table_name} ''' \
                f'''WHERE load_data = '{self.load_data}' AND load_hour = '{self.load_hour}';'''
        try:
            self.cursor.execute(query)
            res = self.cursor.fetchall()
            row_count = int(str(res)[2:-3])
            assert row_count != 0
        except AssertionError:
            logging.error(f'NO ROW COUNT in table {self.table_name}')
        except psycopg2.errors.SyntaxError as err:
            logging.error(f'{err} Execute row count failed with {query}')
        else:
            logging.info(f'Row count added in table {self.table_name} - {row_count}')

    def connection_close(self):
        self.connection.close()
        logging.info('connection.close()')
