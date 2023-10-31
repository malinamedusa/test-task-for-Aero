import json

import requests
from io import StringIO
import pandas as pd
import psycopg2
import yaml
import os


class GetLoadDataPG:

    def __init__(self, info_object, load_data, load_hour, log, cursor):

        self.url = info_object['source_data']
        self.column_nested_json = info_object['column_nested_json']
        self.database = info_object['in_database']
        self.table_name = info_object['out_table_name']

        self.load_data = load_data
        self.load_hour = load_hour

        self.response = None

        self.dataframe = None
        self.data_type = dict()

        self.log = log
        self.cursor = cursor

        self.tmp_file_name = f'tmp/df__{self.table_name}.json'

    def get_data_to_df(self):
        try:
            self.response = requests.get(url=self.url)
            self.dataframe = pd.read_json(StringIO(self.response.text))
            assert self.response.status_code == 200
        except AssertionError:
            self.log.error(f'response.status_code = {self.response.status_code}')
        else:
            self.log.info(f'response.status_code = {self.response.status_code}')

    def add_tech_columns(self):
        self.dataframe['load_data'] = self.load_data
        self.dataframe['load_hour'] = self.load_hour

    def unpacking_nested_json(self):
        # если есть - распаковываем вложенные json в новые поля
        for column in self.column_nested_json:
            sub_df = pd.json_normalize(self.dataframe[column], sep='_')
            self.dataframe = self.dataframe.join(sub_df, how='left')
            self.dataframe.pop(column)

        self.log.info(f'DataFrame {self.table_name} shape - {self.dataframe.shape}')

    def df_to_tmp_json(self):
        to_json = self.dataframe.to_json(orient="records")
        try:
            with open(f'tmp/df__{self.table_name}.json', 'w') as f:
                json.dump(to_json, f)
        except FileNotFoundError as err:
            self.log.error(f'{err} Create {self.tmp_file_name} failed')
        else:
            self.log.info(f'Create {self.tmp_file_name} successfully')

    def open_tmp_file(self):
        try:
            with open(self.tmp_file_name, 'r') as file:
                tmp_json = yaml.safe_load(file)
            self.dataframe = pd.read_json(StringIO(tmp_json))
        except FileNotFoundError as err:
            self.log.error(f'{err} Reading file {self.tmp_file_name} failed')
        else:
            self.log.info(f'Reading file {self.table_name} successfully')

    def data_type_definition(self):
        for column, value in zip(self.dataframe.columns, self.dataframe.iloc[0]):
            if column == 'load_hour':
                self.data_type[column] = 'SMALLINT'
            elif column == 'load_data':
                self.data_type[column] = 'DATE'
            else:
                self.data_type[column] = 'TEXT'

    def delete_table(self):
        query = f"""DELETE FROM {self.table_name}"""
        try:
            self.cursor.execute(query)
        except psycopg2.errors.SyntaxError as err:
            self.log.error(f'{err} Delete table {self.table_name} failed with query {query}')
        else:
            self.log.info(f'Delete table {self.table_name} successfully')

    def check_delete_table(self):
        result = ''
        check_query = f"""SELECT * FROM {self.table_name}"""
        try:
            self.cursor.execute(check_query)
            result = self.cursor.fetchall()
            assert result == []
        except AssertionError:
            self.log.error(f'Row in table {self.table_name} if exists - {result} row')
        except psycopg2.errors.SyntaxError as err:
            self.log.error(f'{err} Execute row count failed with {check_query}')
        else:
            self.log.info(f'No row in table {self.table_name} - {result}')

    def check_table_exists(self):
        query = f"""SELECT distinct table_name """ \
                f"""FROM information_schema.tables """ \
                f"""WHERE table_name = '{self.table_name}'"""
        try:
            self.cursor.execute(query)
            found_table = self.cursor.fetchall()
            if found_table:
                self.log.info(f'Table {self.table_name} is exists')
                return True
            else:
                self.log.info(f'Table {self.table_name} not exists, need create')
                return False
        except psycopg2.errors.SyntaxError as err:
            self.log.error(f'{err} - Execute check table failed with {query}')

    def create_table(self):
        query = ''
        for col, type_data in self.data_type.items():
            query += f"""{col} {type_data}, """

        query = f"""CREATE TABLE IF NOT EXISTS {self.table_name} ({query[:-2]})"""
        try:
            self.cursor.execute(query)
        except psycopg2.errors.SyntaxError as err:
            self.log.error(f'{err} Create table {self.table_name} failed with query {query}')
        else:
            self.log.info(f'Create table {self.table_name} successfully')

    def describe_table(self):
        query = f"""SELECT table_name, column_name, data_type """ \
                f"""FROM information_schema.columns """ \
                f"""WHERE table_name = '{self.table_name}'"""
        try:
            self.cursor.execute(query)
            res = self.cursor.fetchall()
        except psycopg2.errors.SyntaxError as err:
            self.log.error(f'{err} Execute row count failed with {query}')
        else:
            self.log.info(f'Describe table: ')
            for row in res:
                self.log.info(f'{row}')

    def insert_table(self):
        insert_query = f"""INSERT INTO {self.table_name} ({', '.join(self.dataframe.columns)}) VALUES """

        for row in self.dataframe.values:
            insert_query += """("""
            for value in row:
                if pd.isnull(value):
                    insert_query += """NULL, """
                elif isinstance(value, bool):
                    insert_query += f"""{value}, """
                else:
                    insert_query += f"""$${str(value)}$$, """
            insert_query = f"""{insert_query[:-2]}), """
        try:
            self.cursor.execute(insert_query[:-2])
        except psycopg2.errors.SyntaxError as err:
            self.log.error(f'{err} Insert into table {self.table_name} failed with query {insert_query}')
        except psycopg2.errors.InvalidTextRepresentation as err:
            self.log.error(f'{err} Insert into table {self.table_name} failed with query {insert_query}')
        except psycopg2.errors.DatetimeFieldOverflow as err:
            self.log.error(f'{err} Insert into table {self.table_name} failed with query {insert_query}')
        else:
            self.log.info(f'Insert in table {self.table_name} successfully')

    def check_added_data(self):
        query = f"""SELECT count(*) """ \
                f"""FROM {self.table_name} """ \
                f"""WHERE load_data = '{self.load_data}' AND load_hour = '{self.load_hour}'"""
        try:
            self.cursor.execute(query)
            row_count = self.cursor.fetchall()[0][0]
            assert row_count != 0
        except AssertionError as err:
            self.log.error(f'{err} - NO ROW COUNT in table {self.table_name}')
        except psycopg2.errors.SyntaxError as err:
            self.log.error(f'{err} - Execute row count failed with {query}')
        else:
            self.log.info(f'Row count added in table {self.table_name} - {row_count}')

    def delete_tmp_file(self):
        try:
            os.remove(self.tmp_file_name)
        except Exception as err:
            self.log.error(f'{err} Remove {self.tmp_file_name} failed')
        else:
            self.log.info(f'Remove {self.tmp_file_name} successfully')
