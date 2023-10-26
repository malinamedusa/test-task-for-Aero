from connector import LoadingDataHTTPtoPG
import yaml


def main():

    with open('info_about_tables.yaml', 'r') as file:
        info_about_tables = yaml.safe_load(file)

    for i in info_about_tables.keys():
        load_data = LoadingDataHTTPtoPG(info_about_tables[i])
        load_data.__logger__(filename="log/load_data")
        load_data.get_data_to_pandas()
        load_data.add_tech_columns()
        load_data.connect_to_pg()
        load_data.insert_table()
        load_data.check_added_data()
        load_data.connection_close()


main()
