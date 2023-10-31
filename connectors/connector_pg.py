import psycopg2
import yaml


class ConnectorPG:

    def __init__(self, log, dbname):
        with open('settings.yaml', 'r') as file:
            settings = yaml.safe_load(file)

        self.user = settings['postgres']['user']
        self.host = settings['postgres']['host']
        self.password = settings['postgres']['password']

        self.log = log

        try:
            self.log.info(f'Connecting to {dbname} ...')
            self.connection = psycopg2.connect(database=dbname,
                                               user=self.user,
                                               host=self.host,
                                               password=self.password)
        except psycopg2.OperationalError as err:
            self.log.error(f'{err}')
        else:
            self.log.info(f'Connection to server at {self.host}, database {dbname} successfully')
            self.connection.autocommit = True
            self.log.info(f'connection.autocommit = True')
            self.cursor = self.connection.cursor()

    def connection_close(self):
        self.connection.close()
        self.log.info('Connection postgres close')
