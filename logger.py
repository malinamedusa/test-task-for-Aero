from logging import info, error
import logging


class Logging:

    def __init__(self, filename, load_data, load_hour):
        self.info = info
        self.error = error
        filename = f'{filename}{load_data}_{load_hour}h.log'
        logging.getLogger(__name__)
        logging.basicConfig(level=logging.INFO,
                            filename=filename,
                            filemode='a',
                            format='%(asctime)s %(levelname)s %(message)s')
        self.log = logging
        self.log.info('Logger is started')
