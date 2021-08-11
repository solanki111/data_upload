import os
import datetime
import logging
from configs.constants import LOGS, TIME_FORMAT, TXT, RESOURCES


class Logging:

    def __init__(self):
        pass

    # base_path = Path(__file__).parent.parent.parent
    logs_path = os.path.join(os.path.abspath(''), RESOURCES, LOGS)
    file_name = LOGS + "_" + datetime.datetime.now().strftime(TIME_FORMAT) + TXT
    file_name_path = os.path.join(logs_path, file_name)
    logging.basicConfig(filename=file_name_path, filemode='w', format='%(asctime)s - %(message)s',
                        datefmt='%d-%m-%y %H:%M:%S', level=logging.DEBUG)
    logger = logging
