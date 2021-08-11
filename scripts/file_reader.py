""" script to read property file """
import os
import sys
import json
from scripts.logger import Logging
# from scripts.send_notification import sending_alert_notification_via_email
from configs.constants import CONFIG_PROP_FILE, FETCH_FILE_FAILED, CONFIG, CONFIGS_FILE


def get_property_file(file_type):
    """
        Function to read property yaml file.
        :param file_type:
    """
    file_name = CONFIGS_FILE if file_type == CONFIG else CONFIG_PROP_FILE
    config_file = os.path.join(os.path.abspath('src/'), file_name)
    Logging.logger.info('{0} file found at: {1}'.format(file_name.split('/')[1].split('.')[0], config_file))
    # data_loaded = None
    try:
        with open(str(config_file), 'r') as f:
            data_loaded = json.load(f)

    except StandardError as error:
        Logging.logger.error(FETCH_FILE_FAILED + config_file + ' ' + error.__str__())
        # sending_alert_notification_via_email(FETCH_FILE_FAILED + config_file + ' ' + error.__str__())
        sys.exit('Exiting the process...')
    except IOError as ex:
        Logging.logger.error(FETCH_FILE_FAILED + config_file + ' ' + ex.__str__())
        # sending_alert_notification_via_email(FETCH_FILE_FAILED + config_file + ' ' + ex.__str__())
        sys.exit('Exiting the process...')
    return data_loaded
