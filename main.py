# -*- mode: python ; coding: utf-8 -*-

import os
import sys
from scripts.util import run_cmd
from scripts.logger import Logging
from scripts.hiduu_upload import hiduu_file_upload
from scripts.zone_cache import get_active_NN, accumulate_data_local
# from scripts.send_notification import sending_alert_notification_via_email
from configs.constants import DK_NAMENODES, SH_NAMENODES, DATASINK, CACHE, SRC_FILE, DES_FILE


def main():
    """ Script to upload the files from deku's cache to sheik's datasink. """
    Logging.logger.info('File upload process started')
    src_active_NN = get_active_NN(DK_NAMENODES, CACHE, SRC_FILE)
    des_active_NN = get_active_NN(SH_NAMENODES, DATASINK, DES_FILE)
    if not src_active_NN:
        Logging.logger.error('None of the given namenode for source (deku) was active!')
        # sending_alert_notification_via_email('None of the given namenode for source (deku) was active!')
        sys.exit('Aborting the process...')
    Logging.logger.info(('The active NameNode for source (deku) is: ' + src_active_NN))

    if not des_active_NN:
        Logging.logger.error('None of the given namenode for destination (sheik) was active!')
        # sending_alert_notification_via_email('None of the given namenode for destination (sheik) was active!')
        sys.exit('Aborting the process...')
    Logging.logger.info(('The active NameNode for destination (sheik) is: ' + des_active_NN))

    src_filepath = src_active_NN + CACHE + '/' + SRC_FILE
    tmp_dir, client_list = accumulate_data_local(src_filepath, 0)

    if os.path.exists(tmp_dir) and os.path.isdir(tmp_dir):
        if not os.listdir(tmp_dir):
            Logging.logger.error('No folders are found in temp directory at: {0}'.format(tmp_dir))
            # sending_alert_notification_via_email('No folders are found in temp directory at: {0}'.format(tmp_dir))
            sys.exit('Aborting the process...')
        else:
            Logging.logger.info('Folders are created successfully in temp directory per client.')
    else:
        Logging.logger.error("temp directory wasn't created!")
        # sending_alert_notification_via_email("temp directory wasn't created!")
        sys.exit('Aborting Process...')

    hiduu_file_upload(tmp_dir, client_list)
    Logging.logger.info('Files have been uploaded successfully via HIDUU.')
    run_cmd(['rm', '-rf', tmp_dir])


if __name__ == '__main__':
    """ Triggers data upload pipeline """
    main()
