""" script to upload data via HIDUU """
import os
import glob
import datetime
from scripts.util import run_cmd
from scripts.logger import Logging
from scripts.file_reader import get_property_file
from configs.constants import EMPTY_STR, HIDUU_PROCESS_START, HIDUU_PROCESS_END, HIDUU_PROCESS_ERROR_OCCURRED, CONFIG, CSV


def get_all_files(clients_file_folder):
    """
        Function that reads folder and gets contents of it.
        :param clients_file_folder: Directories of clients containing cache data.
    """
    list_of_files = glob.glob(clients_file_folder + '/value-*.txt')  # * means all if need specific format then *.csv
    latest_file = max(list_of_files, key=os.path.getctime)
    log_file_path = latest_file
    log_file = os.path.basename(log_file_path)
    return log_file_path, log_file


def hiduu_file_upload(tmp_dir, client_list):
    """
        Function that does the HIDUU process by uploading the text file based on the
         template provided. The output will bw written in the log file.
        :param tmp_dir: location of temporary directory which all needs to be processed via hiduu.
        :param client_list: Dictionary of clients with sources in array as values
    """
    prop_file = get_property_file(EMPTY_STR)
    date_val = datetime.date.today()
    param_release_key = prop_file['HIDUU_RELEASE']
    release_key = date_val.__str__().replace('-', '')
    hiduu_path = prop_file['HIDUU_EXEC_PATH_UNIX']
    hiduu_exec_path = os.path.join(os.path.abspath('../..'), hiduu_path)
    param_upload_dataset = prop_file['UPLOAD_DATASET']
    param_sys_acc_id = prop_file['SYSTEM_ACC_ID']
    param_sys_acc_secret = prop_file['SYS_ACC_SECRET']
    param_source_id = prop_file['SOURCE_ID']
    param_client_mnemonic = prop_file['HIDUU_CLIENT_MNEMONIC']
    param_dataset_id = prop_file['HIDUU_DATASET_ID']
    param_spec_version = prop_file['HIDUU_SPEC_VERSION']
    spec_version = prop_file['HIDUU_SPEC_VER_VALUE']
    param_file_id = prop_file['HIDUU_FILE_ID']
    file_id = prop_file['HIDUU_FILE_ID_VALUE']
    param_file_key = prop_file['HIDUU_FILE']
    config_file = get_property_file(CONFIG)
    for cl, sources in client_list.iteritems():
        sys_accounts = config_file[cl]
        sys_acc_key = sys_accounts['key']
        sys_acc_secret = sys_accounts['secret']
        for src in sources:
            dataset_id = prop_file['EDW_DATA_SET'] + cl
            upload_file_path = os.path.join(tmp_dir, cl, '{0}{1}'.format(src, CSV))
            try:
                Logging.logger.info(HIDUU_PROCESS_START + src)
                run_cmd([hiduu_exec_path, param_upload_dataset, param_sys_acc_id, sys_acc_key, param_sys_acc_secret,
                         sys_acc_secret, param_source_id, src, param_client_mnemonic, cl, param_dataset_id, dataset_id,
                         param_spec_version, spec_version, param_file_id, file_id, param_release_key, release_key,
                         param_file_key, upload_file_path])

                Logging.logger.info('HIDUU upload succeeded!')
            except Exception as ex:
                Logging.logger.error(HIDUU_PROCESS_ERROR_OCCURRED + ex.__str__())

    Logging.logger.info(HIDUU_PROCESS_END)
