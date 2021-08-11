""" script to upload data via HIDUU """
import os
import sys
import json
import datetime
import logging
import subprocess
from configs.constants import EMPTY_STR, HIDUU_PROCESS_START, HIDUU_PROCESS_END, HIDUU_PROCESS_ERROR_OCCURRED, \
    CSV, CLIENTS_LIST_FINAL_WIN

TXT = '.txt'
LOGS = 'logs'
CONFIG = 'configs'
RESOURCES = 'resources'
TIME_FORMAT = '%Y%m%d-%H%M%S'
CONFIGS_FILE = 'configs/config.json'
CONFIG_PROP_FILE = 'configs/property.json'
FETCH_FILE_FAILED = 'Failed to fetch yaml file '
FILE_DOES_NOT_EXIST = 'The file or folder does not exist.  '


class Logging:
    def __init__(self):
        pass

    logs_path = os.path.join(os.path.abspath('../../'), RESOURCES, LOGS)
    print(logs_path)
    file_name = LOGS + "_" + datetime.datetime.now().strftime(TIME_FORMAT) + TXT
    file_name_path = os.path.join(logs_path, file_name)
    logging.basicConfig(filename=file_name_path, filemode='w', format='%(asctime)s - %(message)s',
                        datefmt='%d-%m-%y %H:%M:%S', level=logging.DEBUG)
    logger = logging


def run_cmd(args_list):
    """
        Function to run linux/hdfs commands
    """
    Logging.logger.info('Running system command: {0}'.format(' '.join(args_list)))
    proc = subprocess.Popen(args_list, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            universal_newlines=True)
    s_output, s_err = proc.communicate()
    s_return = proc.returncode
    if s_return != 0:
        cmd_out = str(s_output.split('\n'))
        Logging.logger.error('Command failed with the error: ' + s_err + ' and the stacktrace: ' + cmd_out)
        sys.exit('Encountered Error: ' + s_err + 'Exiting the process...')
    else:
        Logging.logger.error('Command returned code: ' + str(s_return))


def get_property_file(file_type):
    """
        Function to read property yaml file.
        :param file_type:
    """
    pro_file_name = CONFIGS_FILE if file_type == CONFIG else CONFIG_PROP_FILE
    config_file = os.path.join(os.path.abspath('../'), pro_file_name)
    Logging.logger.info('{0} file found at: {1}'.format(config_file.split('/')[1].split('.')[0], config_file))
    data_loaded = None
    try:
        with open(str(config_file), 'r') as f:
            data_loaded = json.load(f)

    except StandardError as error:
        Logging.logger.error(FETCH_FILE_FAILED + config_file + ' ' + error.__str__())
        sys.exit('Encountered Error: ' + error.__str__() + ' Exiting the process...')
    except IOError as ex:
        Logging.logger.error(FETCH_FILE_FAILED + config_file + ' ' + ex.__str__())
        sys.exit('Encountered Error: ' + ex.__str__() + ' Exiting the process...')
    return data_loaded


def hiduu_file_upload(tmp_dir):
    """
        Function that does the HIDUU process by uploading the text file based on the
         template provided. The output will bw written in the log file.
        :param tmp_dir: location of temporary directory which all needs to be processed via hiduu.
    """
    prop_file = get_property_file(EMPTY_STR)
    date_val = datetime.date.today()
    param_release_key = prop_file['HIDUU_RELEASE']
    release_key = date_val.__str__().replace('-', '')
    hiduu_exec_path = prop_file['HIDUU_EXE_PATH_WINDOWS']
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
    for cl, sources in CLIENTS_LIST_FINAL_WIN.iteritems():
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


hiduu_file_upload('path to temp file')
