"""script to fetch data from zone cache"""
import os
import sys
import json
import urllib2
from scripts.util import run_cmd
from scripts.logger import Logging
# from scripts.send_notification import sending_alert_notification_via_email
from configs.constants import OPEN, TEMP, LENGTH, STATUS, SOURCE, STRING, SUFFIX, EMPTY_STR, FILE_HEADER, FILE_STATUS,\
    FILE_STATUSES, CLIENTS_LIST, MKDIR, CSV


def get_active_NN(namenodes, path, folder):
    """
        Function to determine the active namenode from a list.
        :param namenodes: url and port of the active namemode
        :param path: the path of the folder that needs to present
        :param folder: the folder name
        :return: active namemode if found otherwise process will exit
    """

    for nn in namenodes:
        url = nn + path + STATUS
        Logging.logger.info('Sending http request to ' + path + " '" + url + "'")
        try:
            request = urllib2.urlopen(url)
            file_status = json.loads(request.read())
            if FILE_STATUSES in file_status.keys():
                if FILE_STATUS in file_status[FILE_STATUSES].keys():
                    response = file_status[FILE_STATUSES][FILE_STATUS]
                    for res in response:
                        if res[SUFFIX] == folder:
                            Logging.logger.info("Folder present at '" + res[SUFFIX] + "'")
                            return nn

        except urllib2.HTTPError as e:
            Logging.logger.error('http request failed with the stacktrace: ' + str(e))

    Logging.logger.error('No active namenode found among: {0}!'.format(str(namenodes)))
    # sending_alert_notification_via_email('No active namenode found among: {0}!'.format(str(namenodes)))
    sys.exit('Exiting the process...')


def accumulate_data_local(src_filepath, max_bytes):
    """
        Function to accumulate data based on source id and send the data to /datasink/analytics/
        :param src_filepath: source file uri path
        :param max_bytes: number of bytes
        :return: temp directory path, list of all clients and their respective sources found in deku zone cache.
    """
    if max_bytes > 0:
        max_bytes = LENGTH + str(max_bytes)
    else:
        max_bytes = EMPTY_STR

    # Build webhdfs url to make a http request like below
    # "http://cz34299cly.cernuksphere-ld5.net:50070/webhdfs/v1/ds_cache/zone0_sheik_midas_dscache.dat?op=OPEN"
    url = src_filepath.strip() + OPEN + max_bytes
    Logging.logger.info("Opening file via webhdfs: '" + url + "'")
    Logging.logger.info('Beginning to read the file...')
    row_count = 0
    file_dict = {}
    for line in urllib2.urlopen(url):
        components = line.split()
        if not components or len(components) < 8:
            continue

        row_count += 1
        file_size = components[4]
        file_date = components[5]
        file_time = components[6]
        raw_file = components[7]

        # creating a dictionary 'file_dict' to hold data in an array as values and each client as key
        for raw in raw_file.split('%2F'):
            if raw.find('%3A') != -1:
                com = raw.split('%3A')
                if com[0] == SOURCE and com[1] != STRING:
                    source_id = com[1]
                    if source_id in file_dict.keys():
                        file_dict[source_id].append([file_size, file_date, file_time, raw_file])
                    else:
                        arr = [file_size, file_date, file_time, raw_file]
                        file_dict[source_id] = [arr]

    Logging.logger.info('Number of valid rows processed: ' + str(row_count))

    # creating directories to store and segregate contents of file_dict per client with their respective sources
    client_list = {}
    tmp_dir = os.path.join(os.path.abspath(''), TEMP)
    if os.path.exists(tmp_dir) and os.path.isdir(tmp_dir):
        run_cmd(['rm', '-rf', TEMP])
    else:
        run_cmd([MKDIR, TEMP])
    for client, sources in CLIENTS_LIST.iteritems():
        source_ids = [src for src in sources if src in file_dict.keys()]
        if len(source_ids) == 0:
            continue

        client_list[client] = source_ids
        run_cmd([MKDIR, TEMP + client])
        Logging.logger.info('Directory created for the client: ' + client)
        for src in source_ids:
            output_file_name = '{0}/{1}/{2}{3}'.format(TEMP, client, src, CSV)
            Logging.logger.info('Writing data for the source: {0} at : {1}'.format(src, output_file_name))
            rows = file_dict[src]
            try:
                with open(output_file_name, 'w') as f:
                    f.write('{0}\n'.format(FILE_HEADER))
                    for r in rows:
                        f.write("{0}\n".format(" , ".join(r)))
            except IOError as ex:
                Logging.logger.error('Source: {0}, file not found!'.format(src) + str(ex))

    Logging.logger.info('Files have been accumulated locally per client.')
    Logging.logger.info('returning: tmp_dir path: {0} \n and the client_list dict: {1}'.format(tmp_dir, client_list))
    return tmp_dir, client_list
