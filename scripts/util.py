import sys
import subprocess
from scripts.logger import Logging
from scripts.send_notification import sending_alert_notification_via_email


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
        sending_alert_notification_via_email('Error: Command failed: ' + s_err + ' and the stacktrace: ' + cmd_out)
        sys.exit('Encountered Error: ' + s_err + 'Exiting the process...')
    else:
        Logging.logger.error('Command returned code: ' + s_return)
