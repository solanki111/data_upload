""" script to send email alerts in case of failures. """
import sys
import smtplib
import mimetypes
from email import encoders
from scripts.logger import Logging
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from scripts.file_reader import get_property_file
from configs.constants import EMPTY_STR, NX_LINE


def set_email_contents(prop_file, text):
    """
        Function to configure required email contents.
        :param prop_file:
        :param text:
    """
    msg = MIMEMultipart()
    recipients = prop_file.get('EMAIL_RECIPIENTS')
    msg['From'] = prop_file.get('EMAIL_SENDER')
    msg['To'] = ", ".join(recipients)
    msg['Subject'] = prop_file.get('EMAIL_SUBJECT')
    body = prop_file.get('EMAIL_BODY') + NX_LINE + text + NX_LINE*2 + prop_file.get('EMAIL_FOOTER')
    msg.attach(MIMEText(body, "plain"))
    return msg


def sending_alert_notification_via_email(text):
    """
        Function to send out email alert with text msg and logs as an attachment
        :param text:
    """
    Logging.logger.info('Generating email alert!')
    prop_file = get_property_file(EMPTY_STR)
    user = prop_file.get('EMAIL_USER')
    password = prop_file.get('EMAIL_PASSWORD')
    sender = prop_file.get('EMAIL_SENDER')
    to_list = prop_file.get('EMAIL_RECIPIENTS')
    file_path = Logging.file_name_path
    file_to_send = Logging.file_name
    Logging.logger.info('Preparing message for sending email alert...')
    msg = set_email_contents(prop_file, text)
    Logging.logger.info('Message prepared!')

    ctype, encoding = mimetypes.guess_type(file_to_send)
    if ctype is None or encoding is not None:
        ctype = prop_file.get('CTYPE')

    maintype, subtype = ctype.split("/", 1)
    try:
        with open(file_path, "rb") as fp:
            attachment = MIMEBase(maintype, subtype)
            attachment.set_payload(fp.read())
            encoders.encode_base64(attachment)
            attachment.add_header("Content-Disposition", "attachment", filename=file_to_send)
            msg.attach(attachment)
            server = smtplib.SMTP(prop_file.get('SMTP_HOST'))
            server.starttls()
            server.sendmail(sender, to_list, msg.as_string())
            server.quit()
            Logging.logger.info('Email sent!')

    except smtplib.SMTPException as e:
        Logging.logger.error(prop_file.get('SMTP_ERROR') + str(e))
        sys.exit('Aborting Process...')
