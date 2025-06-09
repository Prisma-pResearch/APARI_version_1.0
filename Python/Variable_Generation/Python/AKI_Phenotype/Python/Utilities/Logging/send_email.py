# -*- coding: utf-8 -*-
"""
Module for sending emails via python.

Created on Tue Apr  7 14:04:34 2020.

@author: ruppert20
"""
from ..Encryption.file_encryption import load_encrypted_file
import smtplib
from email.mime.text import MIMEText
from email.header import Header
import os


def config_email(encrypted_dict_file_path: str = None, private_key_dir: str = None):

    # load config information
    private_keys: dict = load_encrypted_file(fp=encrypted_dict_file_path, key_fp=private_key_dir, force_dict=True)

    private_keys = private_keys.get('system_email', private_keys)

    try:
        # get email params
        email_paras = {}
        email_paras['user'] = private_keys['systemEmailUsername'].replace('/', '\\')
        email_paras['pwd'] = private_keys['systemEmailPassword']
        email_paras['smtp_server'] = private_keys['systemEmailSmtpServer']
        email_paras['smtp_port'] = private_keys['systemEmailSmtpPort']
        email_paras['isssl'] = private_keys['systemEmailSSLConfig']
        email_paras['sender'] = private_keys['systemEmailSender']

        return email_paras
    except:
        raise Exception('The dictionary is missing at least one of the required fields')


def send_email(body: str, email_paras: dict = None, subject: str = 'Data Processsing Update',
               recipients: list = None,
               encrypted_dict_file_path: str = None,
               private_key_dir: str = None):

    if isinstance(email_paras, dict):
        pass
    elif isinstance(encrypted_dict_file_path or os.environ.get('CONFIG_PATH'), str):
        email_paras = config_email(encrypted_dict_file_path=encrypted_dict_file_path, private_key_dir=private_key_dir)
    else:
        raise Exception('No Email Credential Provided')
    recipients = recipients if isinstance(recipients, list) else [recipients or os.environ.get('EMAIL')]
    """Sends email to given data."""
    user = email_paras['user']
    pwd = email_paras['pwd']
    smtp_server = email_paras['smtp_server']
    smtp_port = email_paras['smtp_port']
    isSSL = email_paras['isssl']
    try:
        if isSSL == 'True':
            server = smtplib.SMTP_SSL(smtp_server, smtp_port)
            server.ehlo()

        else:
            server = smtplib.SMTP(smtp_server, smtp_port)
            server.starttls()
            server.ehlo()

        server.login(user, pwd)
        message = MIMEText(body, 'html', 'utf-8')
        message['Subject'] = Header(subject)
        message['From'] = email_paras['sender']
        message['TO'] = str(recipients)
        server.sendmail(email_paras['sender'], recipients, message.as_string())
    except Exception as e:
        print(e)


if __name__ == '__main__':
    send_email(body='test body', subject='test message')
