# -*- coding: utf-8 -*-
"""
Enryption Module.

Created on Thu May 28 15:35:10 2020

@author: ruppert20

YAML Encryption based on Modified version of https://pypi.org/project/cryptoyaml/.
"""
import os
import json
from cryptography.fernet import Fernet
from os import chmod, environ, path
import yaml
import sys
import re
from typing import Union


def generate_key(filepath: str):
    """
    Generate a new, random secret key at the given location on the filesystem and returns its path.

    Parameters
    ----------
    filepath : str
        file path to the secret file you would like to create.

    Returns
    -------
    fs : str
        file path to the new secret file reflected back at the user.

    """
    fs = path.abspath(path.expanduser(filepath))
    with open(fs, 'wb') as outfile:
        outfile.write(Fernet.generate_key())
    chmod(fs, 0o400)
    return fs


def get_key(key: str = None, keyfile: str = None):
    """
    Return a crytographic key.

    Parameters
    ----------
    key : str, optional
        verbatim cryptographic key. The default is None.
    keyfile : str, optional
        path to crytographic key. The default is None.

    Raises
    ------
    MissingKeyException
        Thrown when no key could be found.

    Returns
    -------
    key : str
        crytographic key.

    """
    if key is None:
        if (keyfile or environ.get('CONFIG_KEY')) is None:
            key = environ.get('Encryption_SECRET')
            if key is None:
                raise MissingKeyException(
                    '''You must either provide a key value,'''
                    ''' a path to a key or its value via the environment variable '''
                    '''Encryption_SECRET'''
                )
            else:
                key = key.encode('utf-8')
        else:
            keyfile = keyfile or environ.get('CONFIG_KEY')
            if os.path.isdir(keyfile):
                keyfile: str = os.path.join(keyfile, 'key.key')

            if os.path.exists(keyfile):
                key = open(keyfile, 'rb').read()
            else:
                key = generate_key(filepath=keyfile)

    return key


class MissingKeyException(Exception):
    """Missing Key Exception."""

    def __init__(self, msg):
        self.msg = msg


class CryptoYAML(object):
    """Representation of an encrypted YAML file."""

    def __init__(self, filepath, key=None, keyfile=None):
        self.filepath = path.abspath(path.expanduser(filepath))
        self.key = get_key(key, keyfile)
        assert self.key is not None
        self.fernet = Fernet(self.key)
        self.read()

    def read(self):
        """Read and decrypt data from the filesystem."""
        if path.exists(self.filepath):
            with open(self.filepath, 'rb') as infile:
                self.data = yaml.safe_load(self.fernet.decrypt(infile.read()))
        else:
            self.data = dict()

    def write(self):
        """Encrypt and write the current state back onto the filesystem."""
        with open(self.filepath, 'wb') as outfile:
            outfile.write(
                self.fernet.encrypt(
                    yaml.dump(self.data, encoding='utf-8')))

    def get_file_path(self, key: str, platform: str = None):
        """
        Get file path from config file.

        Parameters
        ----------
        key : str
            keys seperted by / to file_path. e.g. share_drive/idealist or local/home
        platform : str, optional
            Platform for which the key should be retrieved. The default is None. e.g. windows, msr_prod, msr_data, etc.

        Raises
        ------
        Exception
            DESCRIPTION.

        Returns
        -------
        out : str
            file path corresponding to input key.

        """
        if isinstance(platform, str):
            assert platform in list(self.data.get('file_paths').keys())
        else:
            platform: str = 'windows' if sys.platform == 'win32' else self.data.get('defaults', {}).get('linux_platform')

        for i, sk in enumerate(key.split('/')):
            if i == 0:
                out: dict = self.data.get('file_paths', {}).get(platform, {}).get(sk, {})
            else:
                out = out.get(sk, {})

        if isinstance(out, str):
            return out
        else:
            raise Exception(f"Unable to find file_path with key: {key}, available keys include: {self.data.get('file_paths', {}).get(platform, {})}")


def encrypt_dict(dictionary: dict,
                 key_dir: str = None,
                 key: str = None,
                 encrypt_decrypt: str = 'encrypt'):
    """
    Encrypt or decrypt dictionary.

    Parameters
    ----------
    dictionary : dict
        dictionary to be encrypted or decrypted.
    key_dir : str
        folder path for the key.key file or path to a specific key.
    encrypt_decrypt : str, optional
        Wheter to encrypt or decrypt the provided dictionary. The default is 'encrypt'.

    Returns
    -------
    dictionary : dict
        Encrypted or decrypted dictionary.

    """
    if os.path.isdir(key_dir):
        key_fp: str = os.path.join(key_dir, 'key.key')
    else:
        key_fp: str = key_dir
    if (encrypt_decrypt == 'encrypt') and (not os.path.exists(key_fp)):
        generate_key(filepath=key_fp)

    key = get_key(keyfile=key_dir, key=key)

    f = Fernet(key)

    for item in dictionary:
        try:
            temp = str(dictionary[item]).encode()
        except AttributeError:
            temp = dictionary[item]

        if encrypt_decrypt == 'encrypt':
            dictionary[item] = f.encrypt(temp).decode()
        else:
            temp_decrypt: str = f.decrypt(temp).decode()

            if temp_decrypt == 'False':
                dictionary[item] = False
            else:
                dictionary[item] = temp_decrypt

    return dictionary


def load_encrypted_dict(encrypted_dict_file_path: str, key_dir: str):
    """
    Load an enrypted JSON file.

    Parameters
    ----------
    encrypted_dict_file_path : str
        file path to an encrypted json file.
    key_dir : str
        directory where the key.key file can be found.

    Returns
    -------
    dict
        decrypted dictionary.

    """
    with open(encrypted_dict_file_path, 'r') as f:
        dictionary: dict = json.load(f)

    return encrypt_dict(dictionary=dictionary,
                        key_dir=key_dir,
                        encrypt_decrypt='decrypt')


def encrypt_and_save_dict(dictionary_file_path: str,
                          key_dir: str):
    """
    Encrypt dictionary and save it to a json file.

    Parameters
    ----------
    dictionary_file_path : str
        file path to save an encrypted json file.
    key_dir : str
        directory where the key.key file can be found.

    Returns
    -------
    None.

    """
    with open(dictionary_file_path, 'r') as f:
        dictionary: dict = json.load(f)

    temp = encrypt_dict(dictionary=dictionary,
                        key_dir=key_dir)

    with open(dictionary_file_path.replace('.json', '.json_aes'), 'w') as out_file:
        json.dump(temp, out_file)


def load_encrypted_file(fp: str = None, key_fp: str = None, force_dict: bool = False) -> Union[dict, CryptoYAML]:
    """
    Load encrypted file.

    Parameters
    ----------
    fp : str, optional
        file_path_to_encrypted json or yaml file. The default is None, which will load the encyted config file.
    key_fp : str, optional
        file path to the encryption key. The default is None, which will load the config encryption key.
    force_dict : bool, optional
        Whether CryptoYAML will be returned from encrypted yaml documents or a dictionary will be returned. The default is False.

    Returns
    -------
    Union[dict, CryptoYAML]
        dictionary if json or if force_dict is specified otherwise a CryptoYAML object.

    """
    fp = fp or environ.get('CONFIG_PATH')
    assert bool(re.search(r'\.json_aes$|\.yaml_aes$|\.yml_aes$', fp)), 'Only encrypted JSON and YAML documents are currently supported'

    if re.search(r'\.json_aes$', fp):
        return load_encrypted_dict(encrypted_dict_file_path=fp, key_dir=key_fp)

    eYaml: CryptoYAML = CryptoYAML(filepath=fp, keyfile=key_fp)

    if force_dict:
        return eYaml.data

    return eYaml


if __name__ == '__main__':
    pass
