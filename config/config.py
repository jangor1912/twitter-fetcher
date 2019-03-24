import os

import yaml


def get_config(config_file_path="config/config.yml"):
    while True:
        try:
            with open(config_file_path, 'r') as ymlfile:
                config = yaml.load(ymlfile)
                break
        except FileNotFoundError:
            os.chdir("..")
    return config
