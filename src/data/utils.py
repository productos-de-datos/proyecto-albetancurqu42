import json
import pathlib
import requests

PATH_CONFIG_FILES = pathlib.Path(__file__).parent.resolve().parent.joinpath('config')


def initialize_reading_configuration_json(filename_config, config_base_path=None):
    if config_base_path is None:
        config_base_path = pathlib.Path(PATH_CONFIG_FILES)

    with open(config_base_path.joinpath(filename_config), "r") as config_file:
        config = json.load(config_file)

    return config


def read_property_from_config(
    config,
    section_name,
    parameter_name
):

    return config[section_name][parameter_name]


def make_folder(folder_name: pathlib.Path):

    try:
        folder_name.mkdir(parents=False, exist_ok=True)
    except FileNotFoundError:
        print(f'''The folder structure is not correct, the parent folder of {folder_name} is not
         created ''')


def download_file_from_url(url, storage_path):
    response = requests.get(url)

    if response.status_code == 404:
        raise Exception(f'The url is not valid: {url}')

    elif response.status_code == 200:
        output = open(str(storage_path), 'wb')
        output.write(response.content)
        output.close()
