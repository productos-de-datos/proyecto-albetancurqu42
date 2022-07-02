import json
import pathlib

# PATH_CONFIG_FILES = pathlib.Path().absolute().joinpath('configuracion')

PATH_CONFIG_FILES = './src/config/'


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