import pathlib

from src.utils import make_folder


def create_data_lake(folder_structure):
    """This function allows to create the initial folder structure of the datalake defined
    in the 'initial_structure' parameter of the configuration file
    ./src/config/config_initial_datalake_folder_structure.json
    """

    for folder in folder_structure:
        folder = pathlib.Path(folder)
        make_folder(folder)


if __name__ == "__main__":
    import doctest

    doctest.testmod()
