print('aa')

import pathlib
import utils


def create_data_lake():
    # TODO: Escribir docstring
    """This function allows to create the initial folder structure of the datalake defined
    in the 'initial_structure' parameter of the configuration file ./src/config/config_datalake.json
    """
    datalake_config = utils.initialize_reading_configuration_json(
        filename_config='config_datalake.json'
    )
    initial_folder_structure = utils.read_property_from_config(
        config=datalake_config,
        section_name='initial_structure',
        parameter_name='path_list'
    )

    print('ok')
    for folder in initial_folder_structure:
        print(folder)
        folder = pathlib.Path(folder)
        utils.make_folder(folder)

create_data_lake()

if __name__ == "__main__":
    import doctest

    doctest.testmod()
