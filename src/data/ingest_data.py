"""
Módulo de ingestión de datos.
-------------------------------------------------------------------------------

"""
import pathlib

import pandas as pd

import utils

datalake_config = utils.initialize_reading_configuration_json(
    filename_config='config_datalake.json'
)
raw_data_base_url = utils.read_property_from_config(
    config=datalake_config,
    section_name='data_source',
    parameter_name='base_url'
)

landing_path = pathlib.Path.cwd().joinpath('data_lake/landing')


def ingest_data(
        base_url=raw_data_base_url, initial_year: str = '1995', final_year: str = '2021',
        storage_base_path=landing_path
):
    """Ingestion of external data to the landing layer of the data lake.

    Del repositorio jdvelasq/datalabs/precio_bolsa_nacional/xls/ descarge los
    archivos de precios de bolsa nacional en formato xls a la capa landing. La
    descarga debe realizarse usando únicamente funciones de Python.

    """

    date_range = pd.date_range(initial_year, final_year, freq='AS')

    for year in date_range.year:
        try:
            download_url = f'{base_url}/{year}.xlsx'
            storage_path = storage_base_path.joinpath(f'{year}.xlsx')
            if not storage_path.is_file():
                utils.download_file_from_url(download_url, storage_path)

        except Exception:
            download_url = f'{base_url}/{year}.xls'
            storage_path = storage_base_path.joinpath(f'{year}.xls')
            if not storage_path.is_file():
                utils.download_file_from_url(download_url, storage_path)


if __name__ == "__main__":
    import doctest

    ingest_data()
    doctest.testmod()
