"""Calcula el precio de la energía mensual a partir de los precios horarios"""
import pathlib

from utils import read_format_hourly_prices, resample_hourly_prices

base_path = pathlib.Path.cwd()
cleansed_path = base_path.joinpath("data_lake/cleansed")
business_path = base_path.joinpath("data_lake/business")


def compute_monthly_prices(
    source_path=cleansed_path,
    target_path=business_path,
    source_filename="precios-horarios.csv",
    target_namefile="precios-mensuales.csv",
):
    """Calcula los precios promedios mensuales.

    Usando el archivo data_lake/cleansed/precios-horarios.csv, calcula el prcio
    promedio mensual. Las
    columnas del archivo data_lake/business/precios-mensuales.csv son:

    * fecha: fecha en formato YYYY-MM-DD
    * precio: precio promedio mensual de la electricidad en la bolsa nacional
    """
    df_hourly_prices = read_format_hourly_prices(source_path, filename=source_filename)

    df_monthly_prices = resample_hourly_prices(df_hourly_prices, freq="MS")

    df_monthly_prices.to_csv(target_path.joinpath(target_namefile))


if __name__ == "__main__":
    import doctest

    compute_monthly_prices()
    doctest.testmod()
