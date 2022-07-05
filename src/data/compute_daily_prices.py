"""Calcula el precio de la energía diario a partir de los precio horarios"""
import pathlib

from utils import read_format_hourly_prices, resample_hourly_prices

base_path = pathlib.Path.cwd()
cleansed_path = base_path.joinpath("data_lake/cleansed")
business_path = base_path.joinpath("data_lake/business")


def compute_daily_prices(
    source_path=cleansed_path,
    target_path=business_path,
    source_filename="precios-horarios.csv",
    target_namefile="precios-diarios.csv",
):
    """Calcula los precios promedios diarios.

    Usael archivo data_lake/cleansed/precios-horarios.csv, calcula el precio
    promedio diario (sobre las 24 horas del dia) para cada uno de los dias. Las
    columnas del archivo data_lake/business/precios-diarios.csv son:

    * fecha: fecha en formato YYYY-MM-DD
    * precio: precio promedio diario de la electricidad en la bolsa nacional
    """

    df_hourly_prices = read_format_hourly_prices(source_path, filename=source_filename)

    df_daily_prices = resample_hourly_prices(df_hourly_prices, freq="D")

    df_daily_prices.to_csv(target_path.joinpath(target_namefile))


if __name__ == "__main__":
    import doctest

    compute_daily_prices()
    doctest.testmod()
