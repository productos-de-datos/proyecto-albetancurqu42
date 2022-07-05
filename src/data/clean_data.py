"""Modulo de limpieza de datos"""
import pathlib

import pandas as pd

base_path = pathlib.Path.cwd()  # .parent.parent
raw_path = base_path.joinpath("data_lake/raw")
cleansed_path = base_path.joinpath("data_lake/cleansed")


def read_prepare_csv(file):
    """Lee y configura el formato de fechas de una archivo csv"""
    df_raw = pd.read_csv(file, index_col=0, header=0)
    df_raw.index = pd.to_datetime(df_raw.index)
    df_raw.columns = [int(hour[1:]) for hour in df_raw.columns]
    return df_raw


def stack_dataframe(df_source):
    """Transforma datos horarios
    Pasa de un archivo horario donde el indice son las fechas y las columnas las horas,
    a uno donde el indece tiene fecha y hora, con una única columna que es el precio
    respectivo de la energía"""

    df_stack = df_source.copy()

    df_stack = df_stack.stack()
    df_stack = df_stack.reset_index(level=[0, 1])

    df_stack["level_0"] += pd.to_timedelta(df_stack.level_1, unit="h")
    df_stack = df_stack.set_index("level_0")
    df_stack = df_stack.drop(columns="level_1")

    df_stack.columns = ["precio"]
    df_stack.index.name = "fecha"

    return df_stack


def format_dataframe_prices(df_source):
    """Asigna formato al dataframe de precios horarios para guardarlo"""
    df_price = df_source.copy()

    df_price = df_price.asfreq("H")

    df_price.loc[:, "hora"] = df_price.index.hour
    df_price.loc[:, "hora"] = df_price.loc[:, "hora"].astype(str)
    df_price.loc[:, "hora"] = df_price.loc[:, "hora"].str.zfill(2)

    df_price.index.name = "fecha"
    df_price = df_price.filter(["hora", "precio"], axis=1)

    return df_price


def clean_data(source_path=raw_path, target_path=cleansed_path):
    """Realiza la limpieza y transformación de los archivos CSV.

    Usa los archivos data_lake/raw/*.csv, y crea el archivo
    data_lake/cleansed/precios-horarios.csv.
    Las columnas de este archivo son:

    * fecha: fecha en formato YYYY-MM-DD
    * hora: hora en formato HH
    * precio: precio de la electricidad en la bolsa nacional
    """

    list_csv_files = sorted(list(source_path.glob("*.csv")))

    df_price = pd.DataFrame(columns=["precio"])

    for excel_file in list_csv_files:
        df_raw = read_prepare_csv(excel_file)

        df_stack = stack_dataframe(df_raw)

        df_price = pd.concat([df_price, df_stack], axis=0)

    df_price = format_dataframe_prices(df_price)

    df_price.to_csv(target_path.joinpath("precios-horarios.csv"))


if __name__ == "__main__":
    import doctest

    clean_data()
    doctest.testmod()
