import pathlib

import pandas as pd

base_path = pathlib.Path.cwd()
business_path = base_path.joinpath("data_lake/business/")
features_path = base_path.joinpath("data_lake/business/features")


def lag_dataframe_column(df_source, column_to_lag="precio", n_lags=None):
    if n_lags is None:
        n_lags = [30, 60]

    list_lags = list(range(1, n_lags[0])) + list(range(n_lags[0], n_lags[1]))[::2]
    df_to_lag = df_source.copy().loc[:, [column_to_lag]]
    df_lags = pd.concat([df_to_lag.shift(shift) for shift in list_lags], axis=1)
    df_lags.columns = list_lags
    return df_lags


def make_features(source_path=business_path, target_path=features_path):
    """Prepara datos para pronóstico.

    Cree el archivo data_lake/business/features/precios-diarios.csv. Este
    archivo contiene la información para pronosticar los precios diarios de la
    electricidad con base en los precios de los días pasados. Las columnas
    correspoden a las variables explicativas del modelo, y debe incluir,
    adicionalmente, la fecha del precio que se desea pronosticar y el precio
    que se desea pronosticar (variable dependiente).

    En la carpeta notebooks/ cree los notebooks de jupyter necesarios para
    analizar y determinar las variables explicativas del modelo.

    """

    df_daily_prices = pd.read_csv(
        source_path.joinpath("precios-diarios.csv"), index_col=0
    )
    df_daily_prices.index = pd.to_datetime(df_daily_prices.index)

    df_features = lag_dataframe_column(df_daily_prices)
    df_features = pd.concat([df_daily_prices, df_features], axis=1)
    df_features.to_csv(target_path.joinpath("precios-diarios.csv"))


if __name__ == "__main__":
    import doctest

    make_features()
    doctest.testmod()
