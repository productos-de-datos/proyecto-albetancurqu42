import pathlib

import pandas as pd

from utils import (
    get_x_y,
    load_model_pickle,
    read_format_daily_prices,
)

base_path = pathlib.Path.cwd()
business_path = base_path.joinpath("data_lake/business/features")
models_pickle_path = base_path.joinpath("models")
forecasts_path = base_path.joinpath("data_lake/business/forecasts")


def predict_outputs(models_path, x_source):
    model_path = models_path.joinpath("precios-diarios.pkl")
    model = load_model_pickle(model_path)
    y_predict = model.predict(x_source)
    return y_predict


def de_escalate_outputs(models_path, y_predict, y_source):
    scaler_path = models_path.joinpath("scaler_y.pkl")
    scaler_y = load_model_pickle(scaler_path)
    y_predict = scaler_y.inverse_transform(y_predict.reshape(-1, 1))
    df_y_predict = pd.DataFrame(
        y_predict.ravel(), index=y_source.index, columns=["precio_pronostico"]
    )
    return df_y_predict


def make_forecasts(
    source_path=business_path,
    models_path=models_pickle_path,
    target_path=forecasts_path,
):
    """Construya los pronosticos con el modelo entrenado final.

    Cree el archivo data_lake/business/forecasts/precios-diarios.csv. Este
    archivo contiene tres columnas:

    * La fecha.
    * El precio promedio real de la electricidad.
    * El pronóstico del precio promedio real.
    """
    df_daily_prices = read_format_daily_prices(source_path)
    x_source, y_source = get_x_y(df_daily_prices)

    y_predict = predict_outputs(models_path, x_source)

    df_y_predict = de_escalate_outputs(models_path, y_predict, y_source)

    df_output = pd.concat([y_source, df_y_predict], axis=1)

    df_output.to_csv(target_path.joinpath("precios-diarios.csv"))


if __name__ == "__main__":
    import doctest

    make_forecasts()
    doctest.testmod()
