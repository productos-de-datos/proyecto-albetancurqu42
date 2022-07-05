"""Modulo de entrenamiento de modelos"""
import pathlib

import pandas as pd
from sklearn import preprocessing
from sklearn.decomposition import PCA
from sklearn.model_selection import TimeSeriesSplit, GridSearchCV
from sklearn.pipeline import Pipeline
from sklearn.svm import SVR

from utils import (
    get_x_y,
    time_train_test_split,
    save_model_pickle,
    read_format_daily_prices,
)

base_path = pathlib.Path.cwd()
business_path = base_path.joinpath("data_lake/business/features")
features_path = base_path.joinpath("models")
features_path.mkdir(exist_ok=True)

model_parameters = (
    {
        "model_name": "SVR",
        "estimator": SVR(),
        "model_parameters_grid": [
            {
                "estimator__kernel": ["rbf"],
                "estimator__gamma": [1e-3, 1e-4],
                "estimator__C": [1, 10, 100, 1000],
            },
            {"estimator__kernel": ["linear"], "estimator__C": [1, 10, 100, 1000]},
        ],
        "model_parameters_best": {
            "estimator__C": [100],
            "estimator__kernel": ["linear"],
        },
    },
)
model_parameters = model_parameters[0]


def scale_dataframe(df_source, target_path, quantile_range=(10, 90)):
    """Escala los datos usando un rango de percentiles"""
    scaler = preprocessing.RobustScaler(
        with_centering=True, with_scaling=True, quantile_range=quantile_range
    )
    scaler.fit(df_source)

    model_path = target_path.joinpath("scaler_y.pkl")
    save_model_pickle(scaler, model_path)

    output_scaled = scaler.transform(df_source)
    df_target = pd.DataFrame(
        output_scaled, index=df_source.index, columns=df_source.columns
    )
    return df_target


def get_pipeline(model, quantile_range=(10, 90), n_components=10):
    """Estructura el pipeline de los modelos"""
    scaler_pipeline = preprocessing.RobustScaler(
        with_centering=True, with_scaling=True, quantile_range=quantile_range
    )
    pca_pipeline = PCA(n_components=n_components)
    pipeline = Pipeline(
        steps=[("scaler", scaler_pipeline), ("pca", pca_pipeline), ("estimator", model)]
    )
    return pipeline


def get_train_grid_search_cv(
    x_train, y_train, pipeline, grid_parameters, gap, n_splits=10
):
    """Estructura la calibración de hiperparámetros"""
    tscv = TimeSeriesSplit(n_splits=n_splits, gap=gap)
    clf = GridSearchCV(
        estimator=pipeline,
        param_grid=grid_parameters,
        scoring="neg_mean_squared_error",
        n_jobs=-1,
        refit=True,
        cv=tscv,
        verbose=0,
    )
    clf.fit(x_train, y_train.to_numpy().ravel())
    return clf


def train_daily_model(
    model_dict: dict = model_parameters,
    source_path=business_path,
    target_path=features_path,
    gap=60,
    train_with_grid=False,
):
    """Entrena el modelo de pronóstico de precios diarios.

    Con las features entrena el modelo de pronóstico de precios diarios y
    lo guarda en models/precios-diarios.pkl
    """
    df_daily_prices = read_format_daily_prices(source_path)
    x_source, y_source = get_x_y(df_daily_prices)

    x_train, y_train, _, _ = time_train_test_split(x_source, y_source, gap)

    y_train_scaled = scale_dataframe(y_train, target_path)

    model = model_dict["estimator"]
    if train_with_grid is True:
        grid_parameters = model_dict["model_parameters"]
    else:
        grid_parameters = model_dict["model_parameters_best"]

    pipeline = get_pipeline(model)

    clf = get_train_grid_search_cv(
        x_train, y_train_scaled, pipeline, grid_parameters, gap
    )
    model_path = target_path.joinpath("precios-diarios.pkl")
    save_model_pickle(clf, model_path)


if __name__ == "__main__":
    import doctest

    train_daily_model()
    doctest.testmod()
