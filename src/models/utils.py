import pickle

import pandas as pd


def read_format_daily_prices(source_path):
    df_source = pd.read_csv(source_path.joinpath("precios-diarios.csv"), index_col=0)
    df_source.index = pd.to_datetime(df_source.index)
    df_source = df_source.dropna()
    return df_source


def get_x_y(df_source, y_columns=None):
    if y_columns is None:
        y_columns = ["precio"]
    x_source = df_source.copy().drop(columns=y_columns)
    y_source = df_source.copy().loc[:, y_columns]
    return x_source, y_source


def time_train_test_split(x_all, y_all, gap, train_size=0.7):
    end_data_train = int(len(x_all) * train_size)
    star_data_test = end_data_train + gap
    x_train = x_all.iloc[:end_data_train]
    y_train = y_all.iloc[:end_data_train]
    x_test = x_all.iloc[star_data_test:]
    y_test = y_all.iloc[star_data_test:]
    return x_train, y_train, x_test, y_test


def save_model_pickle(model, model_path):
    with open(str(model_path), "wb") as output:
        pickle.dump(model, output)


def load_model_pickle(model_path):
    with open(str(model_path), "rb") as output:
        model = pickle.load(output)
        return model
