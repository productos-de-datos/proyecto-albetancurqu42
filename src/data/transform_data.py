import pathlib

import pandas as pd

base_path = pathlib.Path.cwd()
landing_path = base_path.joinpath('data_lake/landing')
raw_path = base_path.joinpath('data_lake/raw')


def get_index_col_of_value(df_source, value='Fecha'):
    df_where_value = df_source.eq(value)
    pos_index_value = df_where_value.loc[df_where_value.any(axis=1), :].index.values[0]
    pos_col_value = df_where_value.loc[:, df_where_value.any(axis=0)].columns.values[0]
    return pos_col_value, pos_index_value


def clean_dataframe(df_source, pos_col_value, pos_index_value, value='Fecha', limit_hours=24):
    df_clean = df_source.copy()
    df_clean = df_clean.loc[pos_index_value:, pos_col_value:]
    df_clean = df_clean.set_index(0)
    df_clean = df_clean.drop(index=value)

    if len(df_clean.columns) > limit_hours:
        df_clean = df_clean.iloc[:, :limit_hours]

    no_date_index = pd.to_datetime(df_clean.index, errors='coerce')
    df_clean = df_clean.loc[~no_date_index.isna()]
    df_clean.index = pd.to_datetime(df_clean.index)
    df_clean = df_clean[~df_clean.index.duplicated(keep='first')]

    df_clean.columns = [f'H{hour:02d}' for hour in range(0, limit_hours)]
    df_clean.index.name = None

    return df_clean


def transform_data(source_path=landing_path, target_path=raw_path, corner_value='Fecha'):
    """Transforme los archivos xls a csv.

    Transforme los archivos data_lake/landing/*.xls a data_lake/raw/*.csv. Hay
    un archivo CSV por cada archivo XLS en la capa landing. Cada archivo CSV
    tiene como columnas la fecha en formato YYYY-MM-DD y las horas H00, ...,
    H23.

    """
    list_excel_files = sorted(list(source_path.glob('*.xl*')))

    for excel_file in list_excel_files:
        df_raw = pd.read_excel(excel_file, index_col=None, header=None)

        pos_col_value, pos_index_value = get_index_col_of_value(df_raw, value=corner_value)

        df_clean = clean_dataframe(df_raw, pos_col_value, pos_index_value)

        df_clean.to_csv(target_path.joinpath(f'{excel_file.stem}.csv'))


if __name__ == "__main__":
    import doctest

    transform_data()
    doctest.testmod()
