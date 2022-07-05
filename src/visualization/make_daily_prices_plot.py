"""Crea un gráfico con los datos diarios del precio de la energía"""
import pathlib

from utils import read_format_prices, make_prices_figure

base_path = pathlib.Path.cwd()
business_path = base_path.joinpath("data_lake/business")
figures_path = base_path.joinpath("data_lake/business/reports/figures")


def make_daily_prices_plot(source_path=business_path, target_path=figures_path):
    """Crea un grafico de lines que representa los precios promedios diarios
    Usando el archivo data_lake/business/precios-diarios.csv, crea un grafico de
    lines que representa los precios promedios diarios.
    El archivo se guarda en formato PNG en
    data_lake/business/reports/figures/daily_prices.png.
    """
    df_daily_prices = read_format_prices(source_path, "precios-diarios.csv")

    label_line = "Precio"
    xlabel = "Fecha [días]"
    ylabel = "Precio [COP]"
    tilte = "Precio promedio diario de la energía"
    fig_path = target_path.joinpath("daily_prices.png")

    make_prices_figure(df_daily_prices, fig_path, label_line, tilte, xlabel, ylabel)


if __name__ == "__main__":
    import doctest

    make_daily_prices_plot()
    doctest.testmod()
