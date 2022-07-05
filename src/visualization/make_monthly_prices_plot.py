"""Crea un gráfico con los datos mensuales del precio de la energía"""

import pathlib

from utils import read_format_prices, make_prices_figure

base_path = pathlib.Path.cwd()
business_path = base_path.joinpath("data_lake/business")
figures_path = base_path.joinpath("data_lake/business/reports/figures")


def make_monthly_prices_plot(source_path=business_path, target_path=figures_path):
    """Crea un grafico de lines que representa los precios promedios diarios.

    Usando el archivo data_lake/business/precios-diarios.csv, crea un grafico de
    lines que representa los precios promedios diarios.

    El archivo se debe salvar en formato PNG en
    data_lake/business/reports/figures/monthly_prices.png.

    """
    df_monthly_prices = read_format_prices(source_path, "precios-mensuales.csv")

    label_line = "Precio"
    xlabel = "Fecha [meses]"
    ylabel = "Precio [COP]"
    tilte = "Precio promedio mensual de la energía"
    fig_path = target_path.joinpath("monthly_prices.png")

    make_prices_figure(df_monthly_prices, fig_path, label_line, tilte, xlabel, ylabel)


if __name__ == "__main__":
    import doctest

    make_monthly_prices_plot()
    doctest.testmod()
