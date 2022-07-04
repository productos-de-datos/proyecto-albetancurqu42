import pandas as pd

import matplotlib.pyplot as plt
import matplotlib.dates as mdates


def read_format_prices(source_path, filename):
    df_source = pd.read_csv(source_path.joinpath(filename), index_col=0)
    df_source.index = pd.to_datetime(df_source.index)
    df_source = df_source.dropna()
    return df_source


def make_prices_figure(df_daily_prices, fig_path, label_line, tilte, xlabel, ylabel):
    fig = plt.figure(figsize=(16, 6))
    ax = plt.subplot()
    ax.plot(df_daily_prices.index, df_daily_prices.precio, label=label_line)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.xaxis.set_major_formatter(
        mdates.ConciseDateFormatter(ax.xaxis.get_major_locator())
    )
    ax.legend()
    ax.set_title(tilte)
    plt.grid()
    plt.savefig(fig_path, bbox_inches="tight")
    # plt.show()
    plt.close("all")
