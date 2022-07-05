"""Test de procesamiento de datos"""

import pathlib

import pandas as pd
from pandas.testing import assert_frame_equal

import sys

sys.path.append("src/data")

from compute_daily_prices import compute_daily_prices
from compute_monthly_prices import compute_monthly_prices


base_path = pathlib.Path.cwd()
cleansed_path = base_path.joinpath("tests/mocks")
business_path = base_path.joinpath("tests/mocks")


def test_compute_daily_prices():
    """Test de calculo de precios diarios"""
    compute_daily_prices(
        source_path=cleansed_path,
        target_path=business_path,
        source_filename="mock_precios-horarios.csv",
        target_namefile="output_precios-diarios.csv",
    )

    expected_file = pd.read_csv(
        cleansed_path.joinpath("mock_precios-diarios.csv"), index_col=0
    )
    out_file = pd.read_csv(
        cleansed_path.joinpath("output_precios-diarios.csv"), index_col=0
    )

    assert_frame_equal(expected_file, out_file)


def test_compute_monthly_prices():
    """Test de calculo de precios mensuales"""
    compute_monthly_prices(
        source_path=cleansed_path,
        target_path=business_path,
        source_filename="mock_precios-horarios.csv",
        target_namefile="output_precios-mensuales.csv",
    )

    expected_file = pd.read_csv(
        cleansed_path.joinpath("mock_precios-mensuales.csv"), index_col=0
    )
    out_file = pd.read_csv(
        cleansed_path.joinpath("output_precios-mensuales.csv"), index_col=0
    )

    assert_frame_equal(expected_file, out_file)
