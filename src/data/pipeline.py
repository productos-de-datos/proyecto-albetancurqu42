"""
Ejecución de pipeline de Luigi que realiza las siguientes tareas:

* Importa los datos xls
* Transforma los datos xls a csv
* Crea la tabla unica de precios horarios.
* Calcula los precios promedios diarios
* Calcula los precios promedios mensuales
"""

import pathlib

import luigi
from luigi import Task, LocalTarget

from ingest_data import ingest_data
from transform_data import transform_data
from clean_data import clean_data
from compute_daily_prices import compute_daily_prices
from compute_monthly_prices import compute_monthly_prices


base_path = pathlib.Path.cwd()
datalake_path = base_path.joinpath("data_lake")


class PipelineIngestData(Task):
    """Realiza la ingesta de los datos en el pipeline"""

    def output(self):
        return LocalTarget(
            str(datalake_path.joinpath("landing", "result_ingest_data.txt"))
        )

    def run(self):
        with self.output().open("w") as outfile:
            ingest_data()
            outfile.write("Ok PipelineIngestData")


class PipelineTransformData(Task):
    """Realiza la transformación de los datos en el pipeline"""

    def requires(self):
        return PipelineIngestData()

    def output(self):
        return LocalTarget(
            str(datalake_path.joinpath("raw", "result_transform_data.txt"))
        )

    def run(self):
        with self.output().open("w") as outfile:
            transform_data()
            outfile.write("Ok PipelineTransformData")


class PipelineCleanData(Task):
    """Realiza la limpieza de los datos en el pipeline"""

    def requires(self):
        return PipelineTransformData()

    def output(self):
        return LocalTarget(
            str(datalake_path.joinpath("cleansed", "result_clean_data.txt"))
        )

    def run(self):
        with self.output().open("w") as outfile:
            clean_data()
            outfile.write("Ok PipelineCleanData")


class PipelineDailyPricesReport(Task):
    """Realiza la transformación de los datos a escala diaria en el pipeline"""

    def requires(self):
        return PipelineCleanData()

    def output(self):
        return LocalTarget(
            str(datalake_path.joinpath("business", "result_compute_daily_prices.txt"))
        )

    def run(self):
        with self.output().open("w") as outfile:
            compute_daily_prices()
            outfile.write("Ok PipelineDailyPricesReport")


class PipelineMonthlyPricesReport(Task):
    """Realiza la transformación de los datos a escala mensual en el pipeline"""

    def requires(self):
        return PipelineCleanData()

    def output(self):
        return LocalTarget(
            str(datalake_path.joinpath("business", "result_compute_monthly_prices.txt"))
        )

    def run(self):
        with self.output().open("w") as outfile:
            compute_monthly_prices()
            outfile.write("Ok PipelineMonthlyPricesReport")


class ReportsPrices(Task):
    """Ejecuta el pipeline"""

    def requires(self):
        return [PipelineDailyPricesReport(), PipelineMonthlyPricesReport()]


# if __name__ == "__main__":
#
#     luigi.run(["ReportsPrices", "--local-scheduler"])


if __name__ == "__main__":
    import doctest

    luigi.run(["ReportsPrices", "--local-scheduler"])
    doctest.testmod()
