"""
Construya un pipeline de Luigi que:

* Importe los datos xls
* Transforme los datos xls a csv
* Cree la tabla unica de precios horarios.
* Calcule los precios promedios diarios
* Calcule los precios promedios mensuales

En luigi llame las funciones que ya creo.


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
datalake_path = base_path.joinpath('data_lake')


class PipelineIngestData(Task):
    def output(self):
        return LocalTarget(str(datalake_path.joinpath('landing', 'result_ingest_data.txt')))

    def run(self):
        with self.output().open("w") as outfile:
            ingest_data()
            outfile.write("Ok PipelineIngestData")


class PipelineTransformData(Task):
    def requires(self):
        return PipelineIngestData()

    def output(self):
        return LocalTarget(str(datalake_path.joinpath('raw', 'result_transform_data.txt')))

    def run(self):
        with self.output().open("w") as outfile:
            transform_data()
            outfile.write("Ok PipelineTransformData")


class PipelineCleanData(Task):
    def requires(self):
        return PipelineTransformData()

    def output(self):
        return LocalTarget(str(datalake_path.joinpath('cleansed', 'result_clean_data.txt')))

    def run(self):
        with self.output().open("w") as outfile:
            clean_data()
            outfile.write("Ok PipelineCleanData")


class PipelineDailyPricesReport(Task):
    def requires(self):
        return PipelineCleanData()

    def output(self):
        return LocalTarget(
            str(datalake_path.joinpath('business', 'result_compute_daily_prices.txt'))
        )

    def run(self):
        with self.output().open("w") as outfile:
            compute_daily_prices()
            outfile.write("Ok PipelineDailyPricesReport")


class PipelineMonthlyPricesReport(Task):
    def requires(self):
        return PipelineCleanData()

    def output(self):
        return LocalTarget(
            str(datalake_path.joinpath('business', 'result_compute_monthly_prices.txt'))
        )

    def run(self):
        with self.output().open("w") as outfile:
            compute_monthly_prices()
            outfile.write("Ok PipelineMonthlyPricesReport")


class ReportsPrices(Task):
    def requires(self):
        return [PipelineDailyPricesReport(), PipelineMonthlyPricesReport()]


# if __name__ == "__main__":
#
#     luigi.run(["ReportsPrices", "--local-scheduler"])


if __name__ == "__main__":
    import doctest

    luigi.run(["ReportsPrices", "--local-scheduler"])
    doctest.testmod()
