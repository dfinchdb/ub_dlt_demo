"""
Docs:
AutoLoader Options - https://docs.databricks.com/en/ingestion/auto-loader/options.html
CSV Options - https://docs.databricks.com/en/ingestion/auto-loader/options.html#csv-options
Data Quality - https://docs.databricks.com/en/delta-live-tables/expectations.html
Event Hooks - https://docs.databricks.com/en/delta-live-tables/event-hooks.html
Monitoring - https://docs.databricks.com/en/delta-live-tables/observability.html
GitHub Repository - https://github.com/dfinchdb/ub_dlt_demo.git
"""

import dlt

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


def customerpiidata(spark) -> DataFrame:
    spark = SparkSession.builder.getOrCreate()
    df = (
        spark.readStream.format("cloudFiles")
        .options(
            **{
                "cloudFiles.format": "csv",
                "header": "true",
                "delimiter": "||",
                "rescuedDataColumn": "_rescued_data",
                "cloudFiles.validateOptions": "true",
                "cloudFiles.useNotifications": "false",
                "cloudFiles.inferColumnTypes": "true",
                "cloudFiles.backfillInterval": "1 day",
                "cloudFiles.schemaEvolutionMode": "rescue",
                "cloudFiles.allowOverwrites": "false",
            }
        )
        .load(
            "abfss://databricks-poc@oneenvadls.dfs.core.windows.net/umpqua_poc/landing_zone/customerpiidata"
        )
    )
    return df


def customerpiidata_clean() -> DataFrame:
    df = dlt.read_stream("customerpiidata")
    return df


def corporate_customer_data():
    df = dlt.read_stream("customerpiidata_clean").filter(F.col("is_company") == 1)
    return df


def consumer_customer_data():
    df = dlt.read_stream("customerpiidata_clean").filter(F.col("is_company") == 0)
    return df


if __name__ == "__main__":
    pass
