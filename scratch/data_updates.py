from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

user_id = spark.sql('select current_user() as user').collect()[0]['user']
storage_account = "ubsadatabrickspocnpl2"
storage_container = "umpquapocdev"
lz_base_path = "umpqua_poc/landing_zone"

pii_source = f"file:/Workspace/Repos/{user_id}/ub_dlt_demo/fixtures/sample_data/updates/CustomerPIIData_update.csv"
pii_landing_destination = f"abfss://{storage_container}@{storage_account}.dfs.core.windows.net/{lz_base_path}/customerpiidata"

pii_df = (
    spark.read.format("csv")
    .option("delimiter", ",")
    .option("header", "True")
    .load(pii_source)
)

pii_df.write.csv(
    path=pii_landing_destination,
    sep="||",
    header=True,
    mode="append",
)
