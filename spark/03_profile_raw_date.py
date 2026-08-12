from pyspark.sql import SparkSession
from pyspark.sql.functions import(
    col,count,when,isnan,countDistinct)

from pathlib import Path
project_path = Path(__file__).resolve.parents[1]
raw_path = project_path/"data"/"raw"

spark = (
    SparkSession.builder
    .appName("transactionAnalytics-Profiling")
    .getOrCreate()
)

## LOAD ALL DATASETS

branches_df = (
    spark.read
    .option("header",True)
    .option("inferSchema", True)
    .csv(str(raw_path/"branches.csv"))
)

