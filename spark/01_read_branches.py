from pyspark.sql import SparkSession


# --------------------------------------------------
# Create Spark Session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("TransactionAnalytics")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
# --------------------------------------------------
# Read branches CSV
# --------------------------------------------------

branches_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/raw/branches.csv")
)
# added transformaton
maha_branches = branches_df.filter(
    branches_df.state == 'Maharashtra'
)
print("Transformation Created")

# action
print("Executing Action")
maha_branches.show()

# --------------------------------------------------
# Inspect DataFrame
# --------------------------------------------------

# branches_df.show()

# branches_df.printSchema()

# print("Number of rows:", branches_df.count())


# --------------------------------------------------
# Stop Spark
# --------------------------------------------------

spark.stop()