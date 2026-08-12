# from pyspark.sql import SparkSession

# spark = (
#     SparkSession.builder
#     .appName("TransactionAnalytics")
#     .getOrCreate()
# )

# spark.sparkContext.setLogLevel("WARN")


# # --------------------------------------------------
# # Raw data paths
# # --------------------------------------------------
# raw_path = "data/raw"

# # --------------------------------------------------
# # Read all 5 datasets
# # --------------------------------------------------
# branches_df = (
#     spark.read
#     .option("header",True)
#     .option("inferSchema",True)
#     .csv(f"{raw_path}/branches.csv")
# )

# customers_df = (
#     spark.read
#     .option("header",True)
#     .option("inferSchema",True)
#     .csv(f"{raw_path}/customers.csv")
# )

# accounts_df = (
#     spark.read
#     .option("header",True)
#     .option("inferSchema",True)
#     .csv(f"{raw_path}/accounts.csv")
# )

# loans_df = (
#     spark.read
#     .option("header",True)
#     .option("inferSchema",True)
#     .csv(f"{raw_path}/loans.csv")
# )

# transactions_df = (
#     spark.read
#     .option("header",True)
#     .option("inferSchema",True)
#     .csv(f"{raw_path}/transactions.csv")
# )

# # --------------------------------------------------
# # Display row counts
# # --------------------------------------------------
# print("Branches:" , branches_df.count())
# print("Customers:", customers_df.count())
# print("Accounts:", accounts_df.count())
# print("Loans:", loans_df.count())
# print("Transactions:",transactions_df.count())



# # --------------------------------------------------
# # Display schemas
# # --------------------------------------------------

# print("\n========== BRANCHES SCHEMA ==========")
# branches_df.printSchema()

# print("\n========== CUSTOMERS SCHEMA ==========")
# customers_df.printSchema

# print("\n========== ACCOUNTS SCHEMA ==========")
# accounts_df.printSchema()

# print("\n========== LOANS SCHEMA ==========")
# loans_df.printSchema()

# # --------------------------------------------------
# # Show sample records
# # --------------------------------------------------

# print("\n========== BRANCHES ==========")
# branches_df.show(5,truncate=False)

# print("\n========== CUSTOMERS ==========")
# customers_df.show(5,truncate= False)

# print("\n========== ACCOUNTS ==========")
# accounts_df.show(5,truncate = False)

# print("\n========== LOANS ==========")
# loans_df.show(5, truncate=False)

# print("\n========== TRANSACTIONS ==========")
# transactions_df.show(5, truncate = False)

# # --------------------------------------------------
# # Stop Spark
# # --------------------------------------------------
# spark.stop()

from pyspark.sql import SparkSession
spark = (
    SparkSession.builder 
    .appName("Transactionanalytics")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

raw_path = "data/raw"

branches_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{raw_path}/branches.csv")
)

branches_df.show(truncate = False)

spark.stop