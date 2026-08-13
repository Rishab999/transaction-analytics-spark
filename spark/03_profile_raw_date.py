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
#################################
## LOAD ALL DATASETS
#################################
branches_df = (
    spark.read
    .option("header",True)
    .option("inferSchema", True)
    .csv(str(raw_path/"branches.csv"))
)

customers_df = (
    spark.read
    .option("header",True)
    .option("inferSchema",True)
    .csv(str(raw_path/"customers.csv"))
)

accounts_df = (
    spark.read
    .option("header",True)
    .option("inferSchema",True)
    .csv(str(raw_path/"accounts.csv"))
)

loans_df = (
    spark.read
    .option("header",True)
    .option("inferSchema",True)
    .csv(str(raw_path/"loans.csv"))
)

transactions_df = (
    spark.read
    .option("header",True)
    .option("inferSchema",True)
    .csv(raw_path/"transactions.csv")
)
###################################################
#### First profiling check: row counts
###################################################
print("\n========== ROW COUNTS ==========")
print("Branches:", branches_df.count())
print("Customers:", customers_df.count())
print("Accounts:", accounts_df.count())
print("Loans:", loans_df.count())
print("Transactions:", transactions_df.count())

###################################################
#### Schema profiling
###################################################
print("\n========== SCHEMAS ==========")

print("\n--- CUSTOMERS ---")
customers_df.printSchema()

print("\n--- ACCOUNTS ---")
accounts_df.printSchema()

print("\n--- LOANS ---")
loans_df.printSchema()

print("\n--- TRANSACTIONS ---")
transactions_df.printSchema()

###################################################
#### NULL profiling
###################################################

## 1. df.select gives columns 
## 2. for c in df.columns means we are asking c to iterate for every columns that will be returned
## 3. count(....)for c in df.columns does the counting using c
## 4. col(c) => inside count we cast c as spark  column object using col(c) -> we ask it to get inside the column dataframe, and oterate over its rows 
## 4. INSIDE WHEN() -> If the value is NULL or NaN, return the value as alias.(c) = column name itself, else return 1 if value exists somethung but not NULL. later alias.(c) will be counted for all c in df.columns
## 5. alias(c) -> ask it to return value same as ailas c. If we odnt do this then it will give a count(case when .....) result  

##OUTPUT :
# +---------+-----------+-----+----+
# |branch_id|branch_name|state|city|
# +---------+-----------+-----+----+
# |0        |2          |1    |3   |
# +---------+-----------+-----+----+
## .alias(c) -> returns branch_id , branch_name , state , city columns
## 0 , 2 , 1, 3 is returned after counting NULL.


def null_profil(df,name):
    print(f"\n========== NULL PROFILE {name}=========")
    null_counts = df.select([
        count
            (when
                (
                    col(c).isNUll() | isnan(col(c)),1
                ).alias(c)
            ) for c in df.columns
    ])

    null_counts.show()

    null_profile(branches_df  ,"BRANCEHS")
    null_profile(customers_df,"CUSTOMERS")
    null_profile(accounts_df,"ACCOUNTS")
    null_profile(loans_df,"LOANS")
    null_profile(transactions_df,"TRANSACTIONS")