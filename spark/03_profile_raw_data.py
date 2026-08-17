from pyspark.sql import SparkSession
from pyspark.sql.functions import(
    col,count,when,isnan,countDistinct)

from pathlib import Path
project_path = Path(__file__).resolve().parents[1]
raw_path = project_path/"data"/"raw"

spark = (
    SparkSession.builder
    .appName("transactionAnalytics-Profiling")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
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


def null_profile(df,name):
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


###################################################
#### DUPLICATE profiling
###################################################

print("\n========== DUPLICATE CHECK ==========")

print(
    "Duplicate Branch ID's: ",
    branches_df.count() - branches_df.select("branch_id").distinct().count()  
)

print(
    "Duplicate customer ID's:",
    customers_df.count() - customers_df.select("customer_id").distinct().count()
)

print(
    "Duplicate Account ID's:" ,
    accounts_df.count() - accounts_df.select("account_id").distinct().count()
)

print(
    "Duplicate Loan ID's:",
    loans_df.count() - loans_df.select("loan_id").distinct().count()
)

print(
    "Duplicate transaction ID's:",
    transactions_df.count() - transactions_df.select("transaction_id").distinct().count()
)
##################################
## Profile categorical columns
##################################

print("\n========== BRANCH STATES ==========")
### GROUP WISE FETCH THE COUNT OF BRANCHES AND SHOW THEM IN ALPHABATICAL ORDER OF STATE
branches_df.groupBy("state").count().orderBy("state").show()

print("\n========== BRANCH CITIES ==========")

branches_df.groupBy("cities").count().orderBy("cities").show()

print("\n========== CUSTOMER GENDER ==========")
customers_df.groupBy("gender").count().show()

print("\n========== CUSTOMER AGE ==========")
customers_df.groupBy("age").summary().show()

print("\n========== ACCOUNT TYPES ==========")
accounts_df.groupBy("account_type").count().show()

print("\n========== ACCOUNT STATUS ==========")
accounts_df.groupBy("status").count().show()

print("\n========== ACCOUNT BALANCE PROFILE ==========")
accounts_df.select("opening_balance").summary().show()

print("\n========== LOAN TYPES ==========")
loans_df.groupBy("loan_type").count().show()

print("\n========== LOAN STATUS ==========")
loans_df.groupBy("status").count().show()

print("\n========== LOAN NUMERIC PROFILE ==========")
loans_df.select(
    "principal_amount",
    "interest_rate",
    "tenure_months"
).summary().show()

print("\n========== TRANSACTION TYPES ==========")
transactions_df.groupBy("transaction_type").count().show()

print("\n========== TRANSACTION CHANNELS ==========")
transactions_df.groupBy("channels").count().show()

print("\n========== TRANSACTION STATUS ==========")
transactions_df.groupBy("status").count().show()

print("\n========== TRANSACTION AMOUNT PROFILE ==========")
transactions_df.select("amount").summary().show()

print("\n========== TRANSACTION DATE PROFILE ==========")
transactions_df.groupBy("transaction_date").count().show()

print("\n========== TRANSACTION WITH INVALID ACCOUNT ID'S ==========")

inv_trans_accounts = (
    transactions_df
    .join(
        accounts_df.select("account_id"),
        on = "account_id",
        how = "left_anti"
    )
)
print("Transactions with Invald Account ID:", 
      inv_trans_accounts.count()
    )

#   Give me records from the left DataFrame that do not have a matching record in the right DataFrame. 
##  here left_df is to check and right_df(inside join ) is the master
  
print("\n========== ACCOUNT WISE INVALID CUSTOMER ID's  ==========")
inv_cust_acc = (
    accounts_df
    .join
    (
        customers_df.select("customer_id"),
        on="customer_id",
        how= "left_anti"
    )
)

print("Accounts with Invalid Customer ID's:", inv_cust_acc)

print("\n========== ACCOUNT WISE INVALID BRANCH ID's  ==========")

inv_acc_br = (
    accounts_df
    .join(
        branches_df.select("branch_id"),
        on="branch_id",
        how="left_anti"
    )
)

print("Accounts with Invalid Branch ID's:", inv_acc_br)

print("\n========== CUSTOMER WISE INVALID BRANCH ID's  ==========")

inv_cust_br = (
    customers_df
    .join(
        branches_df.select("branc_id"),
        on="branch_id",
        how="left_anti"
    )
)

print("Customers with Invalid Branch ID's:", inv_cust_br)