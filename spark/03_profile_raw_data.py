from pyspark.sql import SparkSession
from pyspark.sql.functions import(
    col,count,when,isnan,countDistinct , min , max , sum)

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
    .csv(str(raw_path/"transactions.csv"))
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
                    col(c).isNull() | isnan(col(c)),1
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

branches_df.groupBy("city").count().orderBy("city").show()

print("\n========== CUSTOMER GENDER ==========")
customers_df.groupBy("gender").count().show()

print("\n========== CUSTOMER AGE ==========")
customers_df.groupBy("age").count().orderBy("age").show()

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
transactions_df.groupBy("channel").count().show()

print("\n========== TRANSACTION STATUS ==========")
transactions_df.groupBy("status").count().show()

print("\n========== TRANSACTION AMOUNT PROFILE ==========")
transactions_df.select("amount").summary().show()

print("\n========== TRANSACTION DATE PROFILE ==========")
transactions_df.select(
    min("transaction_date").alias("min_date"),
    max("transaction_date").alias("max_date")
).show()


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

print("Accounts with Invalid Customer ID's:", inv_cust_acc.show())
inv_cust_acc.show(20,truncate = False)

print("\n========== ACCOUNT WISE INVALID BRANCH ID's  ==========")

inv_acc_br = (
    accounts_df
    .join(
        branches_df.select("branch_id"),
        on="branch_id",
        how="left_anti"
    )
)

print("Accounts with Invalid Branch ID's:", inv_acc_br.show())
inv_acc_br.show(20 , truncate = False)

print("\n========== CUSTOMER WISE INVALID BRANCH ID's  ==========")

inv_cust_br = (
    customers_df
    .join(
        branches_df.select("branch_id"),
        on="branch_id",
        how="left_anti"
    )
)

print("Customers with Invalid Branch ID's:", inv_cust_br.count())
inv_cust_br.show(20, truncate = False)

# ============================================================
# NULL VALUE PROFILING
# ============================================================

print("\n===== NULL VALUE PROFILE =====")

def null_profile(df , df_name):
    print(f"\n--- {df_name} ---")
    #1 df.columns -> ["branch_id", "branch_name", "state", "city"]
    
    #2 for c in df.columns 
    #   c = "branch_id"
    #   c = "branch_name"
    #   c = "state"
    #   c = "city" 

    # 3 col(c) -> c = "state" = col(c)
    #   -> Represent the state column as a Spark Column expression. 
    
    # 4 col(c).isNull() -> Is the value in state NULL?
        #-> state       isNull()

        #   Maharashtra False
        #   NULL        True
        #   Delhi       False
        #   NULL        True
        #   Gujarat     False

    #5 col(c).isNull().cast("int") -> True  → 1 False → 0
    # THEREFORE COUNT OF 1 = ALL NULL VALUES

    null_counts = df.select(
        *[
            sum(col(c).isnull.cast("int").alias(c)) for c in df.columns
        ]
    )
    null_counts.show()
null_profile(branches_df, "BRANCHES")
null_profile(customers_df, "CUSTOMERS")
null_profile(accounts_df, "ACCOUNTS")
null_profile(loans_df, "LOANS")
null_profile(transactions_df, "TRANSACTIONS")

# ============================================================
# CUSTOMER AGE VALIDATION
# ============================================================

print("\n========== INVALID CUSTOMER AGES ==========")

inv_age = (
    customers_df.filter(
        (col("age")<18) |
        (col("age")>100)
    )
)
print("Invalid Customer Ages:", inv_age.count())
inv_age.show()

# ============================================================
# ACCOUNT BALANCE VALIDATION
# ============================================================

print("\n========== INVALID ACCOUNT BALANCES ==========")

inv_acc_bal = (
    accounts_df.filter(col("opening_balance") < 0)
)
print( "Accounts with negative opening balance:",inv_acc_bal.count())
inv_acc_bal.show()

# ============================================================
# LOAN AMOUNT VALIDATION
# ============================================================
print("\n========== INVALID LOAN AMOUNTS ==========")

inv_loan_amt = (
    loans_df.filtert(col("principal_amount") <= 0)
)
print("Loans with Invalid principal Amount:", inv_loan_amt.count())
inv_loan_amt.show()


# ============================================================
# LOAN INTEREST RATE VALIDATION
# ============================================================

print("\n========== INVALID INTEREST RATES ==========")

inv_int_rate =  (
    loans_df.filter(
        (col("interest_rate") <= 0) |
        (col("interest_rate") > 100)
    )
)
print("Loans with INvalid Interest Rate:", inv_int_rate.count())
inv_int_rate.show()

# ============================================================
# LOAN TENURE VALIDATION
# ============================================================

print("\n========== INVALID LOAN TENURE ==========")

inv_loan_tenure = (
    loans_df.filter(
        (COL("tenure_months") <= 0)
    )
)
print("Loans with Invalid tenure:", inv_loan_tenure.count())

inv_loan_tenure.show()

# ============================================================
# TRANSACTION AMOUNT VALIDATION
# ============================================================
print("\n========== INVALID TRANSACTION AMOUNTS ==========")

inv_trans_amt = (
    transactions_df.filter(
        col("amount"<= 0)
    )
)

print("transactions with Invalid Amount:", inv_trans_amt.count())
inv_trans_amt.show()

# ============================================================
# TRANSACTION DATE VALIDATION
# ============================================================

print("\n========== INVALID TRANSACTION DATES ==========")

inv_trans_date = (
    transactions_df.filter(
        col("ransaction_date").isnull()
    )
)

print("transactions with Invalid Transaction Date", inv_trans_date.count())
inv_trans_date.show()


# ============================================================
# CITY-STATE VALIDATION
# ============================================================

print("\n========== INVALID CITY-STATE MAPPINGS ==========")

valid_city_state = {
    "Nagpur":"Maharashtra",
    "Mumbai":"Maharashtra",
    "Pune": "Maharashtra",
    "Hyderabad": "Telangana",
    "Chennai" : "Tamil Nadu",
    "Bengaluru" : "Karnataka",
    "Kolkata" : "West Bengal",\
    "Delhi" : "Delhi"
}

from pyspark.sql.functions import create_map , lit

mapping_expr = create_map(
    *[
        item
        # converting valid city item distiopnary to key value pairs pair[0] = city , pair[1] = state
        for pair in valid_city_state.items()
        # converting above created python dictionaries to spark literals
        for item in (lit(pair[0]), lit(pair[1]))
    ]
)

inv_city_state = (
    branches_df
    .withColumn(
        "expected_state" , 
        mapping_expr[col("city")]
    ).filter(
        col("state") != col("expected_sate")
    )
)

print("Branches with in valid city-state mapping:". inv_city_state.count())

inv_city_state.select(
    "branch_id" , "branch_name" , "city" ,  "state" , "expected_state"
).show(20)