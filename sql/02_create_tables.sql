CREATE TABLE IF NOT EXISTS source.branches (
    branch_id VARCHAR(20) PRIMARY KEY,
    branch_name VARCHAR(100),
    city VARCHAR(50),
    state VARCHAR(50)
);


CREATE TABLE IF NOT EXISTS source.customers (
    customer_id VARCHAR(20) PRIMARY KEY,
    customer_name VARCHAR(100),
    gender VARCHAR(10),
    age INT,
    city VARCHAR(50),
    branch_id VARCHAR(20),
    customer_since DATE
);


CREATE TABLE IF NOT EXISTS source.accounts (
    account_id VARCHAR(20) PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL,
    account_type VARCHAR(30),
    branch_id VARCHAR(20),
    opening_date DATE,
    status VARCHAR(20),
    opening_balance DECIMAL(15,2),

    CONSTRAINT fk_accounts_customer
        FOREIGN KEY (customer_id)
        REFERENCES source.customers(customer_id)
);


CREATE TABLE IF NOT EXISTS source.loans (
    loan_id VARCHAR(30) PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL,
    loan_type VARCHAR(30),
    principal_amount DECIMAL(15,2),
    interest_rate DECIMAL(5,2),
    tenure_months INT,
    status VARCHAR(20),

    CONSTRAINT fk_loans_customer
        FOREIGN KEY (customer_id)
        REFERENCES source.customers(customer_id)
);


CREATE TABLE IF NOT EXISTS source.transactions (
    transaction_id VARCHAR(30) PRIMARY KEY,
    account_id VARCHAR(20) NOT NULL,
    transaction_date DATE,
    transaction_type VARCHAR(20),
    amount DECIMAL(15,2),
    channel VARCHAR(30),
    status VARCHAR(20),

    CONSTRAINT fk_transactions_account
        FOREIGN KEY (account_id)
        REFERENCES source.accounts(account_id)
);