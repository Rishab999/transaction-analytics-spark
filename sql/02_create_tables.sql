CREATE TABLE IF NOT EXISTS source.customers(
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(100),
    city VARCHAR(50),
    state VARCHAR(50),
    customer_type VARCHAR(30),
    created_at TIMESTAMP
);


CREATE TABLE IF NOT EXISTS source.accounts(
    account_id INT PRIMARY KEY,
    customer_id INT NOT NULL,
    account_type VARCHAR(30),
    branch_id INT,
    opening_date DATE,
    balance DECIMAL(15,2),

    CONSTRAINT fk_accounts_customer
        FOREIGN KEY (customer_id)
        REFERENCES source.customers(customer_id)
);

CREATE TABLE IF NOT EXISTS source.transactions (
    transaction_id BIGINT PRIMARY KEY,
    account_id INT NOT NULL,
    transaction_date TIMESTAMP,
    transaction_type VARCHAR(20),
    amount DECIMAL(15,2),
    channel VARCHAR(30),
    status VARCHAR(20),

    CONSTRAINT fk_transactions_account
    FOREIGN KEY (account_id)
    REFERENCES source.accounts(account_id)
);


