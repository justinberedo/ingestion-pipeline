-- Table 1: users
CREATE TABLE users (
    user_id BIGINT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    date_joined DATE NOT NULL,
    total_spent FLOAT
);

-- Table 2: transactions
CREATE TABLE transactions (
    trans_id BIGINT PRIMARY KEY,
    user_id BIGINT REFERENCES users(user_id),
    product VARCHAR(255) NOT NULL,
    amount FLOAT NOT NULL,
    trans_date DATE NOT NULL
);