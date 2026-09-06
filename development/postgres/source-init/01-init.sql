-- Seed data for the source database that Debezium will capture changes from.
CREATE TABLE IF NOT EXISTS customers (
    id          SERIAL PRIMARY KEY,
    first_name  VARCHAR(255) NOT NULL,
    last_name   VARCHAR(255) NOT NULL,
    email       VARCHAR(255) NOT NULL,
    updated_at  TIMESTAMP NOT NULL DEFAULT now()
);

INSERT INTO customers (first_name, last_name, email) VALUES
    ('Ada', 'Lovelace', 'ada@example.com'),
    ('Alan', 'Turing', 'alan@example.com'),
    ('Grace', 'Hopper', 'grace@example.com');

-- Second captured table: simulate-changes.sh drives customers AND orders.
CREATE TABLE IF NOT EXISTS orders (
    id          SERIAL PRIMARY KEY,
    product     VARCHAR(255) NOT NULL,
    quantity    INT NOT NULL DEFAULT 1,
    amount      NUMERIC(10,2) NOT NULL,
    status      VARCHAR(32) NOT NULL DEFAULT 'new',
    updated_at  TIMESTAMP NOT NULL DEFAULT now()
);

INSERT INTO orders (product, quantity, amount, status) VALUES
    ('book', 2, 25.50, 'shipped'),
    ('mug', 1, 9.99, 'new');

-- Debezium's Postgres connector needs a PUBLICATION covering the tables it captures.
CREATE PUBLICATION dbz_publication FOR TABLE customers, orders;
