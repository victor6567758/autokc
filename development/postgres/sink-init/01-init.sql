-- Mirror table the JDBC sink connector will upsert into.
-- auto.create=true on the connector would also create this, but declaring it
-- explicitly makes the schema (and any constraints you add later) obvious.
CREATE TABLE IF NOT EXISTS customers (
    id          INT PRIMARY KEY,
    first_name  VARCHAR(255),
    last_name   VARCHAR(255),
    email       VARCHAR(255),
    updated_at  TIMESTAMP
);

CREATE TABLE IF NOT EXISTS orders (
    id          INT PRIMARY KEY,
    product     VARCHAR(255),
    quantity    INT,
    amount      NUMERIC(10,2),
    status      VARCHAR(32),
    updated_at  TIMESTAMP
);
