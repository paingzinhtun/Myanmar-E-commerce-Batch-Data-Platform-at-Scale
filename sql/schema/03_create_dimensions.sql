CREATE TABLE IF NOT EXISTS analytics.dim_customer (
    customer_id VARCHAR(20) PRIMARY KEY,
    customer_name VARCHAR(255),
    phone_number VARCHAR(20),
    email VARCHAR(255),
    city VARCHAR(100),
    region VARCHAR(100),
    township VARCHAR(100),
    segment VARCHAR(30),
    created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS analytics.dim_seller (
    seller_id VARCHAR(20) PRIMARY KEY,
    seller_name VARCHAR(255),
    city VARCHAR(100),
    region VARCHAR(100),
    seller_type VARCHAR(50),
    rating NUMERIC(5, 2),
    created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS analytics.dim_product (
    product_id VARCHAR(20) PRIMARY KEY,
    seller_id VARCHAR(20),
    product_name VARCHAR(255),
    category VARCHAR(100),
    brand VARCHAR(100),
    unit_price_mmk NUMERIC(18, 2),
    is_active BOOLEAN,
    created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS analytics.dim_region (
    region_key BIGINT PRIMARY KEY,
    region VARCHAR(100),
    city VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS analytics.dim_date (
    date_key INTEGER PRIMARY KEY,
    calendar_date DATE,
    year INTEGER,
    month INTEGER,
    day INTEGER,
    month_name VARCHAR(20),
    week_of_year INTEGER
);
