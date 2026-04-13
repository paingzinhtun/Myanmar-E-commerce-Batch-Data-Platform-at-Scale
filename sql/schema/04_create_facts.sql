CREATE TABLE IF NOT EXISTS analytics.fact_orders (
    order_id VARCHAR(32) PRIMARY KEY,
    customer_id VARCHAR(20),
    region_key BIGINT,
    order_date DATE,
    order_timestamp TIMESTAMP,
    order_status VARCHAR(30),
    payment_method VARCHAR(50),
    shipping_fee_mmk NUMERIC(18, 2),
    subtotal_amount_mmk NUMERIC(18, 2),
    order_total_mmk NUMERIC(18, 2),
    item_count INTEGER,
    total_quantity INTEGER,
    batch_date DATE
);

CREATE TABLE IF NOT EXISTS analytics.fact_order_items (
    order_item_id VARCHAR(32) PRIMARY KEY,
    order_id VARCHAR(32),
    product_id VARCHAR(20),
    seller_id VARCHAR(20),
    category VARCHAR(100),
    brand VARCHAR(100),
    line_number INTEGER,
    quantity INTEGER,
    unit_price_mmk NUMERIC(18, 2),
    gross_line_amount_mmk NUMERIC(18, 2),
    discount_amount_mmk NUMERIC(18, 2),
    net_line_amount_mmk NUMERIC(18, 2),
    batch_date DATE
);

CREATE TABLE IF NOT EXISTS analytics.fact_payments (
    payment_id VARCHAR(32) PRIMARY KEY,
    order_id VARCHAR(32),
    customer_id VARCHAR(20),
    payment_method VARCHAR(50),
    payment_status VARCHAR(30),
    payment_amount_mmk NUMERIC(18, 2),
    payment_date DATE,
    paid_at TIMESTAMP,
    batch_date DATE
);

CREATE TABLE IF NOT EXISTS analytics.fact_inventory_snapshot (
    inventory_snapshot_id VARCHAR(32) PRIMARY KEY,
    product_id VARCHAR(20),
    seller_id VARCHAR(20),
    snapshot_date DATE,
    available_quantity INTEGER,
    reserved_quantity INTEGER,
    inventory_status VARCHAR(30),
    reorder_threshold INTEGER,
    batch_date DATE
);

CREATE TABLE IF NOT EXISTS analytics.fact_delivery_performance (
    delivery_id VARCHAR(32) PRIMARY KEY,
    order_id VARCHAR(32),
    delivery_provider VARCHAR(100),
    region VARCHAR(100),
    city VARCHAR(100),
    promised_delivery_date DATE,
    actual_delivery_date DATE,
    delivery_status VARCHAR(30),
    delivery_fee_mmk NUMERIC(18, 2),
    delivery_delay_days INTEGER,
    batch_date DATE
);
