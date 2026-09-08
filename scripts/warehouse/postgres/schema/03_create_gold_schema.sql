CREATE TABLE IF NOT EXISTS gold.dim_customer (
    customer_id INTEGER PRIMARY KEY,
    customer_name VARCHAR(255),
    customer_type VARCHAR(20),
    customer_name_source VARCHAR(20),
    person_id INTEGER,
    store_id INTEGER,
    territory_id INTEGER,
    account_number VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

ALTER TABLE gold.dim_customer
    ADD COLUMN IF NOT EXISTS customer_type VARCHAR(20);
ALTER TABLE gold.dim_customer
    ADD COLUMN IF NOT EXISTS customer_name_source VARCHAR(20);

CREATE TABLE IF NOT EXISTS gold.dim_product (
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR(255),
    product_number VARCHAR(50),
    product_line VARCHAR(2),
    product_class VARCHAR(2),
    product_style VARCHAR(2),
    list_price NUMERIC(19,4),
    standard_cost NUMERIC(19,4),
    is_discontinued BOOLEAN,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_id INTEGER PRIMARY KEY,
    full_date DATE,
    year_number SMALLINT,
    quarter_number SMALLINT,
    month_number SMALLINT,
    month_name VARCHAR(20),
    day_number SMALLINT,
    is_weekend BOOLEAN
);

CREATE TABLE IF NOT EXISTS gold.fact_sales (
    sales_order_id INTEGER,
    sales_order_detail_id INTEGER,
    order_date_id INTEGER,
    customer_id INTEGER,
    product_id INTEGER,
    territory_id INTEGER,
    salesperson_id INTEGER,
    order_qty INTEGER,
    unit_price NUMERIC(19,4),
    discount_amount NUMERIC(19,4),
    line_total NUMERIC(19,4),
    net_sales NUMERIC(19,4),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS gold.fact_customer_orders (
    customer_id INTEGER,
    sales_order_id INTEGER,
    order_date_id INTEGER,
    product_id INTEGER,
    order_qty INTEGER,
    sales_amount NUMERIC(19,4),
    is_returned BOOLEAN,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS gold.fact_inventory (
    product_id INTEGER,
    location_id INTEGER,
    date_id INTEGER,
    quantity_on_hand INTEGER,
    transaction_qty INTEGER,
    unit_cost NUMERIC(19,4),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS gold.fact_purchasing (
    purchase_order_id INTEGER,
    purchase_order_detail_id INTEGER,
    vendor_id INTEGER,
    product_id INTEGER,
    order_date_id INTEGER,
    quantity INTEGER,
    unit_cost NUMERIC(19,4),
    total_cost NUMERIC(19,4),
    received_qty INTEGER,
    lead_time_days INTEGER,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
