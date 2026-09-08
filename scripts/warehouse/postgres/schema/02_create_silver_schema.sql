CREATE TABLE IF NOT EXISTS silver.sales_order_header_clean (
    sales_order_id INTEGER PRIMARY KEY,
    order_date DATE,
    due_date DATE,
    ship_date DATE,
    customer_id INTEGER,
    salesperson_id INTEGER,
    territory_id INTEGER,
    subtotal NUMERIC(19,4),
    tax_amt NUMERIC(19,4),
    freight NUMERIC(19,4),
    total_due NUMERIC(19,4),
    is_online_order BOOLEAN,
    status_code SMALLINT,
    _source_system VARCHAR(100),
    _load_date TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS silver.sales_order_detail_clean (
    sales_order_id INTEGER,
    sales_order_detail_id INTEGER,
    product_id INTEGER,
    order_qty INTEGER,
    unit_price NUMERIC(19,4),
    unit_price_discount NUMERIC(19,4),
    line_total NUMERIC(19,4),
    _source_system VARCHAR(100),
    _load_date TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS silver.customer_clean (
    customer_id INTEGER PRIMARY KEY,
    person_id INTEGER,
    store_id INTEGER,
    territory_id INTEGER,
    account_number VARCHAR(50),
    customer_name VARCHAR(255),
    customer_type VARCHAR(20),
    customer_name_source VARCHAR(20),
    _source_system VARCHAR(100),
    _load_date TIMESTAMPTZ DEFAULT NOW()
);

ALTER TABLE silver.customer_clean
    ADD COLUMN IF NOT EXISTS customer_type VARCHAR(20);
ALTER TABLE silver.customer_clean
    ADD COLUMN IF NOT EXISTS customer_name_source VARCHAR(20);

CREATE TABLE IF NOT EXISTS silver.product_clean (
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR(255),
    product_number VARCHAR(50),
    product_line VARCHAR(2),
    class VARCHAR(2),
    style VARCHAR(2),
    list_price NUMERIC(19,4),
    standard_cost NUMERIC(19,4),
    is_discontinued BOOLEAN,
    _source_system VARCHAR(100),
    _load_date TIMESTAMPTZ DEFAULT NOW()
);
