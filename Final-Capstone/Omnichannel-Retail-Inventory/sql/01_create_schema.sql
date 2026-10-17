-- ============================================================
-- OMNICHANNEL RETAIL
-- OPERATIONAL DATABASE SCHEMA
-- ============================================================


-- ------------------------------------------------------------
-- DATABASE
-- ------------------------------------------------------------

CREATE DATABASE IF NOT EXISTS omnichannel_retail
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_0900_ai_ci;

USE omnichannel_retail;


-- ============================================================
-- STORES
-- ============================================================

CREATE TABLE IF NOT EXISTS stores (

    store_id VARCHAR(20) NOT NULL,

    store_name VARCHAR(150) NOT NULL,

    city VARCHAR(100) NOT NULL,

    state VARCHAR(100) NOT NULL,

    region VARCHAR(50) NOT NULL,

    store_type VARCHAR(30) NOT NULL,

    channel VARCHAR(30) NOT NULL,

    opening_date DATE NOT NULL,

    active BOOLEAN NOT NULL,

    PRIMARY KEY (store_id)

) ENGINE=InnoDB;


-- ============================================================
-- PRODUCTS
-- ============================================================

CREATE TABLE IF NOT EXISTS products (

    sku VARCHAR(20) NOT NULL,

    product_name VARCHAR(200) NOT NULL,

    category VARCHAR(100) NOT NULL,

    subcategory VARCHAR(100) NOT NULL,

    brand VARCHAR(100) NOT NULL,

    selling_price DECIMAL(12,2) NOT NULL,

    base_cost DECIMAL(12,2) NOT NULL,

    shelf_life_days INT NOT NULL,

    demand_class VARCHAR(20) NOT NULL,

    active BOOLEAN NOT NULL,

    PRIMARY KEY (sku),

    CHECK (selling_price > 0),

    CHECK (base_cost > 0),

    CHECK (base_cost < selling_price),

    CHECK (shelf_life_days > 0)

) ENGINE=InnoDB;


-- ============================================================
-- SUPPLIERS
-- ============================================================

CREATE TABLE IF NOT EXISTS suppliers (

    supplier_id VARCHAR(20) NOT NULL,

    supplier_name VARCHAR(200) NOT NULL,

    city VARCHAR(100) NOT NULL,

    state VARCHAR(100) NOT NULL,

    region VARCHAR(50) NOT NULL,

    default_lead_time_days INT NOT NULL,

    reliability_score DECIMAL(5,4) NOT NULL,

    minimum_order_value DECIMAL(12,2) NOT NULL,

    active BOOLEAN NOT NULL,

    PRIMARY KEY (supplier_id),

    CHECK (default_lead_time_days > 0),

    CHECK (
        reliability_score >= 0
        AND reliability_score <= 1
    ),

    CHECK (minimum_order_value >= 0)

) ENGINE=InnoDB;


-- ============================================================
-- SUPPLIER PRODUCTS
-- ============================================================

CREATE TABLE IF NOT EXISTS supplier_products (

    supplier_id VARCHAR(20) NOT NULL,

    sku VARCHAR(20) NOT NULL,

    supplier_cost DECIMAL(12,2) NOT NULL,

    minimum_order_qty INT NOT NULL,

    lead_time_days INT NOT NULL,

    preferred_supplier BOOLEAN NOT NULL,

    PRIMARY KEY (
        supplier_id,
        sku
    ),

    CONSTRAINT fk_supplier_products_supplier
        FOREIGN KEY (supplier_id)
        REFERENCES suppliers(supplier_id),

    CONSTRAINT fk_supplier_products_product
        FOREIGN KEY (sku)
        REFERENCES products(sku),

    CHECK (supplier_cost > 0),

    CHECK (minimum_order_qty > 0),

    CHECK (lead_time_days > 0)

) ENGINE=InnoDB;


-- ============================================================
-- INVENTORY
-- ============================================================

CREATE TABLE IF NOT EXISTS inventory (

    store_id VARCHAR(20) NOT NULL,

    sku VARCHAR(20) NOT NULL,

    opening_stock INT NOT NULL,

    safety_stock INT NOT NULL,

    reorder_level INT NOT NULL,

    max_stock_level INT NOT NULL,

    last_updated DATETIME NOT NULL,

    PRIMARY KEY (
        store_id,
        sku
    ),

    CONSTRAINT fk_inventory_store
        FOREIGN KEY (store_id)
        REFERENCES stores(store_id),

    CONSTRAINT fk_inventory_product
        FOREIGN KEY (sku)
        REFERENCES products(sku),

    CHECK (opening_stock >= 0),

    CHECK (safety_stock >= 0),

    CHECK (reorder_level >= safety_stock),

    CHECK (max_stock_level >= reorder_level)

) ENGINE=InnoDB;


-- ============================================================
-- USEFUL INDEXES
-- ============================================================

CREATE INDEX idx_products_category
    ON products(category);

CREATE INDEX idx_products_demand_class
    ON products(demand_class);

CREATE INDEX idx_stores_region
    ON stores(region);

CREATE INDEX idx_inventory_sku
    ON inventory(sku);

CREATE INDEX idx_supplier_products_sku
    ON supplier_products(sku);
