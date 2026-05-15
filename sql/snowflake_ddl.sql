-- ============================================================================
-- Medallion Lakehouse - Snowflake Gold-layer DDL
--
-- Create the database/schema and the analytics-ready Gold tables. Run this
-- once per environment before invoking ``src.snowflake.load_to_snowflake``.
--
-- Conventions
--   * All identifiers UPPERCASE (Snowflake default).
--   * Clustering keys chosen to match the most common BI query predicates.
--   * Surrogate keys use VARCHAR (SHA-256 hex) to stay portable.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- Database / schema setup
-- ---------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS LAKEHOUSE
    COMMENT = 'Gold-layer analytics for the Medallion Lakehouse';

CREATE SCHEMA IF NOT EXISTS LAKEHOUSE.GOLD
    COMMENT = 'Business-ready dimensions, facts, and aggregates';

USE SCHEMA LAKEHOUSE.GOLD;


-- ---------------------------------------------------------------------------
-- DIM_CUSTOMER (Slowly Changing Dimension - Type 2)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS DIM_CUSTOMER (
    CUSTOMER_SK      VARCHAR(64)   NOT NULL,         -- SHA-256 surrogate key
    CUSTOMER_ID      VARCHAR(64)   NOT NULL,         -- natural / business key
    FIRST_NAME       VARCHAR(128),
    LAST_NAME        VARCHAR(128),
    EMAIL            VARCHAR(320),                   -- RFC 5321 max length
    COUNTRY          VARCHAR(64),
    SIGNUP_DATE      DATE,
    EFFECTIVE_FROM   TIMESTAMP_NTZ NOT NULL,
    EFFECTIVE_TO     TIMESTAMP_NTZ,                  -- NULL when row is current
    IS_CURRENT       BOOLEAN       NOT NULL,
    CONSTRAINT PK_DIM_CUSTOMER PRIMARY KEY (CUSTOMER_SK)
)
CLUSTER BY (CUSTOMER_ID, IS_CURRENT)
COMMENT = 'SCD Type 2 customer dimension. IS_CURRENT=TRUE indicates the active row.';


-- ---------------------------------------------------------------------------
-- FACT_ORDERS (transaction grain)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS FACT_ORDERS (
    ORDER_ID          VARCHAR(64)   NOT NULL,
    CUSTOMER_ID       VARCHAR(64),
    CUSTOMER_SK       VARCHAR(64),                   -- FK to DIM_CUSTOMER
    COUNTRY           VARCHAR(64),
    PRODUCT_ID        VARCHAR(64),
    ORDER_DATE        DATE          NOT NULL,
    ORDER_TIMESTAMP   TIMESTAMP_NTZ,
    ORDER_STATUS      VARCHAR(32),
    PAYMENT_METHOD    VARCHAR(32),
    QUANTITY          NUMBER(10, 0),
    UNIT_PRICE        NUMBER(12, 2),
    DISCOUNT_AMOUNT   NUMBER(12, 2),
    GROSS_AMOUNT      NUMBER(14, 2),
    NET_AMOUNT        NUMBER(14, 2),
    _GOLD_TIMESTAMP   TIMESTAMP_NTZ,
    CONSTRAINT PK_FACT_ORDERS PRIMARY KEY (ORDER_ID),
    CONSTRAINT FK_FACT_ORDERS_CUSTOMER FOREIGN KEY (CUSTOMER_SK)
        REFERENCES DIM_CUSTOMER (CUSTOMER_SK)
)
CLUSTER BY (ORDER_DATE, COUNTRY)
COMMENT = 'Order-grain fact table. Joins to DIM_CUSTOMER on CUSTOMER_SK.';


-- ---------------------------------------------------------------------------
-- AGG_DAILY_SALES (pre-aggregated for BI)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS AGG_DAILY_SALES (
    ORDER_DATE         DATE          NOT NULL,
    COUNTRY            VARCHAR(64),
    ORDER_COUNT        NUMBER(18, 0),
    UNIQUE_CUSTOMERS   NUMBER(18, 0),
    TOTAL_UNITS        NUMBER(18, 0),
    GROSS_REVENUE      NUMBER(18, 2),
    NET_REVENUE        NUMBER(18, 2),
    AVG_ORDER_VALUE    NUMBER(18, 2),
    _GOLD_TIMESTAMP    TIMESTAMP_NTZ
)
CLUSTER BY (ORDER_DATE)
COMMENT = 'Daily sales aggregation at (order_date, country) grain.';


-- ---------------------------------------------------------------------------
-- Convenience view: current customer dimension
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW VW_DIM_CUSTOMER_CURRENT AS
SELECT *
FROM DIM_CUSTOMER
WHERE IS_CURRENT = TRUE;
