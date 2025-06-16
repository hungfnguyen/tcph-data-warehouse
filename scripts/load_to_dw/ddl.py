from sqlalchemy import create_engine, text
import logging
from .config import POSTGRES_CONFIG

logger = logging.getLogger(__name__)

def create_postgres_engine():
    conn_str = f"postgresql://{POSTGRES_CONFIG['user']}:{POSTGRES_CONFIG['password']}@{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}"
    return create_engine(conn_str)

def create_postgres_schemas_and_tables(engine):
    ddl_sql = """ 
    -- Create schemas
    CREATE SCHEMA IF NOT EXISTS marts;
    
    -- Drop existing tables if they exist
    DROP TABLE IF EXISTS marts.fct_revenue_by_supplier CASCADE;
    DROP TABLE IF EXISTS marts.fct_inventory CASCADE;
    DROP TABLE IF EXISTS marts.fct_sales CASCADE;
    DROP TABLE IF EXISTS marts.dim_date CASCADE;
    DROP TABLE IF EXISTS marts.dim_part CASCADE;
    DROP TABLE IF EXISTS marts.dim_supplier CASCADE;
    DROP TABLE IF EXISTS marts.dim_customer CASCADE;
    
    -- Create dimension tables
    CREATE TABLE marts.dim_customer (
        customer_key BIGINT PRIMARY KEY,
        customer_name VARCHAR(255),
        customer_address TEXT,
        market_segment VARCHAR(50),
        nation_key BIGINT,
        nation_name VARCHAR(100),
        region_key BIGINT,
        region_name VARCHAR(100),
        customer_phone VARCHAR(50),
        account_balance DECIMAL(15,2),
        account_balance_status VARCHAR(20),
        customer_comment TEXT,
        dbt_updated_at TIMESTAMP
    );
    
    CREATE TABLE marts.dim_supplier (
        supplier_key BIGINT PRIMARY KEY,
        supplier_name VARCHAR(255),
        supplier_address TEXT,
        nation_key BIGINT,
        nation_name VARCHAR(100),
        region_key BIGINT,
        region_name VARCHAR(100),
        supplier_phone VARCHAR(50),
        account_balance DECIMAL(15,2),
        supplier_comment TEXT,
        dbt_updated_at TIMESTAMP
    );
    
    CREATE TABLE marts.dim_part (
        part_key BIGINT PRIMARY KEY,
        name VARCHAR(255),
        mfgr VARCHAR(100),
        brand VARCHAR(100),
        type VARCHAR(100),
        size INTEGER,
        container VARCHAR(50),
        retail_price DECIMAL(15,2),
        part_comment TEXT,
        dbt_updated_at TIMESTAMP
    );
    
    CREATE TABLE marts.dim_date (
        date_key DATE PRIMARY KEY,
        year INTEGER,
        month INTEGER,
        day INTEGER,
        quarter INTEGER,
        week_of_year INTEGER,
        day_of_week INTEGER,
        is_weekend BOOLEAN,
        dbt_updated_at TIMESTAMP
    );
    
    -- Create fact tables
    CREATE TABLE marts.fct_sales (
        order_key BIGINT,
        line_number BIGINT,
        customer_key BIGINT,
        part_key BIGINT,
        supplier_key BIGINT,
        order_date DATE,
        ship_date DATE,
        quantity DECIMAL(15,2),
        extended_price DECIMAL(15,2),
        discount DECIMAL(15,2),
        tax DECIMAL(15,2),
        net_price DECIMAL(15,2),
        total_price DECIMAL(15,2),
        discount_amount DECIMAL(15,2),
        order_status VARCHAR(10),
        order_priority VARCHAR(20),
        order_total_price DECIMAL(15,2),
        return_flag VARCHAR(10),
        line_status VARCHAR(10),
        ship_mode VARCHAR(20),
        commit_date DATE,
        receipt_date DATE,
        dbt_updated_at TIMESTAMP,
        PRIMARY KEY (order_key, line_number)
    );
    
    CREATE TABLE marts.fct_inventory (
        part_key BIGINT,
        supplier_key BIGINT,
        available_qty DECIMAL(15,2),
        supply_cost DECIMAL(15,2),
        total_inventory_value DECIMAL(15,2),
        part_name VARCHAR(255),
        part_mfgr VARCHAR(100),
        part_brand VARCHAR(100),
        part_type VARCHAR(100),
        retail_price DECIMAL(15,2),
        supplier_name VARCHAR(255),
        supplier_nation VARCHAR(100),
        supplier_region VARCHAR(100),
        stock_level VARCHAR(20),
        cost_category VARCHAR(20),
        dbt_updated_at TIMESTAMP,
        PRIMARY KEY (part_key, supplier_key)
    );
    
    CREATE TABLE marts.fct_revenue_by_supplier (
        supplier_key BIGINT,
        date_key DATE,
        total_orders INTEGER,
        total_line_items INTEGER,
        total_quantity DECIMAL(15,2),
        revenue DECIMAL(15,2),
        gross_revenue DECIMAL(15,2),
        total_revenue DECIMAL(15,2),
        total_discount DECIMAL(15,2),
        avg_line_value DECIMAL(15,2),
        max_line_value DECIMAL(15,2),
        min_line_value DECIMAL(15,2),
        revenue_per_order DECIMAL(15,2),
        revenue_per_line DECIMAL(15,2),
        discount_rate DECIMAL(15,2),
        performance_tier VARCHAR(20),
        dbt_updated_at TIMESTAMP,
        PRIMARY KEY (supplier_key, date_key)
    );
    
    -- Create indexes for better performance
    CREATE INDEX idx_fct_sales_customer ON marts.fct_sales(customer_key);
    CREATE INDEX idx_fct_sales_part ON marts.fct_sales(part_key);
    CREATE INDEX idx_fct_sales_supplier ON marts.fct_sales(supplier_key);
    CREATE INDEX idx_fct_sales_order_date ON marts.fct_sales(order_date);
    
    CREATE INDEX idx_fct_inventory_part ON marts.fct_inventory(part_key);
    CREATE INDEX idx_fct_inventory_supplier ON marts.fct_inventory(supplier_key);
    
    CREATE INDEX idx_fct_revenue_supplier ON marts.fct_revenue_by_supplier(supplier_key);
    CREATE INDEX idx_fct_revenue_date ON marts.fct_revenue_by_supplier(date_key);
    """
    with engine.connect() as conn:
        for statement in ddl_sql.split(';'):
            if statement.strip():
                conn.execute(text(statement))
        conn.commit()
    logger.info("Created schemas & tables in PostgreSQL")