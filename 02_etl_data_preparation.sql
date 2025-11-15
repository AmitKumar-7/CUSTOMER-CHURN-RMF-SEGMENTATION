-- =====================================================
-- ETL: Data Preparation and Cleaning
-- =====================================================

USE customer_analytics;

-- =====================================================
-- Step 1: Load Raw Data (Simulation)
-- Note: In production, use LOAD DATA INFILE or import tools
-- =====================================================

-- Example: Insert sample cleaned data from raw_transactions
-- This simulates the ETL process

-- =====================================================
-- Step 2: Data Cleaning and Transformation
-- =====================================================

-- Clean and transform raw transaction data
INSERT INTO cleaned_transactions (
    invoice,
    stock_code,
    description,
    quantity,
    invoice_date,
    price,
    customer_id,
    country,
    product_category,
    order_value,
    data_quality_flag
)
SELECT 
    -- Handle NULL invoices - assign placeholder
    COALESCE(TRIM(invoice), 'UNKNOWN') AS invoice,
    
    -- Clean stock code
    TRIM(COALESCE(stock_code, '')) AS stock_code,
    
    -- Clean description
    TRIM(COALESCE(description, 'Unknown Product')) AS description,
    
    -- Handle NULL or negative quantities
    CASE 
        WHEN quantity IS NULL OR quantity <= 0 THEN 1
        ELSE quantity
    END AS quantity,
    
    -- Handle NULL dates - use current date as fallback
    COALESCE(invoice_date, CURRENT_TIMESTAMP) AS invoice_date,
    
    -- Handle NULL or negative prices
    CASE 
        WHEN price IS NULL OR price <= 0 THEN 0.01
        ELSE price
    END AS price,
    
    -- Handle NULL customer IDs - mark for exclusion
    CASE 
        WHEN customer_id IS NULL OR TRIM(customer_id) = '' THEN NULL
        ELSE TRIM(customer_id)
    END AS customer_id,
    
    -- Clean country
    TRIM(COALESCE(country, 'Unknown')) AS country,
    
    -- Categorize products using CASE statements
    CASE 
        WHEN UPPER(description) LIKE '%LIGHT%' OR UPPER(description) LIKE '%LAMP%' THEN 'Lighting'
        WHEN UPPER(description) LIKE '%BAG%' OR UPPER(description) LIKE '%POUCH%' THEN 'Bags'
        WHEN UPPER(description) LIKE '%MUG%' OR UPPER(description) LIKE '%CUP%' THEN 'Drinkware'
        WHEN UPPER(description) LIKE '%CARD%' OR UPPER(description) LIKE '%GREETING%' THEN 'Cards'
        WHEN UPPER(description) LIKE '%TOY%' OR UPPER(description) LIKE '%GAME%' THEN 'Toys'
        WHEN UPPER(description) LIKE '%CLOTH%' OR UPPER(description) LIKE '%DRESS%' THEN 'Clothing'
        WHEN UPPER(description) LIKE '%BOOK%' OR UPPER(description) LIKE '%MANUAL%' THEN 'Books'
        WHEN UPPER(description) LIKE '%DECORATION%' OR UPPER(description) LIKE '%ORNAMENT%' THEN 'Decorations'
        WHEN UPPER(description) LIKE '%KITCHEN%' OR UPPER(description) LIKE '%UTENSIL%' THEN 'Kitchen'
        ELSE 'Other'
    END AS product_category,
    
    -- Calculate order value
    CASE 
        WHEN quantity IS NULL OR quantity <= 0 THEN 
            CASE WHEN price IS NULL OR price <= 0 THEN 0.01 ELSE price END
        WHEN price IS NULL OR price <= 0 THEN 
            CASE WHEN quantity IS NULL OR quantity <= 0 THEN 0.01 ELSE quantity * 0.01 END
        ELSE quantity * price
    END AS order_value,
    
    -- Data quality flag
    CASE 
        WHEN invoice IS NULL THEN 'MISSING_INVOICE'
        WHEN customer_id IS NULL OR TRIM(customer_id) = '' THEN 'MISSING_CUSTOMER'
        WHEN invoice_date IS NULL THEN 'MISSING_DATE'
        WHEN quantity IS NULL OR quantity <= 0 THEN 'INVALID_QUANTITY'
        WHEN price IS NULL OR price <= 0 THEN 'INVALID_PRICE'
        ELSE 'CLEAN'
    END AS data_quality_flag

FROM raw_transactions
WHERE customer_id IS NOT NULL 
  AND TRIM(customer_id) != ''
  AND invoice IS NOT NULL
  AND invoice_date IS NOT NULL;

-- =====================================================
-- Step 3: Create Customer Summary
-- =====================================================

-- Populate customer summary table
INSERT INTO customer_summary (
    customer_id,
    first_purchase_date,
    last_purchase_date,
    total_orders,
    total_spent,
    avg_order_value,
    country
)
SELECT 
    customer_id,
    MIN(DATE(invoice_date)) AS first_purchase_date,
    MAX(DATE(invoice_date)) AS last_purchase_date,
    COUNT(DISTINCT invoice) AS total_orders,
    SUM(order_value) AS total_spent,
    AVG(order_value) AS avg_order_value,
    MAX(country) AS country  -- Assuming one country per customer
FROM cleaned_transactions
WHERE customer_id IS NOT NULL
  AND data_quality_flag = 'CLEAN'
GROUP BY customer_id
ON DUPLICATE KEY UPDATE
    first_purchase_date = VALUES(first_purchase_date),
    last_purchase_date = VALUES(last_purchase_date),
    total_orders = VALUES(total_orders),
    total_spent = VALUES(total_spent),
    avg_order_value = VALUES(avg_order_value),
    country = VALUES(country),
    updated_at = CURRENT_TIMESTAMP;

-- =====================================================
-- Step 4: Data Quality Report
-- =====================================================

-- View to check data quality
CREATE OR REPLACE VIEW vw_data_quality_report AS
SELECT 
    data_quality_flag,
    COUNT(*) AS record_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM cleaned_transactions), 2) AS percentage
FROM cleaned_transactions
GROUP BY data_quality_flag
ORDER BY record_count DESC;

