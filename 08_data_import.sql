-- =====================================================
-- Data Import Script for CSV File
-- =====================================================

USE customer_analytics;

-- =====================================================
-- Method 1: Using LOAD DATA INFILE (Recommended for large files)
-- =====================================================

-- Note: Adjust the file path to match your system
-- On Windows, use forward slashes or double backslashes
-- Enable local_infile if needed: SET GLOBAL local_infile = 1;

LOAD DATA LOCAL INFILE 'online_retail_II.csv'
INTO TABLE raw_transactions
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(invoice, stock_code, description, quantity, @invoice_date, price, @customer_id, country)
SET 
    invoice_date = STR_TO_DATE(@invoice_date, '%Y-%m-%d %H:%i:%s'),
    customer_id = NULLIF(@customer_id, '');

-- =====================================================
-- Method 2: Alternative - Manual Insert (for testing)
-- =====================================================

-- If LOAD DATA INFILE doesn't work, you can use this template
-- to insert data manually or via a script

-- Example insert (replace with actual data):
/*
INSERT INTO raw_transactions (
    invoice, stock_code, description, quantity, invoice_date, price, customer_id, country
) VALUES (
    '489434', '85048', '15CM CHRISTMAS GLASS BALL 20 LIGHTS', 12, '2009-12-01 07:45:00', 6.95, '13085', 'United Kingdom'
);
*/

-- =====================================================
-- Method 3: Python/ETL Script Alternative
-- =====================================================

-- For better control over data cleaning, consider using a Python script:
-- See data_import.py for a Python-based import solution

-- =====================================================
-- Verify Data Import
-- =====================================================

-- Check imported record count
SELECT 
    COUNT(*) AS total_records,
    COUNT(DISTINCT invoice) AS unique_invoices,
    COUNT(DISTINCT customer_id) AS unique_customers,
    MIN(invoice_date) AS earliest_date,
    MAX(invoice_date) AS latest_date
FROM raw_transactions;

-- Check for data quality issues
SELECT 
    COUNT(*) AS records_with_null_customer,
    COUNT(CASE WHEN invoice IS NULL THEN 1 END) AS records_with_null_invoice,
    COUNT(CASE WHEN invoice_date IS NULL THEN 1 END) AS records_with_null_date
FROM raw_transactions;

