-- =====================================================
-- Test and Validation Script
-- Run this after setup to verify everything works
-- =====================================================

USE customer_analytics;

-- =====================================================
-- Test 1: Verify Database and Tables Exist
-- =====================================================

SELECT 'Test 1: Checking database and tables...' AS test_status;

SELECT 
    TABLE_NAME,
    TABLE_ROWS,
    ROUND(((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024), 2) AS 'Size (MB)'
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'customer_analytics'
  AND TABLE_NAME IN (
    'raw_transactions',
    'cleaned_transactions', 
    'customer_summary',
    'rfm_scores',
    'product_category_mapping'
  )
ORDER BY TABLE_NAME;

-- =====================================================
-- Test 2: Verify Data Import
-- =====================================================

SELECT 'Test 2: Checking data import...' AS test_status;

SELECT 
    'raw_transactions' AS table_name,
    COUNT(*) AS total_records,
    COUNT(DISTINCT invoice) AS unique_invoices,
    COUNT(DISTINCT customer_id) AS unique_customers,
    MIN(invoice_date) AS earliest_date,
    MAX(invoice_date) AS latest_date
FROM raw_transactions

UNION ALL

SELECT 
    'cleaned_transactions' AS table_name,
    COUNT(*) AS total_records,
    COUNT(DISTINCT invoice) AS unique_invoices,
    COUNT(DISTINCT customer_id) AS unique_customers,
    MIN(invoice_date) AS earliest_date,
    MAX(invoice_date) AS latest_date
FROM cleaned_transactions;

-- =====================================================
-- Test 3: Verify Data Quality
-- =====================================================

SELECT 'Test 3: Checking data quality...' AS test_status;

SELECT * FROM vw_data_quality_report;

-- =====================================================
-- Test 4: Verify Customer Summary
-- =====================================================

SELECT 'Test 4: Checking customer summary...' AS test_status;

SELECT 
    COUNT(*) AS total_customers,
    COUNT(CASE WHEN total_orders > 0 THEN 1 END) AS customers_with_orders,
    AVG(total_orders) AS avg_orders_per_customer,
    AVG(total_spent) AS avg_spending_per_customer,
    MIN(first_purchase_date) AS earliest_customer,
    MAX(last_purchase_date) AS latest_purchase
FROM customer_summary;

-- =====================================================
-- Test 5: Verify RFM Calculation Views
-- =====================================================

SELECT 'Test 5: Testing RFM calculation views...' AS test_status;

-- Test vw_rfm_calculation
SELECT 
    COUNT(*) AS rfm_calculated_customers,
    AVG(recency_days) AS avg_recency_days,
    AVG(frequency) AS avg_frequency,
    AVG(monetary) AS avg_monetary
FROM vw_rfm_calculation;

-- =====================================================
-- Test 6: Verify RFM Segmentation
-- =====================================================

SELECT 'Test 6: Testing RFM segmentation...' AS test_status;

-- Test vw_rfm_scores
SELECT 
    COUNT(*) AS scored_customers,
    COUNT(DISTINCT recency_score) AS unique_recency_scores,
    COUNT(DISTINCT frequency_score) AS unique_frequency_scores,
    COUNT(DISTINCT monetary_score) AS unique_monetary_scores,
    COUNT(DISTINCT rfm_combined) AS unique_rfm_combinations
FROM vw_rfm_scores;

-- Test vw_customer_segments
SELECT 
    segment,
    churn_risk,
    COUNT(*) AS customer_count
FROM vw_customer_segments
GROUP BY segment, churn_risk
ORDER BY customer_count DESC
LIMIT 10;

-- =====================================================
-- Test 7: Verify Stored Procedures
-- =====================================================

SELECT 'Test 7: Testing stored procedures...' AS test_status;

-- Test if procedures exist
SELECT 
    ROUTINE_NAME,
    ROUTINE_TYPE,
    CREATED,
    LAST_ALTERED
FROM INFORMATION_SCHEMA.ROUTINES
WHERE ROUTINE_SCHEMA = 'customer_analytics'
  AND ROUTINE_TYPE = 'PROCEDURE'
ORDER BY ROUTINE_NAME;

-- =====================================================
-- Test 8: Verify Reporting Views
-- =====================================================

SELECT 'Test 8: Testing reporting views...' AS test_status;

-- Test if views exist
SELECT 
    TABLE_NAME,
    VIEW_DEFINITION IS NOT NULL AS has_definition
FROM INFORMATION_SCHEMA.VIEWS
WHERE TABLE_SCHEMA = 'customer_analytics'
ORDER BY TABLE_NAME;

-- Test a few key views
SELECT 'Testing vw_segment_summary...' AS view_test;
SELECT COUNT(*) AS segment_count FROM vw_segment_summary;

SELECT 'Testing vw_churn_risk_analysis...' AS view_test;
SELECT COUNT(*) AS risk_category_count FROM vw_churn_risk_analysis;

-- =====================================================
-- Test 9: Verify Indexes
-- =====================================================

SELECT 'Test 9: Checking indexes...' AS test_status;

SELECT 
    TABLE_NAME,
    INDEX_NAME,
    GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX SEPARATOR ', ') AS columns,
    INDEX_TYPE
FROM INFORMATION_SCHEMA.STATISTICS
WHERE TABLE_SCHEMA = 'customer_analytics'
  AND TABLE_NAME IN ('raw_transactions', 'cleaned_transactions', 'customer_summary', 'rfm_scores')
GROUP BY TABLE_NAME, INDEX_NAME, INDEX_TYPE
ORDER BY TABLE_NAME, INDEX_NAME;

-- =====================================================
-- Test 10: Test RFM Pipeline (if data exists)
-- =====================================================

SELECT 'Test 10: Testing RFM pipeline...' AS test_status;

-- Only run if we have data
SET @customer_count = (SELECT COUNT(*) FROM customer_summary);

SELECT 
    CASE 
        WHEN @customer_count > 0 THEN 'Data available - can test pipeline'
        ELSE 'No data - skip pipeline test'
    END AS pipeline_status,
    @customer_count AS customer_count;

-- If data exists, test the pipeline
-- Uncomment the line below to actually run the pipeline
-- CALL sp_complete_rfm_pipeline(NULL, NULL, NULL);

-- =====================================================
-- Test 11: Sample Queries
-- =====================================================

SELECT 'Test 11: Running sample queries...' AS test_status;

-- Sample: Get top 5 customers by monetary value
SELECT 
    customer_id,
    monetary AS total_spent,
    frequency AS total_orders,
    recency_days,
    segment,
    churn_risk
FROM vw_rfm_scores
ORDER BY monetary DESC
LIMIT 5;

-- Sample: Segment distribution
SELECT 
    segment,
    COUNT(*) AS customer_count,
    ROUND(AVG(monetary), 2) AS avg_value
FROM vw_rfm_scores
GROUP BY segment
ORDER BY customer_count DESC;

-- =====================================================
-- Test Summary
-- =====================================================

SELECT 'All tests completed!' AS final_status;

SELECT 
    'Validation Summary' AS summary,
    (SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'customer_analytics') AS total_tables,
    (SELECT COUNT(*) FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = 'customer_analytics') AS total_views,
    (SELECT COUNT(*) FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA = 'customer_analytics' AND ROUTINE_TYPE = 'PROCEDURE') AS total_procedures,
    (SELECT COUNT(*) FROM raw_transactions) AS raw_transaction_count,
    (SELECT COUNT(*) FROM cleaned_transactions) AS cleaned_transaction_count,
    (SELECT COUNT(*) FROM customer_summary) AS customer_count,
    (SELECT COUNT(*) FROM rfm_scores) AS rfm_score_count;

