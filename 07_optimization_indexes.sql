-- =====================================================
-- Database Optimization: Indexes for Performance
-- =====================================================

USE customer_analytics;

-- =====================================================
-- Additional Indexes for Raw Transactions
-- =====================================================
-- Note: IF NOT EXISTS is supported in MySQL 8.0.17+
-- For older versions, remove IF NOT EXISTS and handle errors manually

-- Composite index for date range queries
CREATE INDEX IF NOT EXISTS idx_raw_transactions_date_customer 
ON raw_transactions(invoice_date, customer_id);

-- Index for invoice lookups
CREATE INDEX IF NOT EXISTS idx_raw_transactions_invoice_date 
ON raw_transactions(invoice, invoice_date);

-- =====================================================
-- Additional Indexes for Cleaned Transactions
-- =====================================================

-- Composite index for customer transaction history
CREATE INDEX IF NOT EXISTS idx_cleaned_transactions_customer_date 
ON cleaned_transactions(customer_id, invoice_date);

-- Composite index for segment analysis
CREATE INDEX IF NOT EXISTS idx_cleaned_transactions_customer_category 
ON cleaned_transactions(customer_id, product_category);

-- Index for date range filtering
CREATE INDEX IF NOT EXISTS idx_cleaned_transactions_date_range 
ON cleaned_transactions(invoice_date, data_quality_flag);

-- Composite index for order value analysis
CREATE INDEX IF NOT EXISTS idx_cleaned_transactions_value_date 
ON cleaned_transactions(order_value, invoice_date);

-- =====================================================
-- Additional Indexes for Customer Summary
-- =====================================================

-- Composite index for RFM calculations
CREATE INDEX IF NOT EXISTS idx_customer_summary_purchase_dates 
ON customer_summary(last_purchase_date, first_purchase_date);

-- Index for country-based analysis
CREATE INDEX IF NOT EXISTS idx_customer_summary_country 
ON customer_summary(country, total_spent);

-- =====================================================
-- Additional Indexes for RFM Scores
-- =====================================================

-- Composite index for segment queries
CREATE INDEX IF NOT EXISTS idx_rfm_scores_segment_risk 
ON rfm_scores(segment, churn_risk);

-- Composite index for score-based queries
CREATE INDEX IF NOT EXISTS idx_rfm_scores_scores 
ON rfm_scores(recency_score, frequency_score, monetary_score);

-- Composite index for analysis date and segment
CREATE INDEX IF NOT EXISTS idx_rfm_scores_analysis_segment 
ON rfm_scores(analysis_date, segment);

-- Index for monetary value queries
CREATE INDEX IF NOT EXISTS idx_rfm_scores_monetary 
ON rfm_scores(monetary DESC);

-- =====================================================
-- Analyze Tables for Query Optimization
-- =====================================================

-- Update table statistics for query optimizer
ANALYZE TABLE raw_transactions;
ANALYZE TABLE cleaned_transactions;
ANALYZE TABLE customer_summary;
ANALYZE TABLE rfm_scores;

-- =====================================================
-- Performance Optimization Tips
-- =====================================================

-- 1. For large datasets, consider partitioning tables by date
-- 2. Use EXPLAIN to analyze query execution plans
-- 3. Monitor slow query log for optimization opportunities
-- 4. Consider materialized views for frequently accessed aggregations
-- 5. Regular maintenance: OPTIMIZE TABLE for fragmented tables

-- =====================================================
-- Query Performance Monitoring Views
-- =====================================================

-- View to check index usage (requires MySQL 5.7+)
CREATE OR REPLACE VIEW vw_index_usage AS
SELECT 
    TABLE_NAME,
    INDEX_NAME,
    SEQ_IN_INDEX,
    COLUMN_NAME,
    CARDINALITY,
    INDEX_TYPE
FROM INFORMATION_SCHEMA.STATISTICS
WHERE TABLE_SCHEMA = 'customer_analytics'
  AND TABLE_NAME IN ('raw_transactions', 'cleaned_transactions', 'customer_summary', 'rfm_scores')
ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX;

-- =====================================================
-- Table Size Information
-- =====================================================

CREATE OR REPLACE VIEW vw_table_sizes AS
SELECT 
    TABLE_NAME,
    ROUND(((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024), 2) AS 'Size (MB)',
    ROUND((DATA_LENGTH / 1024 / 1024), 2) AS 'Data (MB)',
    ROUND((INDEX_LENGTH / 1024 / 1024), 2) AS 'Index (MB)',
    TABLE_ROWS AS 'Row Count'
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'customer_analytics'
  AND TABLE_NAME IN ('raw_transactions', 'cleaned_transactions', 'customer_summary', 'rfm_scores')
ORDER BY (DATA_LENGTH + INDEX_LENGTH) DESC;

