-- =====================================================
-- RFM Calculation: Recency, Frequency, Monetary
-- =====================================================

USE customer_analytics;

-- =====================================================
-- RFM Calculation Query
-- =====================================================

-- Calculate RFM metrics for all customers
-- This query calculates:
-- Recency: Days since last purchase (relative to cutoff date)
-- Frequency: Total number of distinct orders
-- Monetary: Total amount spent

-- Set cutoff date (typically current date or analysis date)
SET @cutoff_date = CURDATE();

-- Calculate RFM values
CREATE OR REPLACE VIEW vw_rfm_calculation AS
SELECT 
    cs.customer_id,
    
    -- RECENCY: Days since last purchase
    DATEDIFF(@cutoff_date, cs.last_purchase_date) AS recency_days,
    
    -- FREQUENCY: Total number of distinct orders
    cs.total_orders AS frequency,
    
    -- MONETARY: Total amount spent
    cs.total_spent AS monetary,
    
    -- Additional metrics for analysis
    cs.avg_order_value,
    cs.first_purchase_date,
    cs.last_purchase_date,
    cs.country
    
FROM customer_summary cs
WHERE cs.customer_id IS NOT NULL
  AND cs.last_purchase_date IS NOT NULL
  AND cs.total_orders > 0
  AND cs.total_spent > 0;

-- =====================================================
-- Alternative RFM Calculation with Window Functions
-- =====================================================

-- More detailed RFM calculation directly from transactions
CREATE OR REPLACE VIEW vw_rfm_detailed AS
WITH customer_metrics AS (
    SELECT 
        customer_id,
        MAX(DATE(invoice_date)) AS last_purchase_date,
        COUNT(DISTINCT invoice) AS frequency,
        SUM(order_value) AS monetary,
        AVG(order_value) AS avg_order_value,
        MIN(DATE(invoice_date)) AS first_purchase_date
    FROM cleaned_transactions
    WHERE customer_id IS NOT NULL
      AND data_quality_flag = 'CLEAN'
    GROUP BY customer_id
)
SELECT 
    customer_id,
    DATEDIFF(CURDATE(), last_purchase_date) AS recency_days,
    frequency,
    monetary,
    avg_order_value,
    first_purchase_date,
    last_purchase_date
FROM customer_metrics
WHERE frequency > 0 AND monetary > 0;

-- =====================================================
-- RFM Calculation with Date Range Filter
-- =====================================================

-- Function to calculate RFM for a specific date range
-- This can be used in stored procedures

-- Example: Calculate RFM for transactions in last 12 months
CREATE OR REPLACE VIEW vw_rfm_last_12_months AS
WITH customer_metrics AS (
    SELECT 
        customer_id,
        MAX(DATE(invoice_date)) AS last_purchase_date,
        COUNT(DISTINCT invoice) AS frequency,
        SUM(order_value) AS monetary
    FROM cleaned_transactions
    WHERE customer_id IS NOT NULL
      AND data_quality_flag = 'CLEAN'
      AND invoice_date >= DATE_SUB(CURDATE(), INTERVAL 12 MONTH)
    GROUP BY customer_id
)
SELECT 
    customer_id,
    DATEDIFF(CURDATE(), last_purchase_date) AS recency_days,
    frequency,
    monetary
FROM customer_metrics
WHERE frequency > 0 AND monetary > 0;

