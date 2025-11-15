-- =====================================================
-- Reporting Views for Business Intelligence Tools
-- =====================================================

USE customer_analytics;

-- =====================================================
-- View 1: Complete Customer RFM Profile
-- =====================================================

CREATE OR REPLACE VIEW vw_customer_rfm_profile AS
SELECT 
    rs.customer_id,
    cs.country,
    cs.first_purchase_date,
    cs.last_purchase_date,
    DATEDIFF(CURDATE(), cs.last_purchase_date) AS days_since_last_purchase,
    rs.recency_days,
    rs.frequency,
    rs.monetary,
    rs.recency_score,
    rs.frequency_score,
    rs.monetary_score,
    rs.rfm_combined,
    rs.segment,
    rs.churn_risk,
    cs.avg_order_value,
    cs.total_orders,
    rs.analysis_date
FROM rfm_scores rs
INNER JOIN customer_summary cs ON rs.customer_id = cs.customer_id
WHERE rs.analysis_date = (
    SELECT MAX(analysis_date) FROM rfm_scores
);

-- =====================================================
-- View 2: Segment Summary Statistics
-- =====================================================

CREATE OR REPLACE VIEW vw_segment_summary AS
SELECT 
    segment,
    churn_risk,
    COUNT(*) AS customer_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM rfm_scores WHERE analysis_date = (SELECT MAX(analysis_date) FROM rfm_scores)), 2) AS percentage,
    AVG(recency_days) AS avg_recency_days,
    MIN(recency_days) AS min_recency_days,
    MAX(recency_days) AS max_recency_days,
    AVG(frequency) AS avg_frequency,
    MIN(frequency) AS min_frequency,
    MAX(frequency) AS max_frequency,
    AVG(monetary) AS avg_monetary,
    MIN(monetary) AS min_monetary,
    MAX(monetary) AS max_monetary,
    SUM(monetary) AS total_revenue,
    MAX(analysis_date) AS last_analysis_date
FROM rfm_scores
WHERE analysis_date = (
    SELECT MAX(analysis_date) FROM rfm_scores
)
GROUP BY segment, churn_risk
ORDER BY customer_count DESC;

-- =====================================================
-- View 3: Churn Risk Analysis
-- =====================================================

CREATE OR REPLACE VIEW vw_churn_risk_analysis AS
SELECT 
    churn_risk,
    COUNT(*) AS customer_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM rfm_scores WHERE analysis_date = (SELECT MAX(analysis_date) FROM rfm_scores)), 2) AS percentage,
    AVG(recency_days) AS avg_days_since_last_purchase,
    AVG(frequency) AS avg_frequency,
    AVG(monetary) AS avg_monetary,
    SUM(monetary) AS total_revenue_at_risk,
    GROUP_CONCAT(DISTINCT segment ORDER BY segment SEPARATOR ', ') AS segments_in_risk
FROM rfm_scores
WHERE analysis_date = (
    SELECT MAX(analysis_date) FROM rfm_scores
)
GROUP BY churn_risk
ORDER BY 
    CASE churn_risk
        WHEN 'High Risk' THEN 1
        WHEN 'Medium Risk' THEN 2
        WHEN 'Low Risk' THEN 3
        WHEN 'No Risk' THEN 4
    END;

-- =====================================================
-- View 4: High-Value Customers (Top Customers)
-- =====================================================

CREATE OR REPLACE VIEW vw_high_value_customers AS
SELECT 
    rs.customer_id,
    cs.country,
    rs.segment,
    rs.churn_risk,
    rs.monetary AS total_spent,
    rs.frequency AS total_orders,
    cs.avg_order_value,
    rs.recency_days,
    cs.last_purchase_date,
    rs.rfm_combined
FROM rfm_scores rs
INNER JOIN customer_summary cs ON rs.customer_id = cs.customer_id
WHERE rs.analysis_date = (
    SELECT MAX(analysis_date) FROM rfm_scores
)
  AND rs.monetary_score >= 3  -- Top 50% by monetary value
ORDER BY rs.monetary DESC;

-- =====================================================
-- View 5: Customers Needing Attention
-- =====================================================

CREATE OR REPLACE VIEW vw_customers_need_attention AS
SELECT 
    rs.customer_id,
    cs.country,
    rs.segment,
    rs.churn_risk,
    rs.recency_days,
    rs.frequency,
    rs.monetary,
    cs.last_purchase_date,
    DATEDIFF(CURDATE(), cs.last_purchase_date) AS days_since_last_purchase,
    rs.rfm_combined
FROM rfm_scores rs
INNER JOIN customer_summary cs ON rs.customer_id = cs.customer_id
WHERE rs.analysis_date = (
    SELECT MAX(analysis_date) FROM rfm_scores
)
  AND (
    rs.churn_risk IN ('High Risk', 'Medium Risk')
    OR rs.segment IN ('At Risk', 'Cannot Lose Them', 'About to Sleep', 'Need Attention')
  )
ORDER BY rs.churn_risk DESC, rs.monetary DESC;

-- =====================================================
-- View 6: Customer Lifetime Value by Segment
-- =====================================================

CREATE OR REPLACE VIEW vw_customer_lifetime_value AS
SELECT 
    rs.segment,
    COUNT(*) AS customer_count,
    AVG(rs.monetary) AS avg_customer_value,
    SUM(rs.monetary) AS total_segment_value,
    AVG(rs.frequency) AS avg_orders_per_customer,
    AVG(cs.avg_order_value) AS avg_order_value,
    AVG(DATEDIFF(cs.last_purchase_date, cs.first_purchase_date)) AS avg_customer_lifespan_days,
    MAX(rs.analysis_date) AS last_analysis_date
FROM rfm_scores rs
INNER JOIN customer_summary cs ON rs.customer_id = cs.customer_id
WHERE rs.analysis_date = (
    SELECT MAX(analysis_date) FROM rfm_scores
)
GROUP BY rs.segment
ORDER BY avg_customer_value DESC;

-- =====================================================
-- View 7: Monthly Customer Activity Trends
-- =====================================================

CREATE OR REPLACE VIEW vw_monthly_customer_activity AS
SELECT 
    DATE_FORMAT(ct.invoice_date, '%Y-%m') AS month_year,
    COUNT(DISTINCT ct.customer_id) AS active_customers,
    COUNT(DISTINCT ct.invoice) AS total_orders,
    SUM(ct.order_value) AS total_revenue,
    AVG(ct.order_value) AS avg_order_value,
    COUNT(DISTINCT ct.product_category) AS product_categories
FROM cleaned_transactions ct
WHERE ct.data_quality_flag = 'CLEAN'
  AND ct.customer_id IS NOT NULL
GROUP BY DATE_FORMAT(ct.invoice_date, '%Y-%m')
ORDER BY month_year DESC;

-- =====================================================
-- View 8: Product Category Performance by Segment
-- =====================================================

CREATE OR REPLACE VIEW vw_segment_product_performance AS
SELECT 
    rs.segment,
    ct.product_category,
    COUNT(DISTINCT ct.customer_id) AS customers,
    COUNT(DISTINCT ct.invoice) AS orders,
    SUM(ct.order_value) AS revenue,
    AVG(ct.order_value) AS avg_order_value
FROM rfm_scores rs
INNER JOIN cleaned_transactions ct ON rs.customer_id = ct.customer_id
WHERE rs.analysis_date = (
    SELECT MAX(analysis_date) FROM rfm_scores
)
  AND ct.data_quality_flag = 'CLEAN'
GROUP BY rs.segment, ct.product_category
ORDER BY rs.segment, revenue DESC;

-- =====================================================
-- View 9: Geographic Distribution by Segment
-- =====================================================

CREATE OR REPLACE VIEW vw_geographic_segment_distribution AS
SELECT 
    cs.country,
    rs.segment,
    rs.churn_risk,
    COUNT(*) AS customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY cs.country), 2) AS percentage_in_country,
    AVG(rs.monetary) AS avg_customer_value,
    SUM(rs.monetary) AS total_revenue
FROM rfm_scores rs
INNER JOIN customer_summary cs ON rs.customer_id = cs.customer_id
WHERE rs.analysis_date = (
    SELECT MAX(analysis_date) FROM rfm_scores
)
GROUP BY cs.country, rs.segment, rs.churn_risk
ORDER BY cs.country, customer_count DESC;

-- =====================================================
-- View 10: RFM Score Distribution
-- =====================================================

CREATE OR REPLACE VIEW vw_rfm_score_distribution AS
SELECT 
    recency_score,
    frequency_score,
    monetary_score,
    rfm_combined,
    COUNT(*) AS customer_count,
    AVG(recency_days) AS avg_recency_days,
    AVG(frequency) AS avg_frequency,
    AVG(monetary) AS avg_monetary
FROM rfm_scores
WHERE analysis_date = (
    SELECT MAX(analysis_date) FROM rfm_scores
)
GROUP BY recency_score, frequency_score, monetary_score, rfm_combined
ORDER BY recency_score DESC, frequency_score DESC, monetary_score DESC;

