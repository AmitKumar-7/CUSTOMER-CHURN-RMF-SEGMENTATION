-- =====================================================
-- RFM Segmentation using NTILE() and RANK()
-- =====================================================

USE customer_analytics;

-- =====================================================
-- Step 1: Calculate RFM Scores using NTILE()
-- =====================================================

-- NTILE(4) divides customers into 4 quartiles (1-4)
-- For Recency: Lower days = Higher score (more recent = better)
-- For Frequency: Higher frequency = Higher score
-- For Monetary: Higher spending = Higher score

CREATE OR REPLACE VIEW vw_rfm_scores AS
WITH rfm_base AS (
    SELECT 
        customer_id,
        recency_days,
        frequency,
        monetary
    FROM vw_rfm_calculation
),
rfm_scored AS (
    SELECT 
        customer_id,
        recency_days,
        frequency,
        monetary,
        -- Recency Score: Lower days = Higher score (NTILE ordered DESC for recency)
        -- We want most recent customers to get score 4
        NTILE(4) OVER (ORDER BY recency_days ASC) AS recency_score,
        
        -- Frequency Score: Higher frequency = Higher score
        NTILE(4) OVER (ORDER BY frequency DESC) AS frequency_score,
        
        -- Monetary Score: Higher spending = Higher score
        NTILE(4) OVER (ORDER BY monetary DESC) AS monetary_score
    FROM rfm_base
)
SELECT 
    customer_id,
    recency_days,
    frequency,
    monetary,
    recency_score,
    frequency_score,
    monetary_score,
    -- Combine scores as string (e.g., "444", "144")
    CONCAT(recency_score, frequency_score, monetary_score) AS rfm_combined
FROM rfm_scored;

-- =====================================================
-- Step 2: Alternative using RANK() for scoring
-- =====================================================

-- Using RANK() to create percentile-based scores
CREATE OR REPLACE VIEW vw_rfm_scores_rank AS
WITH rfm_base AS (
    SELECT 
        customer_id,
        recency_days,
        frequency,
        monetary
    FROM vw_rfm_calculation
),
rfm_ranked AS (
    SELECT 
        customer_id,
        recency_days,
        frequency,
        monetary,
        -- Calculate percentiles using RANK()
        CASE 
            WHEN recency_rank <= total_customers * 0.25 THEN 4
            WHEN recency_rank <= total_customers * 0.50 THEN 3
            WHEN recency_rank <= total_customers * 0.75 THEN 2
            ELSE 1
        END AS recency_score,
        CASE 
            WHEN frequency_rank <= total_customers * 0.25 THEN 4
            WHEN frequency_rank <= total_customers * 0.50 THEN 3
            WHEN frequency_rank <= total_customers * 0.75 THEN 2
            ELSE 1
        END AS frequency_score,
        CASE 
            WHEN monetary_rank <= total_customers * 0.25 THEN 4
            WHEN monetary_rank <= total_customers * 0.50 THEN 3
            WHEN monetary_rank <= total_customers * 0.75 THEN 2
            ELSE 1
        END AS monetary_score
    FROM (
        SELECT 
            customer_id,
            recency_days,
            frequency,
            monetary,
            RANK() OVER (ORDER BY recency_days ASC) AS recency_rank,
            RANK() OVER (ORDER BY frequency DESC) AS frequency_rank,
            RANK() OVER (ORDER BY monetary DESC) AS monetary_rank,
            COUNT(*) OVER () AS total_customers
        FROM rfm_base
    ) ranked
)
SELECT 
    customer_id,
    recency_days,
    frequency,
    monetary,
    recency_score,
    frequency_score,
    monetary_score,
    CONCAT(recency_score, frequency_score, monetary_score) AS rfm_combined
FROM rfm_ranked;

-- =====================================================
-- Step 3: Customer Segmentation
-- =====================================================

-- Map RFM scores to customer segments
CREATE OR REPLACE VIEW vw_customer_segments AS
SELECT 
    rfm.customer_id,
    rfm.recency_days,
    rfm.frequency,
    rfm.monetary,
    rfm.recency_score,
    rfm.frequency_score,
    rfm.monetary_score,
    rfm.rfm_combined,
    
    -- Segment assignment based on RFM scores
    CASE 
        -- Champions: High R, High F, High M (444, 443, 434, 433, 344, 343, 334, 333)
        WHEN rfm.recency_score >= 3 AND rfm.frequency_score >= 3 AND rfm.monetary_score >= 3 THEN 'Champions'
        
        -- Loyal Customers: High F, High M, Medium R (244, 243, 234, 233, 144, 143, 134, 133)
        WHEN rfm.frequency_score >= 3 AND rfm.monetary_score >= 3 AND rfm.recency_score BETWEEN 1 AND 2 THEN 'Loyal Customers'
        
        -- Potential Loyalists: High R, Medium F, Medium M (424, 423, 324, 323, 414, 413, 314, 313)
        WHEN rfm.recency_score >= 3 AND rfm.frequency_score BETWEEN 2 AND 3 AND rfm.monetary_score BETWEEN 2 AND 3 THEN 'Potential Loyalists'
        
        -- New Customers: High R, Low F, Low M (411, 412, 421, 422, 311, 312, 321, 322)
        WHEN rfm.recency_score >= 3 AND rfm.frequency_score <= 2 AND rfm.monetary_score <= 2 THEN 'New Customers'
        
        -- Promising: Medium R, Low F, Medium M (241, 242, 231, 232, 141, 142, 131, 132)
        WHEN rfm.recency_score BETWEEN 2 AND 3 AND rfm.frequency_score <= 2 AND rfm.monetary_score BETWEEN 2 AND 3 THEN 'Promising'
        
        -- Need Attention: Medium R, Medium F, Low M (214, 213, 124, 123, 114, 113)
        WHEN rfm.recency_score BETWEEN 2 AND 3 AND rfm.frequency_score BETWEEN 2 AND 3 AND rfm.monetary_score <= 2 THEN 'Need Attention'
        
        -- About to Sleep: Low R, Medium F, Medium M (244, 243, 234, 233 but with low R)
        WHEN rfm.recency_score <= 2 AND rfm.frequency_score BETWEEN 2 AND 3 AND rfm.monetary_score BETWEEN 2 AND 3 THEN 'About to Sleep'
        
        -- At Risk: Low R, Medium F, High M (144, 143, 134, 133)
        WHEN rfm.recency_score <= 2 AND rfm.frequency_score BETWEEN 2 AND 3 AND rfm.monetary_score >= 3 THEN 'At Risk'
        
        -- Cannot Lose Them: Low R, High F, High M (144, 143, 134, 133 with high F)
        WHEN rfm.recency_score <= 2 AND rfm.frequency_score >= 3 AND rfm.monetary_score >= 3 THEN 'Cannot Lose Them'
        
        -- Hibernating: Low R, Low F, Medium M (211, 212, 221, 222, 111, 112, 121, 122)
        WHEN rfm.recency_score <= 2 AND rfm.frequency_score <= 2 AND rfm.monetary_score BETWEEN 1 AND 3 THEN 'Hibernating'
        
        -- Lost: Low R, Low F, Low M (111, 112, 121, 122, 211, 212, 221, 222)
        WHEN rfm.recency_score <= 2 AND rfm.frequency_score <= 2 AND rfm.monetary_score <= 2 THEN 'Lost'
        
        ELSE 'Other'
    END AS segment,
    
    -- Churn Risk Assessment
    CASE 
        WHEN rfm.recency_score <= 2 AND rfm.frequency_score <= 2 THEN 'High Risk'
        WHEN rfm.recency_score <= 2 AND rfm.frequency_score = 3 THEN 'Medium Risk'
        WHEN rfm.recency_score = 3 AND rfm.frequency_score <= 2 THEN 'Low Risk'
        ELSE 'No Risk'
    END AS churn_risk
    
FROM vw_rfm_scores rfm;

-- =====================================================
-- Step 4: Populate RFM Scores Table
-- =====================================================

-- Procedure to populate rfm_scores table (called from stored procedure)
-- This will be used in the stored procedure

-- =====================================================
-- Step 5: Segment Distribution Summary
-- =====================================================

CREATE OR REPLACE VIEW vw_segment_distribution AS
SELECT 
    segment,
    churn_risk,
    COUNT(*) AS customer_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM vw_customer_segments), 2) AS percentage,
    AVG(recency_days) AS avg_recency_days,
    AVG(frequency) AS avg_frequency,
    AVG(monetary) AS avg_monetary
FROM vw_customer_segments
GROUP BY segment, churn_risk
ORDER BY customer_count DESC;

