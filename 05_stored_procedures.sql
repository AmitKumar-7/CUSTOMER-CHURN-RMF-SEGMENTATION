-- =====================================================
-- Stored Procedures for Automated RFM Segmentation
-- =====================================================

USE customer_analytics;

-- =====================================================
-- Procedure 1: Calculate and Update RFM Scores
-- =====================================================

DELIMITER //

CREATE PROCEDURE IF NOT EXISTS sp_calculate_rfm_segmentation(
    IN p_cutoff_date DATE,
    IN p_start_date DATE,
    IN p_end_date DATE
)
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        RESIGNAL;
    END;
    
    START TRANSACTION;
    
    -- Set cutoff date if not provided
    IF p_cutoff_date IS NULL THEN
        SET p_cutoff_date = CURDATE();
    END IF;
    
    -- Calculate RFM metrics for the specified date range
    WITH rfm_base AS (
        SELECT 
            cs.customer_id,
            DATEDIFF(p_cutoff_date, cs.last_purchase_date) AS recency_days,
            cs.total_orders AS frequency,
            cs.total_spent AS monetary
        FROM customer_summary cs
        WHERE cs.customer_id IS NOT NULL
          AND cs.last_purchase_date IS NOT NULL
          AND cs.total_orders > 0
          AND cs.total_spent > 0
          AND (p_start_date IS NULL OR cs.last_purchase_date >= p_start_date)
          AND (p_end_date IS NULL OR cs.last_purchase_date <= p_end_date)
    ),
    rfm_scored AS (
        SELECT 
            customer_id,
            recency_days,
            frequency,
            monetary,
            NTILE(4) OVER (ORDER BY recency_days ASC) AS recency_score,
            NTILE(4) OVER (ORDER BY frequency DESC) AS frequency_score,
            NTILE(4) OVER (ORDER BY monetary DESC) AS monetary_score
        FROM rfm_base
    ),
    rfm_segmented AS (
        SELECT 
            customer_id,
            recency_days,
            frequency,
            monetary,
            recency_score,
            frequency_score,
            monetary_score,
            CONCAT(recency_score, frequency_score, monetary_score) AS rfm_combined,
            CASE 
                WHEN recency_score >= 3 AND frequency_score >= 3 AND monetary_score >= 3 THEN 'Champions'
                WHEN frequency_score >= 3 AND monetary_score >= 3 AND recency_score BETWEEN 1 AND 2 THEN 'Loyal Customers'
                WHEN recency_score >= 3 AND frequency_score BETWEEN 2 AND 3 AND monetary_score BETWEEN 2 AND 3 THEN 'Potential Loyalists'
                WHEN recency_score >= 3 AND frequency_score <= 2 AND monetary_score <= 2 THEN 'New Customers'
                WHEN recency_score BETWEEN 2 AND 3 AND frequency_score <= 2 AND monetary_score BETWEEN 2 AND 3 THEN 'Promising'
                WHEN recency_score BETWEEN 2 AND 3 AND frequency_score BETWEEN 2 AND 3 AND monetary_score <= 2 THEN 'Need Attention'
                WHEN recency_score <= 2 AND frequency_score BETWEEN 2 AND 3 AND monetary_score BETWEEN 2 AND 3 THEN 'About to Sleep'
                WHEN recency_score <= 2 AND frequency_score BETWEEN 2 AND 3 AND monetary_score >= 3 THEN 'At Risk'
                WHEN recency_score <= 2 AND frequency_score >= 3 AND monetary_score >= 3 THEN 'Cannot Lose Them'
                WHEN recency_score <= 2 AND frequency_score <= 2 AND monetary_score BETWEEN 1 AND 3 THEN 'Hibernating'
                WHEN recency_score <= 2 AND frequency_score <= 2 AND monetary_score <= 2 THEN 'Lost'
                ELSE 'Other'
            END AS segment,
            CASE 
                WHEN recency_score <= 2 AND frequency_score <= 2 THEN 'High Risk'
                WHEN recency_score <= 2 AND frequency_score = 3 THEN 'Medium Risk'
                WHEN recency_score = 3 AND frequency_score <= 2 THEN 'Low Risk'
                ELSE 'No Risk'
            END AS churn_risk
        FROM rfm_scored
    )
    INSERT INTO rfm_scores (
        customer_id,
        recency_days,
        frequency,
        monetary,
        recency_score,
        frequency_score,
        monetary_score,
        rfm_combined,
        segment,
        churn_risk,
        analysis_date
    )
    SELECT 
        customer_id,
        recency_days,
        frequency,
        monetary,
        recency_score,
        frequency_score,
        monetary_score,
        rfm_combined,
        segment,
        churn_risk,
        p_cutoff_date
    FROM rfm_segmented
    ON DUPLICATE KEY UPDATE
        recency_days = VALUES(recency_days),
        frequency = VALUES(frequency),
        monetary = VALUES(monetary),
        recency_score = VALUES(recency_score),
        frequency_score = VALUES(frequency_score),
        monetary_score = VALUES(monetary_score),
        rfm_combined = VALUES(rfm_combined),
        segment = VALUES(segment),
        churn_risk = VALUES(churn_risk),
        analysis_date = VALUES(analysis_date),
        updated_at = CURRENT_TIMESTAMP;
    
    COMMIT;
    
    SELECT 
        CONCAT('RFM segmentation completed successfully. Analysis date: ', p_cutoff_date) AS message,
        COUNT(*) AS customers_processed
    FROM rfm_scores
    WHERE analysis_date = p_cutoff_date;
    
END //

DELIMITER ;

-- =====================================================
-- Procedure 2: Refresh Customer Summary
-- =====================================================

DELIMITER //

CREATE PROCEDURE IF NOT EXISTS sp_refresh_customer_summary()
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        RESIGNAL;
    END;
    
    START TRANSACTION;
    
    -- Update customer summary from cleaned transactions
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
        MAX(country) AS country
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
    
    COMMIT;
    
    SELECT 
        'Customer summary refreshed successfully' AS message,
        COUNT(*) AS total_customers
    FROM customer_summary;
    
END //

DELIMITER ;

-- =====================================================
-- Procedure 3: Complete ETL and RFM Pipeline
-- =====================================================

DELIMITER //

CREATE PROCEDURE IF NOT EXISTS sp_complete_rfm_pipeline(
    IN p_cutoff_date DATE,
    IN p_start_date DATE,
    IN p_end_date DATE
)
BEGIN
    DECLARE v_customer_count INT;
    
    -- Step 1: Refresh customer summary
    CALL sp_refresh_customer_summary();
    
    -- Step 2: Calculate RFM segmentation
    CALL sp_calculate_rfm_segmentation(p_cutoff_date, p_start_date, p_end_date);
    
    -- Step 3: Return summary
    SELECT 
        COUNT(*) AS total_customers,
        COUNT(DISTINCT segment) AS total_segments,
        COUNT(CASE WHEN churn_risk = 'High Risk' THEN 1 END) AS high_risk_customers,
        COUNT(CASE WHEN churn_risk = 'Medium Risk' THEN 1 END) AS medium_risk_customers,
        COUNT(CASE WHEN segment = 'Champions' THEN 1 END) AS champion_customers
    FROM rfm_scores
    WHERE analysis_date = COALESCE(p_cutoff_date, CURDATE());
    
END //

DELIMITER ;

-- =====================================================
-- Procedure 4: Get Customers by Segment
-- =====================================================

DELIMITER //

CREATE PROCEDURE IF NOT EXISTS sp_get_customers_by_segment(
    IN p_segment VARCHAR(50),
    IN p_churn_risk VARCHAR(20),
    IN p_limit INT
)
BEGIN
    SET p_limit = COALESCE(p_limit, 1000);
    
    SELECT 
        rs.customer_id,
        rs.recency_days,
        rs.frequency,
        rs.monetary,
        rs.recency_score,
        rs.frequency_score,
        rs.monetary_score,
        rs.rfm_combined,
        rs.segment,
        rs.churn_risk,
        cs.country,
        cs.first_purchase_date,
        cs.last_purchase_date,
        cs.avg_order_value
    FROM rfm_scores rs
    INNER JOIN customer_summary cs ON rs.customer_id = cs.customer_id
    WHERE (p_segment IS NULL OR rs.segment = p_segment)
      AND (p_churn_risk IS NULL OR rs.churn_risk = p_churn_risk)
      AND rs.analysis_date = (
          SELECT MAX(analysis_date) FROM rfm_scores
      )
    ORDER BY rs.monetary DESC
    LIMIT p_limit;
    
END //

DELIMITER ;

