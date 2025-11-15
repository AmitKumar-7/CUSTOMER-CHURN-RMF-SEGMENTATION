-- =====================================================
-- Customer Churn Prediction and RFM Segmentation
-- Database Schema Setup
-- =====================================================

-- Create Database
CREATE DATABASE IF NOT EXISTS customer_analytics;
USE customer_analytics;

-- =====================================================
-- Raw Transaction Data Table
-- =====================================================
CREATE TABLE IF NOT EXISTS raw_transactions (
    transaction_id INT AUTO_INCREMENT PRIMARY KEY,
    invoice VARCHAR(50),
    stock_code VARCHAR(50),
    description TEXT,
    quantity INT,
    invoice_date DATETIME,
    price DECIMAL(10, 2),
    customer_id VARCHAR(50),
    country VARCHAR(100),
    order_value DECIMAL(10, 2) GENERATED ALWAYS AS (quantity * price) STORED,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_invoice (invoice),
    INDEX idx_customer_id (customer_id),
    INDEX idx_invoice_date (invoice_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =====================================================
-- Cleaned Transaction Data Table
-- =====================================================
CREATE TABLE IF NOT EXISTS cleaned_transactions (
    transaction_id INT AUTO_INCREMENT PRIMARY KEY,
    invoice VARCHAR(50) NOT NULL,
    stock_code VARCHAR(50),
    description TEXT,
    quantity INT NOT NULL,
    invoice_date DATETIME NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    customer_id VARCHAR(50) NOT NULL,
    country VARCHAR(100),
    product_category VARCHAR(100),
    order_value DECIMAL(10, 2) NOT NULL,
    data_quality_flag VARCHAR(20) DEFAULT 'CLEAN',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_invoice (invoice),
    INDEX idx_customer_id (customer_id),
    INDEX idx_invoice_date (invoice_date),
    INDEX idx_product_category (product_category)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =====================================================
-- Customer Summary Table
-- =====================================================
CREATE TABLE IF NOT EXISTS customer_summary (
    customer_id VARCHAR(50) PRIMARY KEY,
    first_purchase_date DATE,
    last_purchase_date DATE,
    total_orders INT DEFAULT 0,
    total_spent DECIMAL(12, 2) DEFAULT 0.00,
    avg_order_value DECIMAL(10, 2) DEFAULT 0.00,
    country VARCHAR(100),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_last_purchase (last_purchase_date),
    INDEX idx_total_orders (total_orders)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =====================================================
-- RFM Scores Table
-- =====================================================
CREATE TABLE IF NOT EXISTS rfm_scores (
    customer_id VARCHAR(50) PRIMARY KEY,
    recency_days INT,
    frequency INT,
    monetary DECIMAL(12, 2),
    recency_score INT,
    frequency_score INT,
    monetary_score INT,
    rfm_combined VARCHAR(10),
    segment VARCHAR(50),
    churn_risk VARCHAR(20),
    analysis_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_recency (recency_days),
    INDEX idx_frequency (frequency),
    INDEX idx_monetary (monetary),
    INDEX idx_segment (segment),
    INDEX idx_churn_risk (churn_risk),
    INDEX idx_analysis_date (analysis_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =====================================================
-- Product Category Mapping Table (Optional)
-- =====================================================
CREATE TABLE IF NOT EXISTS product_category_mapping (
    category_id INT AUTO_INCREMENT PRIMARY KEY,
    stock_code VARCHAR(50),
    description_keyword VARCHAR(100),
    category_name VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_stock_code (stock_code),
    INDEX idx_keyword (description_keyword)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

