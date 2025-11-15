# Quick Reference Guide

## 🚀 Quick Start

### 1. Initial Setup (One-time)
```sql
-- Run master setup script
SOURCE 00_master_setup.sql;

-- Import data (choose one method)
-- Method A: SQL import
SOURCE 08_data_import.sql;

-- Method B: Python import
python data_import.py
```

### 2. Run Complete Pipeline
```sql
-- Refresh customer data and calculate RFM
CALL sp_complete_rfm_pipeline(NULL, NULL, NULL);
```

## 📊 Common Queries

### View All Segments
```sql
SELECT * FROM vw_segment_summary;
```

### Find At-Risk Customers
```sql
SELECT * FROM vw_customers_need_attention LIMIT 100;
```

### Get High-Value Customers
```sql
SELECT * FROM vw_high_value_customers LIMIT 50;
```

### Churn Risk Analysis
```sql
SELECT * FROM vw_churn_risk_analysis;
```

### Customers by Segment
```sql
-- Get all "Champions"
CALL sp_get_customers_by_segment('Champions', NULL, 100);

-- Get high-risk customers
CALL sp_get_customers_by_segment(NULL, 'High Risk', 50);
```

## 🔧 Stored Procedures

| Procedure | Description | Usage |
|-----------|-------------|-------|
| `sp_calculate_rfm_segmentation()` | Calculate RFM with date range | `CALL sp_calculate_rfm_segmentation('2020-12-31', '2019-01-01', '2020-12-31');` |
| `sp_refresh_customer_summary()` | Update customer metrics | `CALL sp_refresh_customer_summary();` |
| `sp_complete_rfm_pipeline()` | Complete ETL + RFM | `CALL sp_complete_rfm_pipeline(NULL, NULL, NULL);` |
| `sp_get_customers_by_segment()` | Query by segment/risk | `CALL sp_get_customers_by_segment('At Risk', NULL, 100);` |

## 📈 Reporting Views

| View | Purpose |
|------|---------|
| `vw_customer_rfm_profile` | Complete customer profiles |
| `vw_segment_summary` | Segment statistics |
| `vw_churn_risk_analysis` | Churn risk breakdown |
| `vw_high_value_customers` | Top customers by value |
| `vw_customers_need_attention` | At-risk customers list |
| `vw_customer_lifetime_value` | CLV by segment |
| `vw_monthly_customer_activity` | Monthly trends |
| `vw_segment_product_performance` | Product performance by segment |
| `vw_geographic_segment_distribution` | Geographic analysis |
| `vw_rfm_score_distribution` | Score distribution |

## 🎯 RFM Segments

| Segment | RFM Pattern | Action |
|---------|-------------|--------|
| Champions | 444, 443, 434, 433 | Reward, VIP programs |
| Loyal Customers | 244, 243, 234, 233 | Upsell, cross-sell |
| At Risk | 144, 143, 134, 133 | Win-back campaigns |
| Lost | 111, 112, 121, 122 | Re-engagement campaigns |

## 🔍 Data Quality Checks

```sql
-- Check data quality
SELECT * FROM vw_data_quality_report;

-- Check table sizes
SELECT * FROM vw_table_sizes;

-- Check index usage
SELECT * FROM vw_index_usage;
```

## 📝 Maintenance

```sql
-- Refresh customer summary
CALL sp_refresh_customer_summary();

-- Recalculate RFM
CALL sp_calculate_rfm_segmentation(CURDATE(), NULL, NULL);

-- Analyze tables for optimization
ANALYZE TABLE customer_summary;
ANALYZE TABLE rfm_scores;
```

## 🔗 BI Tool Connection

### Tableau
1. Connect to MySQL database
2. Use views from `06_reporting_views.sql`
3. Recommended views: `vw_segment_summary`, `vw_churn_risk_analysis`

### Power BI
1. Get Data > MySQL database
2. Select views for analysis
3. Use `vw_customer_rfm_profile` for detailed analysis

## ⚠️ Troubleshooting

### Procedure not found
```sql
-- Recreate procedures
SOURCE 05_stored_procedures.sql;
```

### View errors
```sql
-- Recreate views
SOURCE 06_reporting_views.sql;
```

### Performance issues
```sql
-- Rebuild indexes
SOURCE 07_optimization_indexes.sql;

-- Analyze tables
ANALYZE TABLE cleaned_transactions;
ANALYZE TABLE customer_summary;
ANALYZE TABLE rfm_scores;
```

