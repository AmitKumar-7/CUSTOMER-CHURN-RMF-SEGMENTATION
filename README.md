# Customer Churn Prediction and RFM Segmentation

This project simulates a business intelligence scenario, focusing on customer behavior analysis and data transformation for reporting using MySQL.

## 🎯 Objective

Analyze historical e-commerce transaction data to segment customers using the RFM model and identify potential churn risks.

## 📋 Project Structure

```
Customer Churn Prediction and RFM Segmentation/
│
├── 01_database_schema.sql          # Database and table creation
├── 02_etl_data_preparation.sql     # Data cleaning and transformation
├── 03_rfm_calculation.sql          # RFM metrics calculation
├── 04_rfm_segmentation.sql         # Customer segmentation using NTILE()
├── 05_stored_procedures.sql        # Automated RFM procedures
├── 06_reporting_views.sql          # Views for BI tools (Tableau/Power BI)
├── 07_optimization_indexes.sql     # Performance optimization
├── 08_data_import.sql              # CSV data import script
├── online_retail_II.csv            # Source transaction data
└── README.md                        # This file
```

## 🛠️ Key Concepts & Features

### 1. Data Preparation (ETL Simulation)
- Clean raw transaction data
- Handle null values and inconsistent entries
- Use CASE statements for data categorization
- Product category mapping

### 2. RFM Calculation
- **Recency**: Days since last purchase using `DATEDIFF()`
- **Frequency**: Total distinct orders using `COUNT(DISTINCT OrderID)`
- **Monetary**: Total amount spent using `SUM(OrderValue)`

### 3. Segmentation using NTILE() and RANK()
- Use `NTILE(4)` to divide customers into quartiles for R, F, and M scores
- Combine scores to assign segments:
  - **Champions** (444, 443, etc.): High R, F, M
  - **Loyal Customers**: High F, M, Medium R
  - **At Risk** (144, etc.): Low R, Medium F, High M
  - **Lost**: Low R, F, M
  - And more...

### 4. Stored Procedures
- `sp_calculate_rfm_segmentation()`: Calculate RFM with date range
- `sp_refresh_customer_summary()`: Update customer metrics
- `sp_complete_rfm_pipeline()`: Complete ETL and RFM process
- `sp_get_customers_by_segment()`: Query customers by segment

### 5. Reporting Views
- `vw_customer_rfm_profile`: Complete customer profiles
- `vw_segment_summary`: Segment statistics
- `vw_churn_risk_analysis`: Churn risk breakdown
- `vw_high_value_customers`: Top customers
- `vw_customers_need_attention`: At-risk customers
- And more for BI tools integration

### 6. Optimization
- Comprehensive indexing strategy
- Composite indexes for common queries
- Table analysis for query optimization

## 🚀 Setup Instructions

### Prerequisites
- MySQL 5.7+ or MySQL 8.0+
- MySQL Workbench or command-line client
- CSV data file: `online_retail_II.csv`

### Step 1: Create Database and Tables
```sql
-- Run the schema script
SOURCE 01_database_schema.sql;
-- Or in MySQL Workbench: File > Run SQL Script
```

### Step 2: Import Data
```sql
-- Option A: Using LOAD DATA INFILE (recommended)
SOURCE 08_data_import.sql;

-- Option B: If LOAD DATA INFILE doesn't work, use a Python script
-- or import via MySQL Workbench's Table Data Import Wizard
```

**Note**: For Windows, you may need to:
1. Enable local file loading: `SET GLOBAL local_infile = 1;`
2. Use forward slashes in file paths or double backslashes
3. Ensure the CSV file path is correct

### Step 3: Run ETL Process
```sql
SOURCE 02_etl_data_preparation.sql;
```

### Step 4: Create RFM Calculations and Segmentation
```sql
SOURCE 03_rfm_calculation.sql;
SOURCE 04_rfm_segmentation.sql;
```

### Step 5: Create Stored Procedures
```sql
SOURCE 05_stored_procedures.sql;
```

### Step 6: Create Reporting Views
```sql
SOURCE 06_reporting_views.sql;
```

### Step 7: Add Optimization Indexes
```sql
SOURCE 07_optimization_indexes.sql;
```

## 📊 Usage Examples

### Run Complete RFM Pipeline
```sql
-- Calculate RFM for all customers with current date as cutoff
CALL sp_complete_rfm_pipeline(NULL, NULL, NULL);

-- Calculate RFM for specific date range
CALL sp_complete_rfm_pipeline('2020-12-31', '2019-01-01', '2020-12-31');
```

### Query Customers by Segment
```sql
-- Get all "At Risk" customers
CALL sp_get_customers_by_segment('At Risk', NULL, 100);

-- Get high-risk customers
CALL sp_get_customers_by_segment(NULL, 'High Risk', 50);
```

### Use Reporting Views
```sql
-- View segment distribution
SELECT * FROM vw_segment_summary;

-- View churn risk analysis
SELECT * FROM vw_churn_risk_analysis;

-- View high-value customers
SELECT * FROM vw_high_value_customers LIMIT 100;

-- View customers needing attention
SELECT * FROM vw_customers_need_attention;
```

### Connect to BI Tools
The views in `06_reporting_views.sql` are designed for direct connection to:
- **Tableau**: Use MySQL connector
- **Power BI**: Use MySQL database connector
- **Excel**: Use MySQL ODBC driver

## 📈 RFM Segmentation Logic

### Score Calculation
- **Recency Score**: `NTILE(4)` ordered by recency_days ASC (lower days = higher score)
- **Frequency Score**: `NTILE(4)` ordered by frequency DESC (higher frequency = higher score)
- **Monetary Score**: `NTILE(4)` ordered by monetary DESC (higher spending = higher score)

### Segment Mapping
| RFM Pattern | Segment | Description |
|------------|---------|-------------|
| 444, 443, 434, 433 | Champions | Best customers |
| 244, 243, 234, 233 | Loyal Customers | Regular buyers |
| 144, 143, 134, 133 | At Risk | High value but inactive |
| 111, 112, 121, 122 | Lost | Churned customers |

## 🔍 Data Quality

Check data quality after import:
```sql
SELECT * FROM vw_data_quality_report;
```

## ⚡ Performance Tips

1. **Index Usage**: All tables have optimized indexes
2. **Query Analysis**: Use `EXPLAIN` before running complex queries
3. **Table Maintenance**: Run `ANALYZE TABLE` periodically
4. **Date Filtering**: Always filter by `analysis_date` when querying RFM scores

## 📝 Notes

- The project uses MySQL-specific features (NTILE, window functions)
- For MySQL 5.6 or earlier, you may need to modify window function usage
- CSV import may require adjusting file paths based on your system
- Consider partitioning large tables by date for better performance

## 🔧 Troubleshooting

### Issue: LOAD DATA INFILE fails
**Solution**: 
- Enable local_infile: `SET GLOBAL local_infile = 1;`
- Check file path and permissions
- Use MySQL Workbench import wizard as alternative

### Issue: Window functions not supported
**Solution**: Upgrade to MySQL 8.0+ or modify queries to use subqueries

### Issue: Stored procedure errors
**Solution**: 
- Ensure all tables exist before running procedures
- Check delimiter settings (should be `//` for procedures)

## 📚 Additional Resources

- [MySQL Window Functions](https://dev.mysql.com/doc/refman/8.0/en/window-functions.html)
- [RFM Analysis Guide](https://en.wikipedia.org/wiki/RFM_(market_research))
- [MySQL Optimization Guide](https://dev.mysql.com/doc/refman/8.0/en/optimization.html)
