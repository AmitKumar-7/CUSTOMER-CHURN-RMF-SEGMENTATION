-- =====================================================
-- Master Setup Script
-- Run this script to set up the entire database
-- =====================================================

-- This script runs all setup scripts in the correct order

SOURCE 01_database_schema.sql;
SOURCE 02_etl_data_preparation.sql;
SOURCE 03_rfm_calculation.sql;
SOURCE 04_rfm_segmentation.sql;
SOURCE 05_stored_procedures.sql;
SOURCE 06_reporting_views.sql;
SOURCE 07_optimization_indexes.sql;

-- Note: Run 08_data_import.sql separately after importing your CSV data
-- Or use the Python script: python data_import.py

SELECT 'Database setup completed successfully!' AS status;

