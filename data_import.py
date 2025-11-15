"""
Python script to import CSV data into MySQL database
Alternative to LOAD DATA INFILE for better control and error handling
"""

import mysql.connector
import csv
import sys
from datetime import datetime

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',  # Change as needed
    'password': '',  # Change as needed
    'database': 'customer_analytics',
    'charset': 'utf8mb4',
    'allow_local_infile': True
}

CSV_FILE = 'online_retail_II.csv'
BATCH_SIZE = 1000  # Insert in batches for better performance


def parse_date(date_str):
    """Parse date string from CSV"""
    try:
        # Try different date formats
        formats = [
            '%Y-%m-%d %H:%M:%S',
            '%m/%d/%Y %H:%M',
            '%d/%m/%Y %H:%M',
        ]
        for fmt in formats:
            try:
                return datetime.strptime(date_str.strip(), fmt)
            except ValueError:
                continue
        return None
    except:
        return None


def clean_value(value):
    """Clean and validate values"""
    if value is None:
        return None
    value = str(value).strip()
    if value == '' or value.lower() == 'nan':
        return None
    return value


def import_data():
    """Import CSV data into MySQL"""
    try:
        # Connect to database
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        print(f"Connected to database: {DB_CONFIG['database']}")
        
        # Read CSV file
        print(f"Reading CSV file: {CSV_FILE}")
        
        with open(CSV_FILE, 'r', encoding='utf-8', errors='ignore') as file:
            reader = csv.DictReader(file)
            
            insert_query = """
            INSERT INTO raw_transactions 
            (invoice, stock_code, description, quantity, invoice_date, price, customer_id, country)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            batch = []
            row_count = 0
            error_count = 0
            
            for row in reader:
                try:
                    # Parse and clean data
                    invoice = clean_value(row.get('Invoice', ''))
                    stock_code = clean_value(row.get('StockCode', ''))
                    description = clean_value(row.get('Description', ''))
                    
                    # Handle quantity
                    try:
                        quantity = int(float(row.get('Quantity', 0) or 0))
                    except:
                        quantity = 1
                    
                    # Parse date
                    invoice_date = parse_date(row.get('InvoiceDate', ''))
                    if invoice_date is None:
                        invoice_date = datetime.now()
                    
                    # Handle price
                    try:
                        price = float(row.get('Price', 0) or 0)
                        if price <= 0:
                            price = 0.01
                    except:
                        price = 0.01
                    
                    # Handle customer ID
                    customer_id = clean_value(row.get('Customer ID', ''))
                    if customer_id:
                        try:
                            customer_id = str(int(float(customer_id)))
                        except:
                            customer_id = None
                    else:
                        customer_id = None
                    
                    country = clean_value(row.get('Country', ''))
                    
                    # Add to batch
                    batch.append((
                        invoice,
                        stock_code,
                        description,
                        quantity,
                        invoice_date,
                        price,
                        customer_id,
                        country
                    ))
                    
                    # Insert batch when full
                    if len(batch) >= BATCH_SIZE:
                        cursor.executemany(insert_query, batch)
                        conn.commit()
                        row_count += len(batch)
                        print(f"Imported {row_count} rows...")
                        batch = []
                
                except Exception as e:
                    error_count += 1
                    if error_count <= 10:  # Print first 10 errors
                        print(f"Error processing row: {e}")
                    continue
            
            # Insert remaining batch
            if batch:
                cursor.executemany(insert_query, batch)
                conn.commit()
                row_count += len(batch)
            
            print(f"\nImport completed!")
            print(f"Total rows imported: {row_count}")
            print(f"Errors encountered: {error_count}")
            
            # Verify import
            cursor.execute("SELECT COUNT(*) FROM raw_transactions")
            total = cursor.fetchone()[0]
            print(f"Total records in database: {total}")
        
        cursor.close()
        conn.close()
        
    except mysql.connector.Error as err:
        print(f"Database error: {err}")
        sys.exit(1)
    except FileNotFoundError:
        print(f"Error: CSV file '{CSV_FILE}' not found!")
        print("Please ensure the file is in the same directory as this script.")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    print("=" * 60)
    print("Customer Analytics - CSV Data Import Script")
    print("=" * 60)
    print()
    
    # Check if CSV file exists
    import os
    if not os.path.exists(CSV_FILE):
        print(f"Error: CSV file '{CSV_FILE}' not found!")
        print("Please ensure the file is in the current directory.")
        sys.exit(1)
    
    # Confirm before proceeding
    response = input(f"Import data from '{CSV_FILE}'? (y/n): ")
    if response.lower() != 'y':
        print("Import cancelled.")
        sys.exit(0)
    
    import_data()
    
    print("\nNext steps:")
    print("1. Run the ETL script: SOURCE 02_etl_data_preparation.sql;")
    print("2. Calculate RFM: CALL sp_complete_rfm_pipeline(NULL, NULL, NULL);")

