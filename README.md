
# Sales Data ETL Project

## Overview
This project provides a Python-based ETL (Extract, Transform, Load) solution for processing sales data from two different regions, transforming it according to specific business rules, and loading it into a SQLite database.

## Prerequisites
- Python 3.8+
- Required Python Libraries:
  - pandas
  - sqlite3

## Installation
1. Clone the repository
2. Install required libraries:
   ```
   pip install pandas
   ```

## Configuration
1. Place your CSV files:
   - `order_region_a.csv` for Region A data
   - `order_region_b.csv` for Region B data
   - Ensure files are in the same directory as the script

## Running the Script
```
python sales_data_etl.py
```

## Business Rules Implemented
1. Combined data from both regions into a single table
2. Added `total_sales` column (QuantityOrdered * ItemPrice)
3. Added `region` column to identify sales record origin
4. Removed duplicate entries based on OrderId
5. Added `net_sale` column (total_sales - PromotionDiscount)
6. Excluded orders with non-positive net sales after discounts

## Data Validation
The script performs the following validations:
- Counts total records
- Calculates total sales by region
- Computes average sales per transaction
- Checks for duplicate OrderIds

## Assumptions
1. CSV files have consistent column names
2. All monetary values are in INR
3. No additional data cleaning beyond specified rules is required

## Output
- Creates `sales_data.db` SQLite database
- Prints validation results to console

## Potential Improvements
- Add logging
- Implement more robust error handling
- Create command-line arguments for file paths

## Troubleshooting
- Ensure all prerequisites are installed
- Verify CSV file paths and permissions
- Check Python version compatibility
