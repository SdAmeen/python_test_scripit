import pandas as pd
import sqlite3
import os

class SalesDataProcessor:
    def __init__(self, region_a_path, region_b_path, db_path='sales_data.db'):
        """
        Initialize the Sales Data Processor
        
        :param region_a_path: Path to Region A CSV file
        :param region_b_path: Path to Region B CSV file
        :param db_path: Path to SQLite database
        """
        self.region_a_path = region_a_path
        self.region_b_path = region_b_path
        self.db_path = db_path
        self.conn = None
        self.cursor = None

    def clean_numeric_columns(self, df):
        """
        Clean and convert numeric columns
        
        :param df: Input DataFrame
        :return: Cleaned DataFrame
        """
        # Columns to convert to numeric
        numeric_columns = ['QuantityOrdered', 'ItemPrice', 'PromotionDiscount']
        
        for col in numeric_columns:
            # Replace non-numeric values with 0
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        return df

    def extract_data(self):
        """
        Extract data from CSV files for both regions
        
        :return: Combined DataFrame with region information
        """
        try:
            # Read CSV files with error handling for encoding
            df_region_a = pd.read_csv(self.region_a_path, encoding='utf-8', low_memory=False)
            df_region_b = pd.read_csv(self.region_b_path, encoding='utf-8', low_memory=False)
            
            # Clean numeric columns
            df_region_a = self.clean_numeric_columns(df_region_a)
            df_region_b = self.clean_numeric_columns(df_region_b)
            
            # Add region column
            df_region_a['region'] = 'A'
            df_region_b['region'] = 'B'
            
            # Combine DataFrames
            combined_df = pd.concat([df_region_a, df_region_b], ignore_index=True)
            
            # Ensure all columns exist
            required_columns = ['OrderId', 'OrderItemId', 'QuantityOrdered', 'ItemPrice', 'PromotionDiscount']
            for col in required_columns:
                if col not in combined_df.columns:
                    raise ValueError(f"Missing required column: {col}")
            
            return combined_df
        except Exception as e:
            print(f"Error extracting data: {e}")
            print(f"Columns in Region A: {df_region_a.columns}")
            print(f"Columns in Region B: {df_region_b.columns}")
            return None

    def transform_data(self, df):
        """
        Transform data according to business rules
        
        :param df: Input DataFrame
        :return: Transformed DataFrame
        """
        if df is None:
            print("No data to transform")
            return None
        
        try:
            # Calculate total sales
            df['total_sales'] = df['QuantityOrdered'] * df['ItemPrice']
            
            # Calculate net sales
            df['net_sale'] = df['total_sales'] - df['PromotionDiscount']
            
            # Remove duplicates based on OrderId
            df.drop_duplicates(subset=['OrderId'], keep='first', inplace=True)
            
            # Filter out orders with non-positive net sales
            df = df[df['net_sale'] > 0]
            
            # Reset index after transformations
            df.reset_index(drop=True, inplace=True)
            
            return df
        except Exception as e:
            print(f"Error transforming data: {e}")
            # Print column types to help diagnose issues
            print(df.dtypes)
            return None

    def create_database(self):
        """
        Create SQLite database and sales_data table
        """
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.cursor = self.conn.cursor()
            
            # Create sales_data table
            self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS sales_data (
                OrderId TEXT PRIMARY KEY,
                OrderItemId TEXT,
                QuantityOrdered REAL,
                ItemPrice REAL,
                PromotionDiscount REAL,
                region TEXT,
                total_sales REAL,
                net_sale REAL
            )
            ''')
            
            self.conn.commit()
        except Exception as e:
            print(f"Error creating database: {e}")

    def load_data(self, df):
        """
        Load transformed data into SQLite database
        
        :param df: Transformed DataFrame
        """
        if df is None or df.empty:
            print("No data to load")
            return
        
        try:
            # Ensure database connection is established
            if not self.conn:
                self.create_database()
            
            # Prepare the DataFrame for SQL insertion
            df_to_load = df.where(pd.notnull(df), None)
            
            # Load data into sales_data table
            df_to_load.to_sql('sales_data', self.conn, if_exists='replace', index=False)
            self.conn.commit()
            print(f"Loaded {len(df)} records into the database")
        except Exception as e:
            print(f"Error loading data: {e}")

    def validate_data(self):
        """
        Validate data through SQL queries
        """
        try:
            # Total number of records
            self.cursor.execute("SELECT COUNT(*) FROM sales_data")
            total_records = self.cursor.fetchone()[0]
            print(f"Total Records: {total_records}")

            # Total sales by region
            self.cursor.execute("SELECT region, SUM(total_sales) as total_region_sales FROM sales_data GROUP BY region")
            region_sales = self.cursor.fetchall()
            print("Total Sales by Region:")
            for region, sales in region_sales:
                print(f"Region {region}: {sales}")

            # Average sales per transaction
            self.cursor.execute("SELECT AVG(total_sales) as avg_sales FROM sales_data")
            avg_sales = self.cursor.fetchone()[0]
            print(f"Average Sales per Transaction: {avg_sales}")

            # Check for duplicate OrderIds
            self.cursor.execute("SELECT OrderId, COUNT(*) FROM sales_data GROUP BY OrderId HAVING COUNT(*) > 1")
            duplicates = self.cursor.fetchall()
            print("Duplicate OrderIds:", duplicates if duplicates else "None")

        except Exception as e:
            print(f"Error validating data: {e}")

    def close_connection(self):
        """
        Close database connection
        """
        if self.conn:
            self.conn.close()

def main():
    # Paths to CSV files (replace with actual paths)
    region_a_path = 'order_region_a.csv'
    region_b_path = 'order_region_b.csv'

    # Initialize processor
    processor = SalesDataProcessor(region_a_path, region_b_path)

    try:
        # Extract data
        raw_data = processor.extract_data()
        
        if raw_data is not None:
            # Transform data
            transformed_data = processor.transform_data(raw_data)
            
            # Create database and load data
            processor.create_database()
            processor.load_data(transformed_data)
            
            # Validate data
            processor.validate_data()
        else:
            print("Failed to extract data. Please check the input files.")
    
    except Exception as e:
        print(f"Processing error: {e}")
    
    finally:
        # Ensure database connection is closed
        processor.close_connection()

if __name__ == "__main__":
    main()