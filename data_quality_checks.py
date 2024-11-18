"""
This script performs data quality checks on a SQLite database using a data dictionary.
The script connects to a SQLite database and performs the following operations:
1. Fetches table and column metadata
2. Reads constraints from a data dictionary JSON file
3. Performs various data quality checks including:
    - Null value validation
    - Zero value validation
    - Duplicate value checks
    - Allowable value range checks
    - Min/max value boundary checks
Functions:
     fetch_tables():
          Retrieves all table names from the database.
          Returns:
                pandas.DataFrame: DataFrame containing table names and owners.
     fetch_columns():
          Retrieves column information for all tables.
          Returns:
                pandas.DataFrame: DataFrame containing column metadata including:
                     - table_name
                     - column_name
                     - data_type
                     - is_nullable
Required files:
     - sample_database.db: SQLite database file
     - data_dictionary.json: JSON file containing data quality constraints
Dependencies:
     - pandas
     - sqlite3
     - json
Author: Not specified
Version: Not specified
"""
import sqlite3
import pandas as pd
import json
import logging
from datetime import datetime

# Setup logging
log_filename = f'data_quality_check_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
logging.basicConfig(
    filename=log_filename,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Connect to SQLite database
db_name = 'sample_database.db'
conn = sqlite3.connect(db_name)


# Function to fetch tables
def fetch_tables():
    query = """
    SELECT
        name AS table_name
    FROM sqlite_master
    WHERE type = 'table' AND name NOT LIKE 'sqlite_%';
    """
    tables_df = pd.read_sql_query(query, conn)
    # Add a placeholder for table_owner
    tables_df['table_owner'] = 'N/A'
    return tables_df


# Function to fetch columns
def fetch_columns():
    # Retrieve all table names
    tables_df = fetch_tables()
    columns_list = []

    for table_name in tables_df['table_name']:
        # PRAGMA table_info returns information about each column in the table
        pragma_query = f"PRAGMA table_info('{table_name}');"
        pragma_df = pd.read_sql_query(pragma_query, conn)
        for index, row in pragma_df.iterrows():
            column_info = {
                'table_name': table_name,
                'column_name': row['name'],
                'data_type': row['type'],
                'is_nullable': 'NO' if row['notnull'] else 'YES',
            }
            columns_list.append(column_info)
    columns_df = pd.DataFrame(columns_list)
    return columns_df

# Fetch tables and columns with constraints
tables_df = fetch_tables()
columns_df = fetch_columns()

# Process each table
for table in tables_df['table_name']:
    logging.info(f"\nStarting checks for table: {table}")
    print(f"Checking table: {table}")
    
    # Get columns and constraints for the table
    table_columns = columns_df[columns_df['table_name'] == table]
    
    # Fetch aggregated data from the database
    data_aggregates = {}
    
    # Load constraints from data dictionary
    with open('data_dictionary.json', 'r') as f:
        data_dictionary = json.load(f)
    
    for _, row in table_columns.iterrows():
        column = row['column_name']
        data_type = row['data_type']
        nullable = row['is_nullable']
        
        # Prepare aggregation queries
        query_parts = [
        f"SUM(CASE WHEN {column} IS NULL THEN 1 ELSE 0 END) AS null_count",
        f"COUNT(DISTINCT {column}) AS distinct_count",
        f"COUNT(*) AS total_count"
        ]
        
        # Min and Max values for numeric columns
        if data_type.upper() in ['INTEGER', 'REAL', 'NUMERIC', 'DECIMAL', 'FLOAT', 'DOUBLE','DATE', 'DATETIME', 'TIMESTAMP']:
            query_parts.append(f"MIN({column}) AS min_value")
            query_parts.append(f"MAX({column}) AS max_value")
        
        # Construct the query
        query = f"SELECT {', '.join(query_parts)} FROM {table};"
        agg_df = pd.read_sql_query(query, conn)
        
        # Store aggregated results
        data_aggregates[column] = agg_df.iloc[0].to_dict()
    
    # Validate data using aggregated results
    for _, row in table_columns.iterrows():
        column = row['column_name']
        column_agg = data_aggregates[column]
        table_constraints = data_dictionary.get(table, {}).get('columns', {}).get(column, {})
        
        # Check for null values if not allowed
        if not table_constraints.get('null_values_allowed', True):
            if column_agg['null_count'] > 0:
                error_msg = f"Column '{column}' has {column_agg['null_count']} null values"
                print(f"- {error_msg}")
                logging.error(error_msg)
                # Get sample of records with null values
                query = f"SELECT * FROM {table} WHERE {column} IS NULL LIMIT 5;"
                sample_records = pd.read_sql_query(query, conn)
                logging.error(f"Sample records with null values:\n{sample_records.to_string()}")

        # Check for zero values if not allowed
        if not table_constraints.get('zeros_allowed', True):
            if data_type.upper() in ['INTEGER', 'REAL', 'NUMERIC', 'DECIMAL', 'FLOAT', 'DOUBLE']:
                query = f"SELECT COUNT(*) as zero_count FROM {table} WHERE {column} = 0;"
                zero_count = pd.read_sql_query(query, conn).iloc[0]['zero_count']
                if zero_count > 0:
                    error_msg = f"Column '{column}' has {zero_count} zero values"
                    print(f"- {error_msg}")
                    logging.error(error_msg)
                    # Get sample of records with zero values
                    query = f"SELECT * FROM {table} WHERE {column} = 0 LIMIT 5;"
                    sample_records = pd.read_sql_query(query, conn)
                    logging.error(f"Sample records with zero values:\n{sample_records.to_string()}")

        # Check for duplicates if not allowed
        if not table_constraints.get('duplicates_allowed', True):
            if column_agg['distinct_count'] < column_agg['total_count']:
                error_msg = f"Column '{column}' has {column_agg['total_count'] - column_agg['distinct_count']} duplicate values"
                print(f"- {error_msg}")
                logging.error(error_msg)
                # Get sample of duplicate records
                query = f"""
                    SELECT *, COUNT(*) as count 
                    FROM {table} 
                    GROUP BY {column} 
                    HAVING COUNT(*) > 1 
                    LIMIT 5;
                """
                sample_records = pd.read_sql_query(query, conn)
                logging.error(f"Sample duplicate records:\n{sample_records.to_string()}")

        # Check for allowable values
        if 'allowable_values' in table_constraints:
            allowable_values = table_constraints['allowable_values']
            allowable_values_list = ', '.join([f"'{v}'" if isinstance(v, str) else str(v) for v in allowable_values])
            query = f"""
                SELECT *, COUNT(*) as invalid_count 
                FROM {table} 
                WHERE {column} NOT IN ({allowable_values_list})
                GROUP BY {column}
                LIMIT 5;
            """
            invalid_records = pd.read_sql_query(query, conn)
            if len(invalid_records) > 0:
                error_msg = f"Column '{column}' has values outside allowable range: {allowable_values}"
                print(f"- {error_msg}")
                logging.error(error_msg)
                logging.error(f"Sample invalid records:\n{invalid_records.to_string()}")

        # Check for minimum value
        if 'min_value' in table_constraints and 'min_value' in column_agg:
            if column_agg['min_value'] < table_constraints['min_value']:
                error_msg = f"Column '{column}' has values below minimum value of {table_constraints['min_value']}"
                print(f"- {error_msg}")
                logging.error(error_msg)
                query = f"SELECT * FROM {table} WHERE {column} < {table_constraints['min_value']} LIMIT 5;"
                sample_records = pd.read_sql_query(query, conn)
                logging.error(f"Sample records below minimum value:\n{sample_records.to_string()}")

        # Check for maximum value
        if 'max_value' in table_constraints and 'max_value' in column_agg:
            if column_agg['max_value'] > table_constraints['max_value']:
                error_msg = f"Column '{column}' has values above maximum value of {table_constraints['max_value']}"
                print(f"- {error_msg}")
                logging.error(error_msg)
                query = f"SELECT * FROM {table} WHERE {column} > {table_constraints['max_value']} LIMIT 5;"
                sample_records = pd.read_sql_query(query, conn)
                logging.error(f"Sample records above maximum value:\n{sample_records.to_string()}")
        
        # Log successful checks
        if not logging.getLogger().hasHandlers():
            logging.info(f"All checks passed for column '{column}' in table '{table}'")

conn.close()
logging.info("Data quality check completed")

