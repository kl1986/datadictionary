import sqlite3
import pandas as pd
import json

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
        if not table_constraints.get('null_values_allowed', True) and column_agg['null_count'] > 0:
            print(f"- Column '{column}' has null values.")

        # Check for zero values if not allowed
        if not table_constraints.get('zeros_allowed', True):
            if data_type.upper() in ['INTEGER', 'REAL', 'NUMERIC', 'DECIMAL', 'FLOAT', 'DOUBLE']:
                if column_agg.get('zero_count', 0) > 0:
                    print(f"- Column '{column}' has zero values.")
        
        # Check for duplicates if not allowed
        if not table_constraints.get('duplicates_allowed', True):
            if column_agg['distinct_count'] < column_agg['total_count']:
                print(f"- Column '{column}' has duplicate values.")
        
        # Check for allowable values
        if 'allowable_values' in table_constraints:
            allowable_values = table_constraints['allowable_values']
            allowable_values_list = ', '.join([f"'{v}'" if isinstance(v, str) else str(v) for v in allowable_values])
            query = f"SELECT COUNT(*) AS invalid_count FROM {table} WHERE {column} NOT IN ({allowable_values_list});"
            invalid_df = pd.read_sql_query(query, conn)
            if invalid_df.iloc[0]['invalid_count'] > 0:
                print(f"- Column '{column}' has values outside allowable range.")
        
        # Check for minimum value
        if 'min_value' in table_constraints and 'min_value' in column_agg:
            if column_agg['min_value'] < table_constraints['min_value']:
                print(f"- Column '{column}' has values below minimum value.")
        
        # Check for maximum value
        if 'max_value' in table_constraints and 'max_value' in column_agg:
            if column_agg['max_value'] > table_constraints['max_value']:
                print(f"- Column '{column}' has values above maximum value.")

# Close the connection
conn.close()