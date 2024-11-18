import sqlite3
import pandas as pd
import json
#import env


# Connect to SQLite database, you will need to change this depending on what database you are connecting to
def get_conn():
    db_name = 'sample_database.db'
    conn = sqlite3.connect(db_name)
    return conn


# Function to fetch tables
def fetch_tables(conn):
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
def fetch_columns(conn):
    # Retrieve all table names
    tables_df = fetch_tables(conn)
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

print(fetch_tables(get_conn()))