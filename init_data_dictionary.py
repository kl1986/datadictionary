# %%
"""
SQLite Database Data Dictionary Manager

This module provides functionality to create, manage, and update a data dictionary for SQLite databases.
It includes features for tracking table and column metadata, schema changes, and statistical information.

Main Functions:
--------------
fetch_tables()
    Retrieves all table names from the SQLite database, excluding system tables.
    Returns: DataFrame with table names and placeholder owners.

fetch_columns()
    Retrieves column information for all tables in the database.
    Returns: DataFrame with column details including name, type, and nullable status.

get_column_stats(table_name: str, column_name: str)
    Calculates basic statistics for a specific column.
    Returns: Tuple of (min_value, max_value, unique_count).

save_data_dictionary(dd: dict, json_file: str='data_dictionary.json')
    Saves the data dictionary to a JSON file with custom datetime handling.

load_data_dictionary(file_path: str='data_dictionary.json')
    Loads an existing data dictionary from a JSON file.
    Returns: Dictionary containing the data dictionary structure.

build_data_dictionary_from_schema(tables_df: DataFrame, columns_df: DataFrame)
    Creates a new data dictionary structure from database schema information.
    Returns: Dictionary with table and column metadata.

update_column_metadata(dd: dict, cols: dict)
    Updates metadata for specified columns, including statistics and value ranges.

get_all_tables_and_columns(dd: dict)
    Extracts all table and column names from the data dictionary.
    Returns: Dictionary mapping table names to lists of column names.

Helper Classes:
--------------
CustomJSONEncoder
    Custom JSON encoder for handling datetime objects in the data dictionary.

Dependencies:
------------
- sqlite3
- pandas
- json
- datetime
- tqdm

Note:
-----
The module requires an active SQLite database connection and appropriate read permissions.
It maintains a JSON-based data dictionary that tracks schema changes and column metadata.
"""


# Import Libraries
import sqlite3
import pandas as pd
import json
import datetime
from tqdm import tqdm

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


# Function to get column stats
def get_column_stats(table_name, column_name):
    query = f"""
    SELECT
        MIN({column_name}) AS min_val,
        MAX({column_name}) AS max_val,
        COUNT(DISTINCT {column_name}) AS unique_count
    FROM {table_name}
    """
    result = conn.execute(query).fetchone()
    min_val = result[0]
    max_val = result[1]
    unique_count = result[2]
    return min_val, max_val, unique_count


# Custom JSON Encoder to handle datetime objects
class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (pd.Timestamp, datetime.datetime, datetime.date)):
            return obj.isoformat()
        return super().default(obj)


# Function to save data dictionary
def save_data_dictionary(dd, json_file='data_dictionary.json'):
    with open(json_file, 'w') as f:
        json.dump(dd, f, cls=CustomJSONEncoder, indent=4)
    print("Data dictionary saved successfully.")


# Function to load existing data dictionary
def load_data_dictionary(json_file='data_dictionary.json'):
    try:
        with open(json_file, 'r') as f:
            dd = json.load(f)
        print("Data dictionary loaded successfully.")
    except FileNotFoundError:
        dd = {}
        print("No existing data dictionary found. Starting fresh.")
    return dd

# Function to load data dictionary
def load_data_dictionary(file_path='data_dictionary.json'):
    with open(file_path, 'r') as file:
        return json.load(file)

# Function to build data dictionary from schema
def build_data_dictionary_from_schema(tables_df, columns_df):
    dd = {}
    # Initialize tables
    for index, row in tables_df.iterrows():
        table_name = row['table_name']
        table_owner = row['table_owner']
        dd[table_name] = {
            'description': '',
            'table_owner': table_owner,
            'columns': {}
        }
    # Populate columns
    for index, row in columns_df.iterrows():
        table_name = row['table_name']
        column_name = row['column_name']
        data_type = row['data_type']
        is_nullable = row['is_nullable']
        column_metadata = {
            'description': '',
            'data_type': data_type,
            'type': '',
            'duplicates_allowed': True,
            'null_values_allowed': is_nullable == 'YES',
        }
        dd[table_name]['columns'][column_name] = column_metadata
    return dd


# Function to update column metadata
def update_column_metadata(dd, cols):
    for table, column in tqdm(cols.items(), desc='Updating Metadata for Columns'):
        table_data = dd[table]
        for column_name in column:
            column_data = table_data['columns'][column_name]
            data_type = column_data['data_type'].lower()
            print(f"Processing {table}.{column_name} with datatype: {data_type}")
            column_data['type'] = 'categorical'  # Set a default type
            if data_type in ['integer', 'real', 'numeric', 'decimal', 'float', 'double', 'date', 'datetime', 'timestamp']:
                column_data['type'] = 'continuous'
                try:
                    min_val, max_val, unique_count = get_column_stats(table, column_name)
                    # Convert dates to strings if necessary
                    if isinstance(min_val, (datetime.datetime, datetime.date)):
                        min_val = min_val.isoformat()
                    if isinstance(max_val, (datetime.datetime, datetime.date)):
                        max_val = max_val.isoformat()
                    column_data['min_value'] = min_val
                    column_data['max_value'] = max_val
                    column_data['unique_count'] = unique_count
                    column_data['zero_allowed'] = (min_val == 0 or max_val == 0)
                except Exception as e:
                    print(f"Error processing {table}.{column_name}: {e}")
            else:
                try:
                    query = f"SELECT DISTINCT {column_name} FROM {table} LIMIT 100"
                    values_df = pd.read_sql_query(query, conn)
                    values = values_df[column_name].tolist()
                    # Convert any dates to strings
                    values = [
                        v.isoformat() if isinstance(v, (datetime.datetime, datetime.date)) else v
                        for v in values
                    ]
                    column_data['allowable_values'] = values
                    column_data['unique_count'] = len(values)
                except Exception as e:
                    print(f"Error processing {table}.{column_name}: {e}")


def get_all_tables_and_columns(dd):
    all_cols = {}
    for table_name, table_data in dd.items():
        cols = list(table_data['columns'].keys())
        all_cols[table_name] = cols
    return all_cols

# %%
# Initiate the data dictionary
if __name__ == '__main__':

    tables = fetch_tables()
    columns = fetch_columns()
    data_dictionary = build_data_dictionary_from_schema(tables, columns)

    # %%
    columns_to_update = get_all_tables_and_columns(data_dictionary)
    update_column_metadata(data_dictionary, columns_to_update)
    save_data_dictionary(data_dictionary)

    #%%
    # Close the database connection
    conn.close()