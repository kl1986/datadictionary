# %%
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
            if data_type in ['integer', 'real', 'numeric', 'decimal', 'float', 'double', 'date', 'datetime']:
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
                column_data['type'] = 'categorical'
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


# Function to refresh data dictionary
def refresh_data_dictionary(json_file='data_dictionary.json', log_file='data_dictionary_changes.log',
                            update_all_columns=False):
    # Load existing data dictionary
    existing_data_dictionary = load_data_dictionary(json_file)

    # Check if it's the initial run (data dictionary is empty)
    initial_run = not bool(existing_data_dictionary)

    # Fetch current schema
    tables_df = fetch_tables()
    columns_df = fetch_columns()

    # Build new data dictionary from current schema
    new_data_dictionary = build_data_dictionary_from_schema(tables_df, columns_df)

    # Initialize logs
    changes_log = []
    columns_to_update = {}  # Dictionary to keep track of columns that need metadata updates

    if initial_run or update_all_columns:
        # Update metadata for all columns
        print("Updating metadata for all columns.")
        existing_data_dictionary = new_data_dictionary  # Use the new data dictionary
        for table_name, table_data in new_data_dictionary.items():
            columns = list(table_data['columns'].keys())
            columns_to_update[table_name] = columns
    else:
        # Subsequent runs: Handle changes interactively
        existing_tables = set(existing_data_dictionary.keys())
        current_tables = set(new_data_dictionary.keys())

        # Process each table individually
        # 1. Handle Removed Tables
        for table_name in existing_tables - current_tables:
            print(f"\nTable '{table_name}' has been removed from the database.")
            response = input(
                f"Do you want to remove '{table_name}' from the data dictionary? (yes/no): ").strip().lower()
            if response in ['yes', 'y']:
                del existing_data_dictionary[table_name]
                changes_log.append(f"Table removed: {table_name}")
                print(f"'{table_name}' has been removed from the data dictionary.")
            else:
                print(f"'{table_name}' has been kept in the data dictionary.")

        # 2. Handle New Tables
        for table_name in current_tables - existing_tables:
            print(f"\nNew table detected: '{table_name}'")
            response = input(f"Do you want to add '{table_name}' to the data dictionary? (yes/no): ").strip().lower()
            if response in ['yes', 'y']:
                existing_data_dictionary[table_name] = new_data_dictionary[table_name]
                changes_log.append(f"New table added: {table_name}")
                print(f"'{table_name}' has been added to the data dictionary.")
                # Add all columns of the new table to columns_to_update
                columns_to_update[table_name] = list(new_data_dictionary[table_name]['columns'].keys())
            else:
                print(f"'{table_name}' has not been added to the data dictionary.")

        # 3. Handle Existing Tables
        for table_name in existing_tables & current_tables:
            existing_table = existing_data_dictionary[table_name]
            new_table = new_data_dictionary[table_name]

            # Preserve existing table-level metadata
            new_table['description'] = existing_table.get('description', '')

            # Identify existing and current columns
            existing_columns = set(existing_table['columns'].keys())
            current_columns = set(new_table['columns'].keys())

            # a. Handle Removed Columns
            for column_name in existing_columns - current_columns:
                print(f"\nColumn '{column_name}' in table '{table_name}' has been removed from the database.")
                response = input(
                    f"Do you want to remove column '{column_name}' from the data dictionary? (yes/no): ").strip().lower()
                if response in ['yes', 'y']:
                    del existing_table['columns'][column_name]
                    changes_log.append(f"Column removed from {table_name}: {column_name}")
                    print(f"Column '{column_name}' has been removed from the data dictionary.")
                else:
                    print(f"Column '{column_name}' has been kept in the data dictionary.")

            # b. Handle New Columns
            for column_name in current_columns - existing_columns:
                print(f"\nNew column detected in table '{table_name}': '{column_name}'")
                response = input(
                    f"Do you want to add column '{column_name}' to the data dictionary? (yes/no): ").strip().lower()
                if response in ['yes', 'y']:
                    # Copy column metadata from new_table to existing_table
                    existing_table['columns'][column_name] = new_table['columns'][column_name]
                    changes_log.append(f"New column added in {table_name}: {column_name}")
                    print(f"Column '{column_name}' has been added to the data dictionary.")
                    # Add the new column to columns_to_update
                    if table_name not in columns_to_update:
                        columns_to_update[table_name] = []
                    columns_to_update[table_name].append(column_name)
                else:
                    print(f"Column '{column_name}' has not been added to the data dictionary.")

            # c. Handle Existing Columns
            for column_name in existing_columns & current_columns:
                existing_column = existing_table['columns'][column_name]
                new_column = new_table['columns'][column_name]
                # Preserve existing column-level metadata
                new_column['description'] = existing_column.get('description', '')
                # Preserve any other user-added metadata
                for key, value in existing_column.items():
                    if key not in new_column:
                        new_column[key] = value
                # Update column metadata in existing data dictionary
                existing_table['columns'][column_name] = new_column
                # Do not add existing columns to columns_to_update; their metadata will remain untouched

    # Update column metadata
    if columns_to_update:
        update_column_metadata(existing_data_dictionary, columns_to_update)
    else:
        print("\nNo columns to update metadata for.")

    # Save the updated data dictionary
    save_data_dictionary(existing_data_dictionary, json_file)

    # Output changes log
    if changes_log:
        print("\nData dictionary has been updated with the following changes:")
        for change in changes_log:
            print(f"- {change}")
        # Save changes log to file
        with open(log_file, 'w') as f:
            f.write('\n'.join(changes_log))
        print(f"\nChanges have been logged to {log_file}.")
    else:
        print("\nNo changes were made to the data dictionary.")


def get_all_tables_and_columns(dd):
    all_cols = {}
    for table_name, table_data in dd.items():
        cols = list(table_data['columns'].keys())
        all_cols[table_name] = cols
    return all_cols


# %%
# Initiate the data dictionary
tables = fetch_tables()
columns = fetch_columns()
data_dictionary = build_data_dictionary_from_schema(tables, columns)

# %%

columns_to_update = get_all_tables_and_columns(data_dictionary)
update_column_metadata(data_dictionary, columns_to_update)
save_data_dictionary(data_dictionary)

# %%
# Run the refresh data dictionary function
#data_dictionary = load_data_dictionary('data_dictionary.json')
data_dictionary = load_data_dictionary()
refresh_data_dictionary()


#%%
# Close the database connection
#conn.close()