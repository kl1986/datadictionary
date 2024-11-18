from data_dictionary import *

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

# %%
# Initiate the data dictionary
if __name__ == '__main__':
    refresh_data_dictionary()

    #%%
    # Close the database connection
    conn.close()