# Database Quality Check System

A Python-based system for performing data quality checks on databases using a data dictionary approach.

## Overview

This project consists of several components that work together to create and validate a SQLite database:

1. Database creation and sample data population
2. Data dictionary creation
3. Data dictionary management
3. Data quality validation

## Project Structure

- [create_sample_db.py](create_sample_db.py) - Creates a sample SQLite database with test data
- [data_quality_checks.py](data_quality_checks.py) - Performs data validation and quality checks
- [init_data_dictionary.py](data_dictionary.py) - Manages the data dictionary configuration
- [refresh_data_dictionary.py](refresh_data_dictionary.py) - Checks the database for any new/deleted tables/columns and prompts user on whether to add/del from the data dictionary
- [data_dictionary.json](data_dictionary.json) - Stores data validation rules and constraints
- [db_conn.py](db_conn.py) - creates connection to database, currently set to use sample sqlite db but can be changed to use any database

## Database Schema

The sample database contains three main tables:

### Users Table
- user_id (INTEGER PRIMARY KEY)
- username (TEXT)
- email (TEXT)
- created_at (DATE)

### Orders Table
- order_id (INTEGER PRIMARY KEY)
- user_id (INTEGER, FOREIGN KEY)
- product_id (INTEGER)
- quantity (INTEGER)
- order_date (TIMESTAMP)

### Products Table
- product_id (INTEGER PRIMARY KEY)
- product_name (TEXT)
- price (REAL)
- in_stock (INTEGER)

## Setup and Usage

1. Clone repo

2. Set up connection to database

Goto ```db_conn.py``` and set up relevant connection.  Note that it is recommended that you create a ```env.py``` (DO NOT COMMIT) and store your credentials there or set them up as environmental variables if you can do so.

3. (OPTIONAL) Create the sample database:
```python
python [create_sample_db.py]
```

4. Create the data dictionary:

```python
python [data_dictionary.py]
```

This will initiate the data dictionary structure, however the metadata and other constraints will need to be added/edited in 

```
[data_dictionary.json]
```

5. Update the data dictionary

    5a. Create a new branch

    5b. Open ```data_dictionary.json``` and navigate to the table / column that you own.  Update the relevant fields and save the file.

    5c. Update the main branch by commit, pushing the changes and then submitting a pull request



6. Run and view data quality checks:

```python
python [data_quality_checks.py]
```

You can then review the results in the log file ```DQ_Report_{DATETIMESTAMP}.log```

## Dependencies
- pandas
- sqlite3 (or other database connection library you are using)
- json
- logging
- datetime
- tqdm

## Data Quality Checks
The system performs various data quality validations including:

- Null value validation
- Zero value validation
- Duplicate value checks
- Allowable value range checks
- Min/max value boundary checks

## Data Dictionary
The data dictionary (data_dictionary.json) defines the constraints and validation rules for each column in the database. Update this file to modify the validation criteria.