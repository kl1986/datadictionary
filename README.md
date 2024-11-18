# Database Quality Check System

A Python-based system for performing data quality checks on SQLite databases using a data dictionary approach.

## Overview

This project consists of several components that work together to create and validate a SQLite database:

1. Database creation and sample data population
2. Data quality validation
3. Data dictionary management

## Project Structure

- [create_sample_db.py](create_sample_db.py) - Creates a sample SQLite database with test data
- [data_quality_checks.py](data_quality_checks.py) - Performs data validation and quality checks
- [data_dictionary.py](data_dictionary.py) - Manages the data dictionary configuration
- [data_dictionary.json](data_dictionary.json) - Stores data validation rules and constraints

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

1. Create the sample database:
```python
python [create_sample_db.py]
```

2. Create the data dictionary:

```python
python [data_dictionary.py]
```

This will initiate the data dictionary structure, however the metadata and other constraints will need to be added

3. Run data quality checks:

```python
python [data_quality_checks.py]
```

## Dependencies
- pandas
- sqlite3
- json

## Data Quality Checks
The system performs various data quality validations including:

- Null value validation
- Zero value validation
- Duplicate value checks
- Allowable value range checks
- Min/max value boundary checks

## Data Dictionary
The data dictionary (data_dictionary.json) defines the constraints and validation rules for each column in the database. Update this file to modify the validation criteria.