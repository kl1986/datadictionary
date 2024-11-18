# Import Libraries
import sqlite3

# Function to create a sample SQLite database
def create_sample_database(db_name='sample_database.db'):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Create users table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        username TEXT,
        email TEXT,
        created_at DATE
    )
    ''')

    # Create orders table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS orders (
        order_id INTEGER PRIMARY KEY,
        user_id INTEGER,
        product_id INTEGER,
        quantity INTEGER,
        order_date DATE,
        FOREIGN KEY(user_id) REFERENCES users(user_id)
    )
    ''')

    # Create products table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS products (
        product_id INTEGER PRIMARY KEY,
        product_name TEXT,
        price REAL,
        in_stock INTEGER
    )
    ''')

    # Insert sample data into users table
    users_data = [
        (1, 'alice', 'alice@example.com', '2021-01-15'),
        (2, 'bob', 'bob@example.com', '2021-02-20'),
        (3, 'charlie', 'charlie@example.com', '2021-03-25'),
    ]
    cursor.executemany('INSERT INTO users VALUES (?, ?, ?, ?)', users_data)

    # Insert sample data into products table
    products_data = [
        (1, 'Laptop', 999.99, 10),
        (2, 'Smartphone', 499.99, 20),
        (3, 'Headphones', 199.99, 50),
    ]
    cursor.executemany('INSERT INTO products VALUES (?, ?, ?, ?)', products_data)

    # Insert sample data into orders table
    orders_data = [
        (1, 1, 1, 1, '2021-05-10'),
        (2, 2, 2, 2, '2021-06-15'),
        (3, 3, 3, 1, '2021-07-20'),
    ]
    cursor.executemany('INSERT INTO orders VALUES (?, ?, ?, ?, ?)', orders_data)

    conn.commit()
    conn.close()
    print(f"Sample database '{db_name}' created successfully.")

# Create the sample database
create_sample_database()