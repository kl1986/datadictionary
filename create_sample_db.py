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
        order_date TIMESTAMP,
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
        (1, 1, 1, 1, '2021-05-10 10:00:00'),
        (2, 2, 2, 2, '2021-06-15 11:00:00'),
        (3, 3, 3, 1, '2021-07-20 12:00:00'),
        (4, 4, 4, 1, '2021-08-10 10:00:00'),
        (5, 5, 5, 2, '2021-09-15 11:00:00'),
        (6, 6, 6, 1, '2021-10-20 12:00:00'),
        (7, 7, 7, 3, '2021-11-25 13:00:00'),
        (8, 8, 8, 2, '2021-12-30 14:00:00'),
        (9, 9, 9, 1, '2022-01-05 15:00:00'),
        (10, 10, 10, 2, '2022-02-10 16:00:00'),
    ]
    cursor.executemany('INSERT INTO orders VALUES (?, ?, ?, ?, ?)', orders_data)
    # Insert more sample data into users table
    additional_users_data = [
        (4, 'david', 'david@example.com', '2021-04-10'),
        (5, 'eve', 'eve@example.com', '2021-05-15'),
        (6, 'frank', 'frank@example.com', '2021-06-20'),
        (7, 'grace', 'grace@example.com', '2021-07-25'),
        (8, 'heidi', 'heidi@example.com', '2021-08-30'),
        (9, 'ivan', 'ivan@example.com', '2021-09-05'),
        (10, 'judy', 'judy@example.com', '2021-10-10'),
        (11, 'mallory', 'mallory@example.com', '2021-11-15'),
        (12, 'oscar', 'oscar@example.com', '2021-12-20'),
        (13, 'peggy', 'peggy@example.com', '2022-01-25'),
        (14, 'trent', 'trent@example.com', '2022-02-28'),
        (15, 'victor', 'victor@example.com', '2022-03-05'),
        (16, 'walter', 'walter@example.com', '2022-04-10'),
        (17, 'xavier', 'xavier@example.com', '2022-05-15'),
        (18, 'yvonne', 'yvonne@example.com', '2022-06-20'),
        (19, 'zach', 'zach@example.com', '2022-07-25'),
        (20, 'aaron', 'aaron@example.com', '2022-08-30'),
        (21, 'betty', 'betty@example.com', '2022-09-05'),
        (22, 'carl', 'carl@example.com', '2022-10-10'),
        (23, 'diana', 'diana@example.com', '2022-11-15'),
        (24, 'edward', 'edward@example.com', '2022-12-20'),
        (25, 'fiona', 'fiona@example.com', '2023-01-25'),
        (26, 'george', 'george@example.com', '2023-02-28'),
        (27, 'hannah', 'hannah@example.com', '2023-03-05'),
        (28, 'ian', 'ian@example.com', '2023-04-10'),
        (29, 'jane', 'jane@example.com', '2023-05-15'),
        (30, 'kyle', 'kyle@example.com', '2023-06-20')
    ]
    cursor.executemany('INSERT INTO users VALUES (?, ?, ?, ?)', additional_users_data)

    # Insert more sample data into products table
    additional_products_data = [
        (4, 'Tablet', 299.99, 30),
        (5, 'Monitor', 199.99, 25),
        (6, 'Keyboard', 49.99, 100),
        (7, 'Mouse', 29.99, 150),
        (8, 'Printer', 149.99, 20),
        (9, 'Camera', 399.99, 15),
        (10, 'Speaker', 99.99, 40),
        (11, 'Router', 79.99, 35),
        (12, 'Hard Drive', 89.99, 50),
        (13, 'SSD', 129.99, 60),
        (14, 'RAM', 69.99, 70),
        (15, 'GPU', 499.99, 10),
        (16, 'CPU', 299.99, 20),
        (17, 'Motherboard', 199.99, 25),
        (18, 'Power Supply', 79.99, 30),
        (19, 'Case', 59.99, 40),
        (20, 'Cooling Fan', 19.99, 100),
        (21, 'Webcam', 49.99, 50),
        (22, 'Microphone', 39.99, 60),
        (23, 'Headset', 89.99, 70),
        (24, 'Charger', 29.99, 80),
        (25, 'USB Cable', 9.99, 200),
        (26, 'HDMI Cable', 14.99, 150),
        (27, 'Ethernet Cable', 12.99, 100),
        (28, 'Flash Drive', 19.99, 120),
        (29, 'Memory Card', 24.99, 130),
        (30, 'Docking Station', 99.99, 40)
    ]
    cursor.executemany('INSERT INTO products VALUES (?, ?, ?, ?)', additional_products_data)

    # Insert more sample data into orders table
    additional_orders_data = [
        (11, 11, 11, 3, '2022-03-15 17:00:00'),
        (12, 12, 12, 1, '2022-04-20 18:00:00'),
        (13, 13, 13, 2, '2022-05-25 19:00:00'),
        (14, 14, 14, 3, '2022-06-30 20:00:00'),
        (15, 15, 15, 1, '2022-07-05 21:00:00'),
        (16, 16, 16, 2, '2022-08-10 22:00:00'),
        (17, 17, 17, 3, '2022-09-15 23:00:00'),
        (18, 18, 18, 1, '2022-10-20 00:00:00'),
        (19, 19, 19, 2, '2022-11-25 01:00:00'),
        (20, 20, 20, 3, '2022-12-30 02:00:00'),
        (21, 21, 21, 1, '2023-01-05 03:00:00'),
        (22, 22, 22, 2, '2023-02-10 04:00:00'),
        (23, 23, 23, 3, '2023-03-15 05:00:00'),
        (24, 24, 24, 1, '2023-04-20 06:00:00'),
        (25, 25, 25, 2, '2023-05-25 07:00:00'),
        (26, 26, 26, 3, '2023-06-30 08:00:00'),
        (27, 27, 27, 1, '2023-07-05 09:00:00'),
        (28, 28, 28, 2, '2023-08-10 10:00:00'),
        (29, 29, 29, 3, '2023-09-15 11:00:00'),
        (30, 30, 30, 1, '2023-10-20 12:00:00')
    ]
    cursor.executemany('INSERT INTO orders VALUES (?, ?, ?, ?, ?)', additional_orders_data)
    conn.commit()
    conn.close()
    print(f"Sample database '{db_name}' created successfully.")

# Create the sample database
create_sample_database()