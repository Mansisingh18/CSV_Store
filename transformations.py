import pandas as pd
import requests
import psycopg2

# Task 1: Download CSV files from github
# URLs for the CSV files
customer_csv_url = "https://github.com/Mansisingh18/CSV_Store/raw/7c39870c01d698aee51cbf8920f06f0740cce41c/customer.csv"
destination_csv_url = "https://github.com/Mansisingh18/CSV_Store/raw/7c39870c01d698aee51cbf8920f06f0740cce41c/destination_data.csv"
booking_csv_url = "https://github.com/Mansisingh18/CSV_Store/raw/7c39870c01d698aee51cbf8920f06f0740cce41c/booking_data.csv"

#Download CSV file and load it into DataFrame
def download_csv_to_dataframe(url):
    response = requests.get(url)
    if response.status_code == 200:
        return pd.read_csv(url)
    else:
        print(f"Failed to download CSV from {url}")
        return None

# Download CSV files and load them into DataFrames
customer_data = download_csv_to_dataframe(customer_csv_url)
destination_data = download_csv_to_dataframe(destination_csv_url)
booking_data = download_csv_to_dataframe(booking_csv_url)

# Task 2: Cleanse and transform the data
# Merge booking_data with customer_data on customer_id
merged_data = pd.merge(booking_data, customer_data, on='customer_id', how='left')

# Merge merged_data with destination_data on destination
final_data = pd.merge(merged_data, destination_data, on='destination', how='left')

# Convert date formats
final_data['booking_date'] = pd.to_datetime(final_data['booking_date'])

# Handling missing or erroneous data (for example, fill missing values with 0)
final_data.fillna(0, inplace=True)

# Calculate total booking value
final_data['total_booking_value'] = final_data['number_of_passengers'] * final_data['cost_per_passenger']

# Save the transformed data to a new CSV file
final_data.to_csv('transformed_data.csv', index=False)

print("Data cleansing and transformation completed.")

# Task 3: Load the transformed data into a PostgreSQL database
# Connect to the PostgreSQL database
conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="pwd123",
    host="localhost",
    port="5432"
)

# Create a cursor object to execute SQL queries
cur = conn.cursor()

# Create tables for customer, booking, and destination
cur.execute("""
    CREATE TABLE IF NOT EXISTS customers (
        customer_id SERIAL PRIMARY KEY,
        first_name VARCHAR(255),
        last_name VARCHAR(255),
        email VARCHAR(255),
        phone VARCHAR(20)
    )
""")

cur.execute("""
    CREATE TABLE IF NOT EXISTS destinations (
        destination_id SERIAL PRIMARY KEY,
        destination VARCHAR(255),
        country VARCHAR(255),
        popular_season VARCHAR(20)
    )
""")

cur.execute("""
    CREATE TABLE IF NOT EXISTS bookings (
        booking_id SERIAL PRIMARY KEY,
        customer_id INT REFERENCES customers(customer_id),
        booking_date DATE,
        destination_id INT REFERENCES destinations(destination_id),
        number_of_passengers INT,
        cost_per_passenger FLOAT,
        total_booking_value FLOAT
    )
""")

# Commit the changes
conn.commit()

# Insert data into customers table
customers_data = final_data[['customer_id', 'first_name', 'last_name', 'email', 'phone']].drop_duplicates()
for _, row in customers_data.iterrows():
    cur.execute("""
        INSERT INTO customers (customer_id, first_name, last_name, email, phone)
        VALUES (%s, %s, %s, %s, %s)
    """, (row['customer_id'], row['first_name'], row['last_name'], row['email'], row['phone']))

# Insert data into destinations table
destinations_data = final_data[['destination_id', 'destination', 'country', 'popular_season']].drop_duplicates()
for _, row in destinations_data.iterrows():
    cur.execute("""
        INSERT INTO destinations (destination_id, destination, country, popular_season)
        VALUES (%s, %s, %s, %s)
    """, (row['destination_id'], row['destination'], row['country'], row['popular_season']))

# Insert data into bookings table
for _, row in final_data.iterrows():
    cur.execute("""
        INSERT INTO bookings (customer_id, booking_date, destination_id, number_of_passengers, cost_per_passenger, total_booking_value)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (row['customer_id'], row['booking_date'], row['destination_id'], row['number_of_passengers'], row['cost_per_passenger'], row['total_booking_value']))

# Commit the changes
conn.commit()

# Close the cursor and connection
cur.close()
conn.close()

print("Data loaded into PostgreSQL database successfully.")
