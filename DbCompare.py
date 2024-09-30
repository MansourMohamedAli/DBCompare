import pyodbc
import pandas as pd
import argparse
import os

# Function to connect to an Access Database (.mdb)
def connect_to_mdb(db_path):
    # Change DRIVER based on the architecture of your Office installation (32-bit vs 64-bit)
    conn_str = (
        r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
        r'DBQ=' + db_path + ';'
    )
    return pyodbc.connect(conn_str)

# Function to retrieve table names from the database
def get_table_names(connection):
    cursor = connection.cursor()
    cursor.tables()
    tables = [row.table_name for row in cursor if row.table_type == 'TABLE']
    return tables

# Function to read a table into a pandas DataFrame
def read_table(connection, table_name):
    query = f"SELECT * FROM [{table_name}]"
    return pd.read_sql(query, connection)

# Function to compare two DataFrames and return the differences
def compare_tables(df1, df2, table_name):
    diff = pd.concat([df1, df2]).drop_duplicates(keep=False)
    if not diff.empty:
        print(f"Differences found in table '{table_name}':")
        print(diff)
    else:
        print(f"No differences in table '{table_name}'.")

# Main comparison function
def compare_databases(db1_path, db2_path):
    # Connect to both databases
    conn1 = connect_to_mdb(db1_path)
    conn2 = connect_to_mdb(db2_path)

    # Get table names from both databases
    tables_db1 = set(get_table_names(conn1))
    tables_db2 = set(get_table_names(conn2))

    # Find common tables
    common_tables = tables_db1.intersection(tables_db2)
    if not common_tables:
        print("No common tables found between the two databases.")
        return

    # Compare each common table
    for table in common_tables:
        df1 = read_table(conn1, table)
        df2 = read_table(conn2, table)
        compare_tables(df1, df2, table)

    # Close the connections
    conn1.close()
    conn2.close()

# Argument parser to accept database paths as positional arguments
def main():
    parser = argparse.ArgumentParser(description="Compare two MDB databases.")
    parser.add_argument("db1", help="Path to the first MDB database")
    parser.add_argument("db2", help="Path to the second MDB database")

    args = parser.parse_args()

    # Get the current working directory
    current_dir = os.getcwd()

    # Build full paths for the database files
    db1_path = os.path.join(current_dir, args.db1)
    db2_path = os.path.join(current_dir, args.db2)

    # Compare the databases
    compare_databases(db1_path, db2_path)

if __name__ == "__main__":
    main()
