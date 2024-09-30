import os
import pyodbc
import pandas as pd
import argparse

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
def compare_tables(df1, df2, table_name, output_file):
    # Add a source column to identify the origin of each row
    df1['source'] = 'db1'
    df2['source'] = 'db2'

    # Concatenate the two DataFrames and drop duplicates based on the data columns (ignore 'source')
    diff = pd.concat([df1, df2]).drop_duplicates(subset=df1.columns[:-1], keep=False)

    if not diff.empty:
        with open(output_file, 'a') as f:
            f.write(f"Differences found in table '{table_name}':\n")
            f.write(diff.to_string())
            f.write("\n\n")
    else:
        with open(output_file, 'a') as f:
            f.write(f"No differences in table '{table_name}'.\n\n")

# Main comparison function
def compare_databases(db1_path, db2_path, output_file):
    # Connect to both databases
    conn1 = connect_to_mdb(db1_path)
    conn2 = connect_to_mdb(db2_path)

    # Get table names from both databases
    tables_db1 = set(get_table_names(conn1))
    tables_db2 = set(get_table_names(conn2))

    # Find common tables
    common_tables = tables_db1.intersection(tables_db2)
    if not common_tables:
        with open(output_file, 'a') as f:
            f.write("No common tables found between the two databases.\n")
        return

    # Compare each common table
    for table in common_tables:
        df1 = read_table(conn1, table)
        df2 = read_table(conn2, table)
        compare_tables(df1, df2, table, output_file)

    # Close the connections
    conn1.close()
    conn2.close()

# Argument parser to accept database paths as positional arguments
def main():
    parser = argparse.ArgumentParser(description="Compare two MDB databases and write differences to a file.")
    parser.add_argument("db1", help="File name of the first MDB database")
    parser.add_argument("db2", help="File name of the second MDB database")
    parser.add_argument("--output", help="File to write the differences (default: differences.txt)", default="differences.txt")

    args = parser.parse_args()

    # Get the current working directory
    current_dir = os.getcwd()

    # Build full paths for the database files
    db1_path = os.path.join(current_dir, args.db1)
    db2_path = os.path.join(current_dir, args.db2)
    output_file = os.path.join(current_dir, args.output)

    # Clear the output file if it already exists
    open(output_file, 'w').close()

    # Compare the databases
    compare_databases(db1_path, db2_path, output_file)

if __name__ == "__main__":
    main()
