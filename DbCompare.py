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

# Function to compare two DataFrames for specific column differences but return all columns in the output
def compare_column_differences(df1, df2, table_name, columns_to_compare, output_file):
    # Add a source column to identify the origin of each row
    df1['source'] = 'db1'
    df2['source'] = 'db2'

    # Merge the two DataFrames based on the specified columns to compare
    merge_on = columns_to_compare
    merged = pd.merge(df1, df2, on=merge_on, how='outer', indicator=True)

    # Separate out rows that differ between the two DataFrames
    differences = merged[merged['_merge'] != 'both']

    if not differences.empty:
        with open(output_file, 'a') as f:
            f.write(f"Differences found in table '{table_name}' for columns: {', '.join(columns_to_compare)}\n")
            f.write("Full row differences:\n")
            f.write(differences.to_string(index=False))
            f.write("\n\n")
    else:
        with open(output_file, 'a') as f:
            f.write(f"No differences in table '{table_name}' for specified columns: {', '.join(columns_to_compare)}\n\n")

# Main comparison function
def compare_databases(db1_path, db2_path, output_file, table_name, columns_to_compare):
    # Connect to both databases
    conn1 = connect_to_mdb(db1_path)
    conn2 = connect_to_mdb(db2_path)

    # Get table names from both databases
    tables_db1 = set(get_table_names(conn1))
    tables_db2 = set(get_table_names(conn2))

    # Check if the specified table exists in both databases
    if table_name not in tables_db1 or table_name not in tables_db2:
        with open(output_file, 'a') as f:
            f.write(f"Table '{table_name}' not found in both databases.\n")
        return

    # Compare the specified table
    df1 = read_table(conn1, table_name)
    df2 = read_table(conn2, table_name)
    compare_column_differences(df1, df2, table_name, columns_to_compare, output_file)

    # Close the connections
    conn1.close()
    conn2.close()

# Argument parser to accept database paths as positional arguments
def main():
    parser = argparse.ArgumentParser(description="Compare two MDB databases and write differences to a file.")
    parser.add_argument("db1", help="File name of the first MDB database")
    parser.add_argument("db2", help="File name of the second MDB database")
    parser.add_argument("--table", help="Table name to compare", required=True)
    parser.add_argument("--output", help="File to write the differences (default: differences.txt)", default="differences.txt")
    parser.add_argument("--columns", nargs='+', help="List of columns to compare for differences", required=True)

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
    compare_databases(db1_path, db2_path, output_file, args.table, args.columns)

if __name__ == "__main__":
    main()
