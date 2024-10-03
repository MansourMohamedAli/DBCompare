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
def compare_column_differences(df1, df2, table_name, output_file, db1_name, db2_name):
    # Add a source column to identify the origin of each row
    df1['source'] = 'db1'
    df2['source'] = 'db2'

    # Merge the two DataFrames based on the specified columns to compare
    merge_on = df1.columns[0]
    merged = pd.merge(df1, df2, on=merge_on, how='outer', indicator=True)

    # Separate out rows that differ between the two DataFrames
    new_in_db1 = merged[merged['_merge'] == 'left_only'].dropna(axis=1,how='all')
    new_in_db2 = merged[merged['_merge'] == 'right_only'].dropna(axis=1,how='all')

    headings_list = list()
    for heading in new_in_db1.columns.values:
        if heading in df1.columns[0] or heading == "_merge":
            headings_list.append(heading)
        else:
            headings_list.append(heading[:-2])
    new_in_db1.columns = headings_list

    headings_list = list()
    for heading in new_in_db2.columns.values:
        if heading in df1.columns[0] or heading == "_merge":
            headings_list.append(heading)
        else:
            headings_list.append(heading[:-2])
    new_in_db2.columns = headings_list
    
    # Write the results to the output file
    with open(output_file, 'a') as f:
        if not new_in_db1.empty:
            f.write(f"New entries in table '{table_name}' found only in {db1_name}:\n")
            f.write(new_in_db1.to_string(index=False))
            f.write("\n\n")
        else:
            f.write(f"No new entries in table '{table_name}' found only in {db1_name}.\n\n")

        if not new_in_db2.empty:
            f.write(f"New entries in table '{table_name}' found only in {db2_name}:\n")
            f.write(new_in_db2.to_string(index=False))
            f.write("\n\n")
        else:
            f.write(f"No new entries in table '{table_name}' found only in {db2_name}.\n\n")
    
    if not new_in_db1.empty and df1.columns[0] == 'f_ptid':
        create_add_file(new_in_db1, db1_name[:-4])
        
    if not new_in_db2.empty and df1.columns[0] == 'f_ptid':
        create_add_file(new_in_db2, db2_name[:-4])


def create_add_file(db, db_name):
        with open(f"{db_name}.add", 'w') as f:
            for row in db.itertuples(index=True, name='Pandas'):
                f.write(f'add {row.f_ptid}\n')
                try:
                    f.write(f'.desc {row.f_brief}\n')
                except AttributeError: # No Value
                    f.write(f'.desc\n')
                try:
                    f.write(f'{row.f_ldes}\n')
                except AttributeError: # No Value
                    f.write(f'\n')
                try:
                    if str(row.f_unit) == "None":
                        unit = ""
                    else:
                        unit = row.f_unit       
                    f.write(f'.units {unit}\n')
                except AttributeError: # No Value
                    f.write(f'.units\n')
                f.write(f'.type {row.f_dtype}*{int(row.f_precs)}\n')
                try:
                    if str(row.f_value) == "None":
                        valu = ""
                    else:
                        valu = row.f_value
                    f.write(f'.valu {valu}\n')
                except AttributeError: # No Value
                    f.write(f'.valu\n')
                f.write(f'.dim {int(row.f_dim1)}, {int(row.f_dim2)}, {int(row.f_dim3)}\n')
                f.write(f'.pred {row.f_pred}\n\n')                    
                
# Main comparison function
def compare_databases(db1_path, db2_path, output_file, table_name, db1_name, db2_name):
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
    compare_column_differences(df1, df2, table_name, output_file, db1_name, db2_name)

    # Close the connections
    conn1.close()
    conn2.close()

# Argument parser to accept database paths as positional arguments
def main():
    parser = argparse.ArgumentParser(description="Compare two MDB databases and write differences to a file.")
    parser.add_argument("db1", help="File name of the first MDB database")
    parser.add_argument("db2", help="File name of the second MDB database")
    parser.add_argument("--table", help="Table name to compare", required=True)
    # parser.add_argument("--column", help="Column to compare for differences (default: f_ptid)", default="f_ptid")
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
    # compare_databases(db1_path, db2_path, output_file, "vars", "f_ptid", args.db1, args.db2)
    compare_databases(db1_path, db2_path, output_file, args.table, args.db1, args.db2)

if __name__ == "__main__":
    main()
