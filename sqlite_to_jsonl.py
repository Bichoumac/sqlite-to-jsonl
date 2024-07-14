import os
import argparse
import sqlite3
import json
import shutil

def is_sqlite3(filename):
    """
    Check if a file is a SQLite3 database.

    Args:
    filename (str): The path to the file.

    Returns:
    bool: True if the file is a SQLite3 database, False otherwise.
    """
    if not os.path.isfile(filename):
        print (f"[ERROR] {filename} is not a file")
        return False
    if os.path.getsize(filename) < 100:  # SQLite database file header is 100 bytes
        print (f"[ERROR] {filename} is too small to be a SQLite3 database")
        return False

    with open(filename, 'rb') as file:
        header = file.read(16)
    
    result_bool = header == b'SQLite format 3\x00'
    if not result_bool:
        print (f"[ERROR] {filename} has the wrong header: {header[:15]}")
    
    return result_bool

def write_dicts_to_jsonl(dict_list, output_file):
    """
    Writes a list of dictionaries to a JSON Lines (jsonl) file.

    Args:
    dict_list (list): A list of dictionaries.
    output_file (str): The name of the output JSON Lines file.
    """
    try:
        output_folder = os.sep.join(output_file.split(os.sep)[:-1])
        create_folder_if_not_exists(output_folder)

        with open(output_file, "w+") as f:
            for entry in dict_list:
                json_line = json.dumps(entry)
                f.write(json_line + '\n')

    except Exception as e:
        print(f"[EXCEPTION RAISED] An error occurred when writing to the file '{output_file}': {e}")

def to_string(data):
    """
    Check if the data is bytes, 
    if it's bytes, then we're going to return the string '\x00\x00' (if the data is like \x00\x00)
    else return the data
    """
    if isinstance(data, bytes):
        return str(data).lstrip("b").strip("'")
    else :
        return data
    
def process_table (output_dir, table_name, cursor):
    """
    Reads the contents of a table from a SQLite3 database and writes it to a jsonl file.

    Args:
    database_path (str): The path to the SQLite3 database.
    table_name (str): The name of the table to read.
    """
        
    # Get the column names
    cursor.execute(f"PRAGMA table_info({table_name});")
    columns_info = cursor.fetchall()
    column_names = [column[1] for column in columns_info]
    
    # Execute the query to get the contents of the table
    cursor.execute(f"SELECT * FROM {table_name};")
    rows = cursor.fetchall()
            
    output_file = os.path.join(output_dir, table_name) + ".jsonl"
        
    rows_list = []
    
    for row in rows:
        row_dict = {column_name: to_string(value) for column_name, value in zip(column_names, row)}
        rows_list.append(row_dict)
    
    write_dicts_to_jsonl(rows_list, output_file)
 
def get_tables (cursor):
    """
    Read a SQLite3 database to extract all the tables.
    """

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]
    return tables

def apply_wal (conn, input_filename):
    """
    Apply the wal file to the database
    """

    wal_name = input_filename+"-wal"
    if not os.path.exists(wal_name):
        print("[INFO] No WAL applied to", input_filename)
    
    # Backup of the original
    # Faites une copie de sauvegarde de la base de données originale
    backup_path = input_filename + '.backup'
    shutil.copy(input_filename, backup_path)
    print(f"[INFO] A backup has been done: {backup_path}.")
    
    try : 
        conn.execute("PRAGMA wal_checkpoint(FULL);")
        conn.execute("VACUUM;")
        os.remove(backup_path)
    except Exception as e :
        shutil.copy(backup_path, input_filename)
        os.remove(backup_path)
        print("[EXCEPTION RAISED] Applying WAL:", e)
    

def process_file(input_filename, output_dir, already_checked=False):
    """
    Convert the input to jsonl file, one file per table.
    """
    conn = ""
    cursor = ""
    print (f"[INFO] Processing {input_filename}...")
    
    if is_sqlite3(input_filename) or already_checked :
        # Create a folder for the Database
        output_dir = str(os.path.join(output_dir, input_filename))
        create_folder_if_not_exists(output_dir)
        try:
            # Connect to the sqlite3 database
            conn = sqlite3.connect(input_filename)
            cursor = conn.cursor()
            
            # Try to apply the WAL file
            apply_wal(conn, input_filename)
            
            # Retrieve all the tables
            tables = get_tables(cursor)
            
            # Process each table
            for table in tables:
                process_table(output_dir, table, cursor)

            # Close the connection

        except Exception as e:
            print(f"[EXCEPTION RAISED] {e}")
            cursor.close()
            conn.close()   

    else :
        print(f"[ERROR] {input_filename} is not a sqlite3 file")

def process_folder(input_foldername, output_dir):
    """
    Read each file of a folder, and process all sqlite3 file
    """
    
    list_file = os.listdir(input_foldername)
    for file in list_file:
        filename = os.path.join(input_foldername, file)
        if os.path.isfile(filename) and is_sqlite3(filename):
            process_file(filename, output_dir, already_checked=True)
        
def create_folder_if_not_exists(folder_path):
    """
    Create a folder and its parent directories if they do not exist.
    
    Args:
    folder_path (str): The path of the folder to create.
    """
    try:
        os.makedirs(folder_path, exist_ok=True)
        #print(f"Folder '{folder_path}' created successfully or already exists.")
    except Exception as e:
        print(f"[EXCEPTION RAISED] An error occurred while creating the folder '{folder_path}': {e}")

def main():
    parser = argparse.ArgumentParser(description="Process a sqlite file or all sqlite file of a given folder and optionally specify an output directory.")
    parser.add_argument('-o', '--output_dir', type=str, help='The output directory')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-f', '--input_filename', type=str, help='The input filename')
    group.add_argument('-d', '--input_foldername', type=str, help='The input foldername')
    
    args = parser.parse_args()

    output_folder = os.path.join(os.getcwd(), "output") if args.output_dir is None else args.output_dir

    create_folder_if_not_exists(output_folder)

    if args.input_filename:
        process_file(args.input_filename, output_folder)
    elif args.input_foldername:
        process_folder(args.input_foldername, output_folder)


if __name__ == "__main__":
    main()

