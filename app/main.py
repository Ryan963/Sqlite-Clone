import sys

from dataclasses import dataclass

import sys


def find_table_metadata(file, table_name):
    """
    Finds the root page and CREATE TABLE statement for the specified table.
    """
    table_name = table_name.lower()  # Normalize for case-insensitive matching

    # Go to the sqlite_schema table, typically on page 1 (offset 100 bytes)
    file.seek(103)  # Offset where the number of cells is stored
    number_of_tables = int.from_bytes(file.read(2), byteorder='big')

    # Move to cell pointer array which starts after page header (assumed at byte 108)
    file.seek(108)
    cells_arr = [int.from_bytes(file.read(2), byteorder='big') for _ in range(number_of_tables)]

    # Loop through each cell
    for cell_pointer in cells_arr:
        # Calculate the correct offset to start reading the cell
        sqlite_schema_page_offset = 100
        cell_start_offset = sqlite_schema_page_offset + cell_pointer
        file.seek(cell_start_offset)
        parse_varint(file)  # Read and ignore the size of the record
        parse_varint(file)  # Read and ignore the rowid
        record = parse_record(file)

        if len(record) > 2:
            tbl_name = record[2]
            if isinstance(tbl_name, bytes):
                tbl_name = tbl_name.decode('utf-8', errors='ignore').strip().lower()

            # Compare the normalized table name
            if tbl_name == table_name:

                create_table_sql = record[4]
                if isinstance(create_table_sql, bytes):
                    create_table_sql = create_table_sql.decode('utf-8', errors='ignore')
                return record[3], create_table_sql

    raise ValueError(f"Table '{table_name}' not found in sqlite_schema")




def parse_record(file):
    """
    Parses a record from the file and returns a list of column values.
    """
    # Read the record header size (this is a varint)
    header_size = parse_varint(file)
    # header_start = file.tell()
    # print("header size", header_size)

    # List to store serial types of each column
    serial_types = []

    # Read serial types until the end of the header
    for i in range(5):
        serial_type = parse_varint(file)
        serial_types.append(serial_type)

    # print(f"Serial types: {serial_types}")

    # List to store actual column values
    values = []

    # Read each column value based on its serial type
    for serial_type in serial_types:
        # print(f"Reading column with serial type: {serial_type}")
        if serial_type == 0:  # NULL
            values.append(None)
        elif serial_type == 1:  # 8-bit signed integer
            values.append(int.from_bytes(file.read(1), byteorder='big', signed=True))
        elif serial_type == 2:  # 16-bit signed integer
            values.append(int.from_bytes(file.read(2), byteorder='big', signed=True))
        elif serial_type == 3:  # 24-bit signed integer
            values.append(int.from_bytes(file.read(3), byteorder='big', signed=True))
        elif serial_type == 4:  # 32-bit signed integer
            values.append(int.from_bytes(file.read(4), byteorder='big', signed=True))
        elif serial_type == 5:  # 48-bit signed integer
            values.append(int.from_bytes(file.read(6), byteorder='big', signed=True))
        elif serial_type == 6:  # 64-bit signed integer
            values.append(int.from_bytes(file.read(8), byteorder='big', signed=True))
        elif serial_type == 7:  # IEEE floating point
            values.append(float.fromhex(file.read(8).hex()))
        elif serial_type == 8:  # 0
            values.append(0)
        elif serial_type == 9:  # 1
            values.append(1)
        elif serial_type >= 12 and serial_type % 2 == 0:  # BLOB
            length = (serial_type - 12) // 2
            values.append(file.read(length))
        elif serial_type >= 13 and serial_type % 2 == 1:  # TEXT
            length = (serial_type - 13) // 2
            values.append(file.read(length))
    
    # print(f"Parsed values: {values}")
    return values



def parse_varint(file):
    """
    Reads a variable-length integer from the file.
    Returns the decoded integer and advances the file pointer accordingly.
    """
    value = 0
    shift = 0

    while True:
        byte = file.read(1)
        if not byte:
            raise EOFError("Unexpected end of file while reading varint")
        byte = ord(byte)  # Convert byte to an integer value
        value |= (byte & 0x7F) << shift  # Combine the 7 bits of the byte into the value
        if (byte & 0x80) == 0:  # If the most significant bit is not set, it's the end of varint
            break
        shift += 7  # Move to the next 7 bits
    return value

def extract_query_components(command):
    """
    Extracts the column name, table name, and WHERE clause (if any) from the SELECT command.
    """
    parts = command.strip().split()
    if len(parts) < 4 or parts[0].upper() != "SELECT" or parts[2].upper() != "FROM":
        raise ValueError("Invalid SQL command format")

    column_name = parts[1]
    table_name = parts[3]
    where_clause = None

    if "WHERE" in command.upper():
        where_index = command.upper().index("WHERE")
        where_clause = command[where_index + len("WHERE"):].strip()

    return column_name, table_name, where_clause

def parse_where_clause(where_clause):
    """
    Parses a simple WHERE clause, supporting equality checks.
    """
    if "=" not in where_clause:
        raise ValueError("Only simple equality WHERE clauses are supported")

    column_name, value = where_clause.split("=", 1)
    column_name = column_name.strip()
    value = value.strip().strip("'")  # Remove any surrounding quotes from value
    return column_name, value

def read_page_header(file, page_number):
    """
    Reads the header of a page and returns the number of cells and cell pointers.
    """
    page_offset = (page_number - 1) * 4096  # Assuming a page size of 4096 bytes
    file.seek(page_offset)
    page_header = file.read(8)
    num_cells = int.from_bytes(page_header[3:5], byteorder='big')
    cell_pointers = [int.from_bytes(file.read(2), byteorder='big') for _ in range(num_cells)]
    return num_cells, cell_pointers

def find_table_rootpage(file, table_name):
    """
    Finds the root page number for the specified table in the sqlite_schema.
    """
    database_file.seek(103)
    number_of_tables = int.from_bytes(database_file.read(2), byteorder='big')
    # skip the page header then the table header
    database_file.seek(108)
    cells_arr = [int.from_bytes(database_file.read(2), byteorder='big') for _ in range(number_of_tables)]
    for cell_pointer in cells_arr:
        file.seek(cell_pointer)
        parse_varint(file)  # Read and ignore the size of the record
        parse_varint(file)  # Read and ignore the rowid
        record = parse_record(file)
        if record[2].decode('utf-8') == table_name:  # Check if the tbl_name matches the table name
            return record[3]  # Return the root page
    raise ValueError(f"Table {table_name} not found in sqlite_schema")

def find_column_index(create_table_sql, column_name):
    """
    Finds the index of the specified column in the CREATE TABLE statement.
    """

    start = create_table_sql.find("(")
    end = create_table_sql.rfind(")")

    if start == -1 or end == -1 or start > end:
        raise ValueError("Invalid CREATE TABLE statement format")
    columns_part = create_table_sql[start + 1:end].strip()
    column_defs = [col.strip() for col in columns_part.split(",")]

    for index, column_def in enumerate(column_defs):
        col_name = column_def.split()[0]  # The column name is the first word in the definition
        if col_name == column_name:
            return index

    # If the column name wasn't found
    raise ValueError(f"Column '{column_name}' not found in CREATE TABLE statement")


def extract_column_values(file, root_page_number, column_index, where_clause=None):
    """
    Extracts the values of the specified column from the table given its root page number.
    Applies the WHERE clause if specified.
    """
    num_cells, cell_pointers = read_page_header(file, root_page_number)
    column_values = []

    where_column_index = None
    where_value = None

    if where_clause:
        where_column_name, where_value = parse_where_clause(where_clause)

        # Find the index of the column specified in the WHERE clause
        create_table_sql = find_table_metadata(file, table_name)[1]
        where_column_index = find_column_index(create_table_sql, where_column_name)

    for cell_pointer in cell_pointers:
        file.seek((root_page_number - 1) * 4096 + cell_pointer)
        parse_varint(file)  # Record size
        parse_varint(file)  # Rowid
        record = parse_record(file)

        if where_clause:
            # Apply filter: only add rows that match the WHERE condition
            if where_column_index < len(record) and record[where_column_index] != where_value:
                continue

        if column_index < len(record):
            value = record[column_index]
            if isinstance(value, bytes):  # Decode if it's bytes (TEXT)
                value = value.decode('utf-8')
            column_values.append(value)

    return column_values


def count_rows_in_table(file, root_page_number):
    """
    Counts the number of rows in a table given the root page number.
    """
    num_cells, cell_pointers = read_page_header(file, root_page_number)
    return num_cells


database_file_path = sys.argv[1]
command = sys.argv[2]

with open(database_file_path, "rb") as database_file:
    if command == ".dbinfo":
            # You can use print statements as follows for debugging, they'll be visible when running tests.
            print("Logs from your program will appear here!")

            database_file.seek(16)  # Skip the first 16 bytes of the header
            page_size = int.from_bytes(database_file.read(2), byteorder="big")
            print(f"database page size: {page_size}")
            database_file.seek(103)
            number_of_tables = int.from_bytes(database_file.read(2), byteorder='big')
            print(f"number of tables: {number_of_tables}")
    elif command.startswith("SELECT"):
        # Extract the column name and table name from the command
        column_name, table_name, where_clause = extract_query_components(command)
        root_page, create_table_sql = find_table_metadata(database_file, table_name)
        column_index = find_column_index(create_table_sql, column_name)
        column_values = extract_column_values(database_file, root_page, column_index)
        for value in column_values:
            print(value)
    elif command == '.tables':
            database_file.seek(103)
            number_of_tables = int.from_bytes(database_file.read(2), byteorder='big')
            # skip the page header then the table header
            database_file.seek(108)
            cells_arr = [int.from_bytes(database_file.read(2), byteorder='big') for _ in range(number_of_tables)]
            sqlite_schema_rows = []
            for cell_pointer in cells_arr:
                database_file.seek(cell_pointer)
                record_size = parse_varint(database_file)
                rowid = parse_varint(database_file)
                record = parse_record(database_file)
                # Table contains columns: type, name, tbl_name, rootpage, sql
                sqlite_schema_rows.append(
                    {
                        "type": record[0],
                        "name": record[1],
                        "tbl_name": record[2],
                        "rootpage": record[3],
                        "sql": record[4],
                    }
                )
            print(" ".join([n["name"].decode("utf-8") for n in sqlite_schema_rows]))

    else:
        print(f"Invalid command: {command}")
