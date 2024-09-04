import sys

from dataclasses import dataclass

# import sqlparse - available if you need it!
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



database_file_path = sys.argv[1]
command = sys.argv[2]


if command == ".dbinfo":
    with open(database_file_path, "rb") as database_file:
        # You can use print statements as follows for debugging, they'll be visible when running tests.
        print("Logs from your program will appear here!")

        database_file.seek(16)  # Skip the first 16 bytes of the header
        page_size = int.from_bytes(database_file.read(2), byteorder="big")
        print(f"database page size: {page_size}")
        database_file.seek(103)
        number_of_tables = int.from_bytes(database_file.read(2), byteorder='big')
        print(f"number of tables: {number_of_tables}")
elif command == '.tables':
     with open(database_file_path, "rb") as database_file:
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
