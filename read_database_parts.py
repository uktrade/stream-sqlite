from io import BytesIO

BYTEORDER = 'big'

INTERIOR_INDEX = 2
INTERIOR_TABLE = 5
LEAF_INDEX = 10
LEAF_TABLE = 13

SQLITE_SCHEMA_COLUMNS = {
    "type": "text",
    "name": "text",
    "tbl_name": "text",
    "rootpage": "integer",
    "sql_text": "text",
}


def print_buffer(title, buffer, size):
    print(title)
    for i in range(0,size):
        print(f" byte {i} = {buffer[i]} {hex(buffer[i])} {chr(buffer[i])} ")

def _read_one(stream):
    """Read a byte from the file (as an integer)
    raises EOFError if the stream ends while reading bytes.
    """
    c = stream.read(1)
    if c == b'':
        raise EOFError("Unexpected EOF while reading bytes")
    return ord(c)

def decode_stream(stream):
    """Read a varint from `stream`"""
    shift = 0
    result = 0
    while True:
        i = _read_one(stream)
        result |= (i & 0x7f) << shift
        shift += 7
        if not (i & 0x80):
            break

    return int(result)


def read_varint_from_file(fptr):
    return decode_stream(fptr)


def convert_bytes_to_int(byte_array, start, size):
    return int.from_bytes(byte_array[start:start+size], byteorder=BYTEORDER)


# https://www.sqlite.org/fileformat.html
def read_database_header_from_file(fptr):
    buffer = fptr.read(100)
    # print_buffer("Database header", buffer, 100)
    page_size = convert_bytes_to_int(buffer, 16, 2)
    return page_size


def read_page_header_from_file(fptr):
    buffer = fptr.read(8)
    # print_buffer("page header", buffer, 8)
    page_type = buffer[0]
    if page_type == INTERIOR_INDEX or page_type == INTERIOR_TABLE:
        buffer1 = fptr.read(4)
        right_most_pointer = convert_bytes_to_int(buffer1, 0, 4)
    elif page_type == LEAF_INDEX or page_type == LEAF_TABLE:
        right_most_pointer = 0
    else:
        raise Exception("Invalid page.")
    content_start = convert_bytes_to_int(buffer, 5, 2)
    number_of_cells = convert_bytes_to_int(buffer, 3, 2)
    schema_format = convert_bytes_to_int(buffer, 44, 4)
    return page_type, content_start, number_of_cells, right_most_pointer, schema_format


def read_cell_index_from_file(fptr, howmany_cells):
    cell_index = []
    # 2 bytes for offsets
    buffer = fptr.read(howmany_cells * 2)
    for cell_ptr in range (0, howmany_cells):
            cell_index.append(convert_bytes_to_int(buffer, cell_ptr*2, 2))
    return cell_index


def read_btree_leaf_from_file(fptr):
    print("==================================")
    payload_size = read_varint_from_file(fptr)
    key = read_varint_from_file(fptr)
    print(f"payload_size = {payload_size}, key = {key}")

    buffer = fptr.read(payload_size)
    print(f"Record {key}, {buffer}")
    payload_stream = BytesIO(buffer)
    header_size = decode_stream(payload_stream)
    column_size = []
    for _ in range(1, header_size-1):
        a = decode_stream(payload_stream)
        if a > 12 and a % 2:
            #     string
            column_size.append((a-13)/2)
        elif a > 11 and (a % 2) == 0:
            #     string
            column_size.append((a-12)/2)
        else:
            column_size.append(a)
    start = 0
    print(f"header_size = {header_size}")
    row_list = []
    for size in column_size:
        column = payload_stream.read(int(size))
        row_list.append(column)
        print(f"size = {size} start = {start} column = {column}")
        start += size

    return row_list
    # print(f"column sizes = {column_size}")
    # print(payload_stream)

    # buffer = fptr.read(payload_size)
    # print_buffer("cell leaf", buffer, payload_size)
    # print(buffer)


def read_schema_cell(fptr):
    return read_btree_leaf_from_file(SQLITE_SCHEMA_COLUMNS, fptr)