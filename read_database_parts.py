import io

BYTEORDER = 'big'

INTERIOR_INDEX = 2
INTERIOR_TABLE = 5
LEAF_INDEX = 10
LEAF_TABLE = 13

PAGE_SIZE = 4096

SQLITE_SCHEMA_COLUMNS = {
    "type": "text",
    "name": "text",
    "tbl_name": "text",
    "rootpage": "integer",
    "sql_text": "text",
}


class DatabaseHandler():
    def __init__(self, path):
        self.db_path = path
        
    def get_page(self,page_number):
        with open(self.db_path, "rb") as fptr:
            database_pointer = fptr
            database_pointer.seek((page_number - 1) * PAGE_SIZE)
            buffer = database_pointer.read(PAGE_SIZE)
        fptr.close
        return io.BytesIO(buffer)


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

def decode_stream(stream, show_bite=False):
    """Read a varint from `stream`"""
    shift = 0
    result = 0
    shift = 7
    while True:
        i = _read_one(stream)
        if show_bite:
            print(f"byte = {i} {hex(i)}")
        result = ((result & 0x7f) << shift) + (i & 0x7f)
        if show_bite:
            print(f"result = {result} {hex(result)}, shift = {shift}")
        # shift += 7
        if not (i & 0x80):
            break
    return int(result)



def convert_bytes_to_int(byte_array, start, size):
    return int.from_bytes(byte_array[start:start+size], byteorder=BYTEORDER)


# https://www.sqlite.org/fileformat.html
def read_database_header_from_file(fptr):
    buffer = fptr.read(100)
    # print_buffer("Database header", buffer, 100)
    page_size = convert_bytes_to_int(buffer, 16, 2)
    return page_size


def read_page_header(fptr):
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
    return page_type, content_start, number_of_cells, right_most_pointer


def read_cell_index_from_page(fptr, howmany_cells):
    cell_index = []
    # 2 bytes for offsets
    buffer = fptr.read(howmany_cells * 2)
    for cell_ptr in range (0, howmany_cells):
            cell_index.append(convert_bytes_to_int(buffer, cell_ptr*2, 2))
    return cell_index

def read_internal_table_cell(fptr):
    buffer = fptr.read(4)
    left_child_pointer = convert_bytes_to_int(buffer, 0, 4)
    key = decode_stream(fptr)
    return left_child_pointer, key


def read_btree_internal_table(fptr, cell_index):
    # A 4-byte big-endian page number which is the left child pointer.
    # A varint which is the integer key
    page_index = []
    for start in cell_index:
        fptr.seek(start)
        left_child_pointer, key = read_internal_table_cell(fptr)
        page_index.append(left_child_pointer)
    return page_index



def read_btree_leaf_from_file(fptr):
    # print("==================================")
    payload_size = decode_stream(fptr)
    # print("========== READ key ==========")
    key = decode_stream(fptr)
    # print(f"========== key = {key} ==========")

    # print(f"payload_size = {payload_size}, key = {key}")
    header_start = fptr.tell()
    header_size = decode_stream(fptr)
    # print(f"header_size = {header_size}")

    data_start = header_start + header_size
    col_info = []
    while fptr.tell() < data_start :
        a = decode_stream(fptr)
        if a > 12 and a % 2:
            col_size = (a-13)/2
            type = "string"
        elif a > 11 and (a % 2) == 0:
            #     String
            col_size=(a-12)/2
            type = "blob"
        else:
            col_size = a
            type = "int"
        col_info.append((col_size, type))
    total = header_size
    row_data = [key]
    for size_type in col_info:
        size = int(size_type[0])
        total += size
        value = fptr.read(size)
        row_data.append(value)
    # print(f"data used = {total}")
    return row_data


def read_schema(path):
    database_info = DatabaseHandler(path)
    page1 = database_info.get_page(1)
    read_database_header_from_file(page1)
    read_page_from_block(page1, database_info)

def read_page(path, number):
    database_info = DatabaseHandler(path)
    page = database_info.get_page(number)
    read_page_from_block(page, database_info)


def read_page_from_block(fptr, database_info):
    # fptr is pointing at the beginning of the page
    page_type, \
    content_start, \
    number_of_cells, \
    right_most_pointer \
        = read_page_header(fptr)
    # print(f"page_type = {page_type}")
    # print(f"content_start = {content_start}")
    # print(f"number_of_cells = {number_of_cells}")
    # print(f"right_most_pointer = {right_most_pointer}")
    cell_index = read_cell_index_from_page(fptr, number_of_cells)
    # print(f"Cells addresses {cell_index}")
    # print(f"after reading {fptr.tell()}")
    if page_type == LEAF_TABLE:
        for cell_start in cell_index:
            fptr.seek(cell_start)
            row_list = read_btree_leaf_from_file(fptr)
            print(row_list)
    if page_type == INTERIOR_TABLE:
        fptr.seek(0)
        print(f"after page {fptr.tell()}")
        page_index = read_btree_internal_table(fptr, cell_index)
        print (f"page_index = {page_index}")
        for page in page_index:
            page_buffer = database_info.get_page(page)
            # print(f"====== Reading page {page}")
            read_page_from_block(page_buffer, database_info)

        if right_most_pointer:
            page_buffer = database_info.get_page(right_most_pointer)
            read_page_from_block(page_buffer, database_info)



