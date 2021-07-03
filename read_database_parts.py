from io import BytesIO

BYTEORDER = 'big'

INTERIOR_INDEX = 2
INTERIOR_TABLE = 5
LEAF_INDEX = 10
LEAF_TABLE = 13


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

    return result

def decode_bytes(buf):
    """Read a varint from from `buf` bytes"""
    return decode_stream(BytesIO(buf))



def read_varint_from_buffer(buffer):
    return decode_bytes(buffer)

    # https://sqlite.org/src4/doc/trunk/www/varint.wiki
    A0 = buffer.pop(0)
    if A0 <= 240:
        value = A0
    elif (A0 > 240 and A0 < 249):
        A1 = buffer.pop(0)
        value = 240 + 256 * (A0 - 251) + A1
    elif A0 == 249:
        A1 = buffer.pop(0)
        A2 = buffer.pop(0)
        value = 2288 + 256 * A1 + A2
    # elif A0 >= 250:
    #     value_size = 3 + A0 - 250
    #     buffer = fptr.read(value_size)
    #     value = int.from_bytes(buffer[0: value_size], byteorder=BYTEORDER)
    else:
        raise Exception("Invalid varint size")

    return int(value)

def read_varint_from_file(fptr):
    return decode_stream(fptr)
    # https://sqlite.org/src4/doc/trunk/www/varint.wiki
    buffer = fptr.read(1)
    A0 = buffer[0]
    if A0 <= 240:
        value = A0
    elif (A0 > 240 and A0 < 249):
        buffer = fptr.read(1)
        A1 = buffer[0]
        value = 240 + 256 * (A0 - 251) + A1
    elif A0 == 249:
        buffer = fptr.read(2)
        A1 = buffer[0]
        A2 = buffer[1]
        value = 2288 + 256 * A1 + A2
    elif A0 >= 250:
        value_size = 3 + A0 - 250
        buffer = fptr.read(value_size)
        value = int.from_bytes(buffer[0: value_size], byteorder=BYTEORDER)
    else:
        raise Exception("Invalid varint size")

    return value

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


def read_btree_leaf_from_file(fptr):
    payload_size = read_varint_from_file(fptr)
    key = read_varint_from_file(fptr)
    print(f"payload_size = {payload_size}, key = {key}")
    buffer = fptr.read(payload_size)
    # print_buffer("payload_buffer", buffer, payload_size)
    payload_buffer = bytearray(buffer)

    header_size = read_varint_from_buffer(payload_buffer)
    column_size = []
    for i in range(0, header_size-1):
        a = read_varint_from_buffer(payload_buffer[i:])
        print(f"Read a = {a}")
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
    for size in column_size:
        column = payload_buffer[int(start):int(start+size)]
        print(f"size = {size} start = {start} column = {column}")
        start += size

    # print(f"column sizes = {column_size}")
    print(payload_buffer)
    # header_size = read_varint_from_file(fptr)
    # print(f"Header size = {header_size}")
    #
    # column_size = []
    # for i in range(0, header_size-1):
    #     a = read_varint_from_file(fptr)
    #     if a > 12 and a % 2:
    #         #     string
    #         column_size.append((a-13)/2)
    #     elif a > 11 and (a % 2) == 0:
    #         #     string
    #         column_size.append((a-12)/2)
    #     else:
    #         column_size.append(a)
    #
    # print(f"column sizes = {column_size}")
    # header_buffer = fptr.read(header_size - 1)
    # print_buffer("Header", header_buffer, header_size - 1)

    # buffer = fptr.read(payload_size)
    # print_buffer("cell leaf", buffer, payload_size)
    # print(buffer)
