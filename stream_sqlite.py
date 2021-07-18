from struct import Struct
import sqlite3


def stream_sqlite(sqlite_chunks, chunk_size=65536):
    INTERIOR_INDEX = b'\x02'
    INTERIOR_TABLE = b'\x05'
    LEAF_INDEX = b'\x0a'
    LEAF_TABLE = b'\x0d'

    unsigned_short = Struct('>H')
    unsigned_long = Struct('>L')

    def get_byte_readers(iterable):
        # Return functions to return bytes from the iterable
        # - _yield_all: yields chunks as they come up (often for a "body")
        # - _yield_num: yields chunks as the come up, up to a fixed number of bytes
        # - _get_num: returns a single `bytes` of a given length

        chunk = b''
        offset = 0
        it = iter(iterable)

        def _yield_all():
            nonlocal chunk
            nonlocal offset

            while True:
                if not chunk:
                    try:
                        chunk = next(it)
                    except StopIteration:
                        break
                prev_offset = offset
                prev_chunk = chunk
                to_yield = min(len(chunk) - offset, chunk_size)
                offset = (offset + to_yield) % len(chunk)
                chunk = chunk if offset else b''
                yield prev_chunk[prev_offset:prev_offset + to_yield]

        def _yield_num(num):
            nonlocal chunk
            nonlocal offset

            while num:
                if not chunk:
                    try:
                        chunk = next(it)
                    except StopIteration:
                        raise ValueError('Fewer bytes than expected in SQLite file') from None
                prev_offset = offset
                prev_chunk = chunk
                to_yield = min(num, len(chunk) - offset, chunk_size)
                offset = (offset + to_yield) % len(chunk)
                chunk = chunk if offset else b''
                num -= to_yield
                yield prev_chunk[prev_offset:prev_offset + to_yield]

        def _get_num(num):
            return b''.join(chunk for chunk in _yield_num(num))

        def _return_unused(unused):
            nonlocal chunk
            nonlocal offset
            if len(unused) <= offset:
                offset -= len(unused)
            else:
                chunk = unused + chunk[offset:]
                offset = 0

        return _yield_all, _yield_num, _get_num, _return_unused

    def get_chunk_readers(chunk):
        # Set of functions to read a chunk of bytes, which it itself made of
        # of variable length chunks. Maintains a pointer to the current index
        # of the main chunk
        p = 0

        def _get_num(num):
            nonlocal p
            p_orig = p
            p += num
            return chunk[p_orig:p_orig+num]

        def _get_varint():
            # This probably doesn't work with negative numbers
            nonlocal p
            
            value = 0
            high_bit = 1
            i = 0

            while high_bit and i < 9:
                high_bit = chunk[p] >> 7
                value = \
                    ((value << 8) + chunk[p + i]) if i == 8 else \
                    ((value << 7) + (chunk[p + i] & 0x7F))

                i += 1
                p += 1

            return value, i

        return _get_num, _get_varint

    def type_length(serial_type):
        return \
            1 if serial_type == 1 else \
            int((serial_type - 12)/2) if serial_type >= 12 and serial_type % 2 == 0 else \
            int((serial_type - 13)/2) if serial_type >= 13 and serial_type % 2 == 1 else \
            None

    def query_list_of_dicts(cur, sql):
        cols = None
        cur.execute(sql)
        rows = cur.fetchall()

        def dicts():
            nonlocal cols
            for row in rows:
                if cols is None:
                    cols = [d[0] for d in cur.description]
                yield {col: row[i] for i, col in enumerate(cols)}

        return list(dicts())

    def parse_master_table_cells(master_table_records):
        def schema(cur, table_name, sql):
             cur.execute(sql)
             return query_list_of_dicts(cur, "PRAGMA table_info('"+ table_name + "');")

        with sqlite3.connect(':memory:') as con:
            cur = con.cursor()

            return {
                record[1]: {
                    'columns': schema(cur, record[1].decode(), record[4].decode()),
                    'first_page': record[3]
                }
                for record in master_table_records
                if record[0] == b'table'
            }

    yield_all, yield_num, get_num, return_unused = get_byte_readers(sqlite_chunks)

    header = get_num(100)
    total_bytes = len(header)

    if header[:16] != b'SQLite format 3\0':
        raise ValueError('Magic SQLite header string not found')

    page_size, = unsigned_short.unpack(header[16:18])
    page_size = 65536 if page_size == 1 else page_size
    num_pages_expected, = unsigned_long.unpack(header[28:32])

    return_unused(header)

    def yield_page_nums_pages_readers(page_size, num_pages_expected):
        for page_num in range(1, num_pages_expected + 1):
            page_bytes = get_num(page_size)
            page_reader, _ = get_chunk_readers(page_bytes)
            if page_num == 1:
                page_reader(100)
            yield page_num, page_bytes, page_reader

    def yield_page_cells(page_nums_pages_readers):

        def _yield_cells(pointers):
            for i in range(0, len(pointers) - 1):
                cell_num_reader, cell_varint_reader = get_chunk_readers(page_bytes[pointers[i]:pointers[i + 1]])

                if page_type == LEAF_TABLE:
                    payload_size, _ = cell_varint_reader()
                    rowid, _ = cell_varint_reader()

                    header_remaining, header_varint_size = cell_varint_reader()
                    header_remaining -= header_varint_size

                    serial_types = []
                    while header_remaining:
                        h, v_size = cell_varint_reader()
                        serial_types.append(h)
                        header_remaining -= v_size

                    yield [
                        cell_num_reader(type_length(serial_type))
                        for serial_type in serial_types
                    ]

        for page_num, page_bytes, page_reader in page_nums_pages_readers:
            page_type = page_reader(1)
            first_free_block, num_cells, cell_content_start, num_frag_free = \
                Struct('>HHHB').unpack(page_reader(7))
            cell_content_start = 65536 if cell_content_start == 0 else cell_content_start
            right_most_pointer = \
                page_reader(4) if page_type in (INTERIOR_INDEX, INTERIOR_TABLE) else \
                None

            if first_free_block:
                raise ValueError('Freeblock found, but are not supported')
            pointers = Struct('>{}H'.format(num_cells)).unpack(page_reader(num_cells * 2)) + (page_size,)

            yield page_num, [
                cell
                for cell in _yield_cells(pointers)
            ]

    page_nums_pages_readers = yield_page_nums_pages_readers(page_size, num_pages_expected)
    page_cells = yield_page_cells(page_nums_pages_readers)

    master_table_records = []
    for page_num, cells in page_cells:
        if page_num == 1:
            master_table_records = parse_master_table_cells(cells)
        else:
            table_name = list(master_table_records.keys())[0]
            yield table_name, [
                {
                    master_table_records[table_name]['columns'][i]['name']: value
                    for i, value in enumerate(cell)
                }
                for cell in cells
            ]

    extra = False
    for _ in yield_all():
        extra = True

    if extra:
        raise Exception('More bytes than expected in SQLite file')
