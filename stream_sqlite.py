from struct import Struct
import sqlite3


def stream_sqlite(sqlite_chunks, chunk_size=65536):
    INTERIOR_INDEX = b'\x02'
    INTERIOR_TABLE = b'\x05'
    LEAF_INDEX = b'\x0a'
    LEAF_TABLE = b'\x0d'

    signed_char = Struct('b')
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

        return _yield_all, _yield_num, _get_num

    def get_chunk_readers(chunk, p=0):
        # Set of functions to read a chunk of bytes, which it itself made of
        # of variable length chunks. Maintains a pointer to the current index
        # of the main chunk

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
                    ((value << 8) + chunk[p]) if i == 8 else \
                    ((value << 7) + (chunk[p] & 0x7F))

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

    def parse_serial_value(serial_type, raw):
        if serial_type == 1:
            return signed_char.unpack(raw)[0]
        else:
            return raw

    yield_all, yield_num, get_num = get_byte_readers(sqlite_chunks)

    header = get_num(100)

    if header[:16] != b'SQLite format 3\0':
        raise ValueError('SQLite header not found at start of stream')

    page_size, = unsigned_short.unpack(header[16:18])
    page_size = 65536 if page_size == 1 else page_size
    num_pages_expected, = unsigned_long.unpack(header[28:32])

    def yield_page_nums_pages_readers(page_size, num_pages_expected):
        page_bytes = header + get_num(page_size - 100)
        page_reader, _ = get_chunk_readers(page_bytes)
        page_reader(100)

        yield 1, page_bytes, page_reader

        for page_num in range(2, num_pages_expected + 1):
            page_bytes = get_num(page_size)
            page_reader, _ = get_chunk_readers(page_bytes)

            yield page_num, page_bytes, page_reader

    def yield_tables(page_nums_pages_readers):
        cached_pages = {}
        known_table_pages = {
            1: 'sqlite_schema',
        }
        master_table = {}

        def _yield_leaf_table_cells(page_bytes, pointers):
            for pointer in pointers:
                cell_num_reader, cell_varint_reader = get_chunk_readers(page_bytes, pointer)

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
                    parse_serial_value(serial_type, cell_num_reader(type_length(serial_type)))
                    for serial_type in serial_types
                ]

        def _yield_interior_table_cells(page_bytes, pointers):
            for pointer in pointers:
                cell_num_reader, cell_varint_reader = get_chunk_readers(page_bytes, pointer)
                page_number, =  unsigned_long.unpack(cell_num_reader(4))
                yield page_number

        def try_process_cached(table_name, page_nums):
            for page_num in page_nums:
                try:
                    cached_page_bytes, cached_page_reader = cached_pages.pop(page_num)
                except KeyError:
                    pass
                else:
                    yield from process_table_page(table_name, cached_page_bytes, cached_page_reader)

        def process_table_page(table_name, page_bytes, page_reader):
            page_type = page_reader(1)
            first_free_block, num_cells, cell_content_start, num_frag_free = \
                Struct('>HHHB').unpack(page_reader(7))
            cell_content_start = 65536 if cell_content_start == 0 else cell_content_start
            right_most_pointer = \
                page_reader(4) if page_type == INTERIOR_TABLE else \
                None

            pointers = Struct('>{}H'.format(num_cells)).unpack(page_reader(num_cells * 2))

            if page_type == LEAF_TABLE and table_name == 'sqlite_schema':
                for row in get_master_table(_yield_leaf_table_cells(page_bytes, pointers)):
                    master_table[row['name']] = row['info']
                    known_table_pages[row['root_page']] = row['name']
                    yield from try_process_cached(row['name'], [row['root_page']])

            elif page_type == LEAF_TABLE:
                table_info = master_table[table_name]
                yield table_name, table_info, [
                    {
                        table_info[i]['name']: value
                        for i, value in enumerate(cell)
                    }
                    for cell in _yield_leaf_table_cells(page_bytes, pointers)
                ]

            elif page_type == INTERIOR_TABLE:
                for page_num in _yield_interior_table_cells(page_bytes, pointers):
                    known_table_pages[page_num] = table_name
                    yield from try_process_cached(table_name, [page_num])

            else:
                raise Exception('Unhandled page type')

        for page_num, page_bytes, page_reader in page_nums_pages_readers:
            try:
                table_name = known_table_pages[page_num]
            except KeyError:
                cached_pages[page_num] = (page_bytes, page_reader)
            else:
                yield from process_table_page(table_name, page_bytes, page_reader)

    def get_master_table(master_cells):

        def schema(cur, table_name, sql):
            cur.execute(sql)
            cur.execute("PRAGMA table_info('" + table_name + "');")
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
            return [{col: row[i] for i, col in enumerate(cols)} for row in rows]

        with sqlite3.connect(':memory:') as con:
            cur = con.cursor()

            return [
                {
                    'name': cell[1].decode(),
                    'info': schema(cur, cell[1].decode(), cell[4].decode()),
                    'root_page': cell[3],
                }
                for cell in master_cells
                if cell[0] == b'table'
            ]

    page_nums_pages_readers = yield_page_nums_pages_readers(page_size, num_pages_expected)
    yield from yield_tables(page_nums_pages_readers)

    extra = False
    for _ in yield_all():
        extra = True

    if extra:
        raise ValueError('More bytes than expected in SQLite file')
