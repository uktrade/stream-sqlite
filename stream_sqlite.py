from functools import partial
from itertools import groupby
from struct import Struct, unpack
from sqlite3 import connect


def stream_sqlite(sqlite_chunks, chunk_size=65536):
    INTERIOR_INDEX = b'\x02'
    INTERIOR_TABLE = b'\x05'
    LEAF_INDEX = b'\x0a'
    LEAF_TABLE = b'\x0d'

    signed_char = Struct('b')
    unsigned_short = Struct('>H')
    unsigned_long = Struct('>L')
    table_header = Struct('>HHHB')
    freelist_trunk_header = Struct('>LL')

    def get_byte_reader(iterable):
        chunk = b''
        offset = 0
        it = iter(iterable)

        def _yield_num(num):
            nonlocal chunk
            nonlocal offset

            while num:
                if not chunk:
                    try:
                        chunk = next(it)
                    except StopIteration:
                        raise ValueError('Fewer bytes than expected in SQLite stream') from None
                prev_offset = offset
                prev_chunk = chunk
                to_yield = min(num, len(chunk) - offset, chunk_size)
                offset = (offset + to_yield) % len(chunk)
                chunk = chunk if offset else b''
                num -= to_yield
                yield prev_chunk[prev_offset:prev_offset + to_yield]

        def _get_num(num):
            return b''.join(chunk for chunk in _yield_num(num))

        return _get_num

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

    def parse_header(header):
        if header[:16] != b'SQLite format 3\0':
            raise ValueError('SQLite header not found at start of stream')

        page_size, = unsigned_short.unpack(header[16:18])
        page_size = 65536 if page_size == 1 else page_size
        num_pages_expected, = unsigned_long.unpack(header[28:32])
        first_freelist_trunk_page, = unsigned_long.unpack(header[32:36])

        return page_size, num_pages_expected, first_freelist_trunk_page

    def yield_page_nums_pages_readers(get_bytes, page_size, num_pages_expected):
        page_bytes = bytes(100) + get_bytes(page_size - 100)
        page_reader, _ = get_chunk_readers(page_bytes)
        page_reader(100)

        yield 1, page_bytes, page_reader

        for page_num in range(2, num_pages_expected + 1):
            page_bytes = get_bytes(page_size)
            page_reader, _ = get_chunk_readers(page_bytes)

            yield page_num, page_bytes, page_reader

    def yield_table_pages(page_nums_pages_readers, first_freelist_trunk_page):
        page_buffer = {}
        page_types = {}
        master_table = {}

        def process_table_page(table_name, page_bytes, page_reader):

            def type_length(serial_type):
                return \
                    1 if serial_type == 1 else \
                    int((serial_type - 12)/2) if serial_type >= 12 and serial_type % 2 == 0 else \
                    int((serial_type - 13)/2) if serial_type >= 13 and serial_type % 2 == 1 else \
                    None

            def parse_serial_value(serial_type, raw):
                return \
                    signed_char.unpack(raw)[0] if serial_type == 1 else \
                    raw

            def yield_leaf_table_cells(page_bytes, pointers):
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

            def yield_interior_table_cells(page_bytes, pointers):
                for pointer in pointers:
                    cell_num_reader, cell_varint_reader = get_chunk_readers(page_bytes, pointer)
                    page_number, =  unsigned_long.unpack(cell_num_reader(4))
                    yield page_number

            def get_master_table(master_cells):

                def schema(cur, table_name, sql):
                    cur.execute(sql)
                    cur.execute("PRAGMA table_info('" + table_name + "');")
                    rows = cur.fetchall()
                    cols = [d[0] for d in cur.description]
                    return [{col: row[i] for i, col in enumerate(cols)} for row in rows]

                with connect(':memory:') as con:
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

            def process_if_buffered_or_remember(table_name, page_num):
                try:
                    page_bytes, page_reader = page_buffer.pop(page_num)
                except KeyError:
                    page_types[page_num] = partial(process_table_page, table_name)
                else:
                    yield from process_table_page(table_name, page_bytes, page_reader)

            page_type = page_reader(1)
            first_free_block, num_cells, cell_content_start, num_frag_free = \
                table_header.unpack(page_reader(7))
            cell_content_start = 65536 if cell_content_start == 0 else cell_content_start
            right_most_pointer, = \
                unsigned_long.unpack(page_reader(4)) if page_type == INTERIOR_TABLE else \
                (None,)

            pointers = unpack('>{}H'.format(num_cells), page_reader(num_cells * 2))

            if page_type == LEAF_TABLE and table_name == 'sqlite_schema':
                for row in get_master_table(yield_leaf_table_cells(page_bytes, pointers)):
                    master_table[row['name']] = row['info']
                    yield from process_if_buffered_or_remember(row['name'], row['root_page'])

            elif page_type == LEAF_TABLE:
                table_info = master_table[table_name]
                yield table_name, table_info, (
                    {
                        table_info[i]['name']: value
                        for i, value in enumerate(cell)
                    }
                    for cell in yield_leaf_table_cells(page_bytes, pointers)
                )

            elif page_type == INTERIOR_TABLE:
                for page_num in yield_interior_table_cells(page_bytes, pointers):
                    yield from process_if_buffered_or_remember(table_name, page_num)
                yield from process_if_buffered_or_remember(table_name, right_most_pointer)

            else:
                raise ValueError('Unhandled page type in SQLite stream')

        def process_freelist_trunk_page(page_bytes, page_reader):
            def trunk_process_if_buffered_or_remember(page_num):
                try:
                    page_bytes, page_reader = page_buffer.pop(page_num)
                except KeyError:
                    page_types[page_num] = process_freelist_trunk_page
                else:
                    yield from process_freelist_trunk_page(page_bytes, page_reader)

            def leaf_process_if_buffered_or_remember(page_num):
                try:
                    del page_buffer[page_num]
                except KeyError:
                    page_types[page_num] = process_freelist_leaf_page

            next_trunk, num_leaves = freelist_trunk_header.unpack(page_reader(8))
            leaf_pages = unpack('>{}L'.format(num_leaves), page_reader(num_leaves * 4))

            for page_num in leaf_pages:
                leaf_process_if_buffered_or_remember(page_num)

            if next_trunk != 0:
                yield from trunk_process_if_buffered_or_remember(next_trunk)

        def process_freelist_leaf_page(page_bytes, page_reader):
            yield from []

        page_types[1] = partial(process_table_page, 'sqlite_schema')
        page_types[first_freelist_trunk_page] = process_freelist_trunk_page

        for page_num, page_bytes, page_reader in page_nums_pages_readers:
            try:
                process_page = page_types.pop(page_num)
            except KeyError:
                page_buffer[page_num] = (page_bytes, page_reader)
            else:
                yield from process_page(page_bytes, page_reader)

        if len(page_buffer) != 0:
            raise ValueError('Unidentified pages in buffer')

    def group_by_table(table_pages):
        grouped_by_name = groupby(
            table_pages, key=lambda name_info_pages: (name_info_pages[0], name_info_pages[1]))

        def _rows(single_table_pages):
            for _, _, table_rows in single_table_pages:
                yield from table_rows

        for (name, info), single_table_pages in grouped_by_name:
            yield name, info, _rows(single_table_pages)

    get_bytes = get_byte_reader(sqlite_chunks)
    page_size, num_pages_expected, first_freelist_trunk_page = parse_header(get_bytes(100))

    page_nums_pages_readers = yield_page_nums_pages_readers(get_bytes, page_size, num_pages_expected)
    table_pages = yield_table_pages(page_nums_pages_readers, first_freelist_trunk_page)
    yield from group_by_table(table_pages)
