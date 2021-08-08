from collections import deque, namedtuple
from functools import partial
from itertools import groupby
from math import ceil
from struct import Struct
from sqlite3 import connect


def stream_sqlite(sqlite_chunks, max_buffer_size):
    LEAF_INDEX = b'\x0a'
    LEAF_TABLE = b'\x0d'

    unsigned_char = Struct('B')
    unsigned_short = Struct('>H')
    unsigned_long = Struct('>L')
    double = Struct('>d')
    leaf_header = Struct('>HHHB')
    interior_header = Struct('>HHHBL')
    freelist_trunk_header = Struct('>LL')

    master_row_constructor = namedtuple('MasterRow', ('rowid', 'type', 'name', 'tbl_name', 'rootpage', 'sql'))
    column_constructor = namedtuple('Column', ('cid', 'name', 'type', 'notnull', 'dflt_value', 'pk'))

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
                to_yield = min(num, len(chunk) - offset)
                yield chunk[offset:offset + to_yield]
                num -= to_yield
                offset = (offset + to_yield) % len(chunk)
                chunk = chunk if offset else b''

        def _get_num(num):
            return b''.join(chunk for chunk in _yield_num(num))

        def _finish():
            try:
                while True:
                    next(it)
            except StopIteration:
                pass

        return _get_num, _finish

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
            nonlocal p

            value = 0
            high_bit = 1
            i = 0

            while high_bit and i < 9:
                high_bit = chunk[p] & 0x80
                value = \
                    ((value << 8) + chunk[p]) if i == 8 else \
                    ((value << 7) + (chunk[p] & 0x7F))

                i += 1
                p += 1

            is_negative = value & 0x8000000000000000
            value = \
                value if not is_negative else \
                -1 * (~(value - 1) & 0xFFFFFFFFFFFFFFFF)

            return value, i

        return _get_num, _get_varint

    def parse_header(header):
        if header[:16] != b'SQLite format 3\0':
            raise ValueError('SQLite header not found at start of stream')

        encoding, = unsigned_long.unpack(header[56:60])
        # 0 if the database is empty. This is not documented at https://www.sqlite.org/fileformat.html
        if encoding not in (0, 1):
            raise ValueError('Unsupported encoding')

        reserved_space, = unsigned_char.unpack(header[20:21])
        if reserved_space != 0:
            raise ValueError('Reserved space is not supported')

        page_size, = unsigned_short.unpack(header[16:18])
        page_size = 65536 if page_size == 1 else page_size
        num_pages_expected, = unsigned_long.unpack(header[28:32])
        first_freelist_trunk_page, = unsigned_long.unpack(header[32:36])
        incremental_vacuum = any(header[52:56])

        return page_size, num_pages_expected, first_freelist_trunk_page, incremental_vacuum

    def yield_page_nums_pages_readers(get_bytes, page_size, num_pages_expected, incremental_vacuum):
        page_bytes = bytes(100) + get_bytes(page_size - 100)
        page_reader, _ = get_chunk_readers(page_bytes)
        page_reader(100)

        lock_byte_page = 1073741824 // page_size + 1
        ptrmap_j = int(ceil(page_size/5))

        yield 1, page_bytes, page_reader

        for page_num in range(2, num_pages_expected + 1):
            page_bytes = get_bytes(page_size)
            page_reader, _ = get_chunk_readers(page_bytes)

            # ptrmap and lock bytes pages are not processed in any way
            ptrmap_page_prev = incremental_vacuum and (page_num - 1 - 2) % ptrmap_j == 0
            ptrmap_page_curr = incremental_vacuum and (page_num - 2) % ptrmap_j == 0
            lock_byte_page_prev = (page_num - 1) == lock_byte_page
            lock_byte_page_curr = page_num == lock_byte_page

            # Keeping below separate so code coverage reveals which are not tested
            if ptrmap_page_curr:
                continue

            if lock_byte_page_curr:
                continue

            if (ptrmap_page_prev and lock_byte_page_prev):
                continue

            yield page_num, page_bytes, page_reader

    def yield_table_rows(page_nums_pages_readers, first_freelist_trunk_page):
        # Map of page number -> bytes. Populated when we reach a page that
        # can't be identified and so can't be processed.
        page_buffer = {}

        # Map of page number -> page processor function. Populated when we
        # identify a page, but don't have it in the buffer yet.
        page_processors = {}

        # Bytes currently in the page_buffer, and all of the deques that store
        # overflow pages in the partially applied page_processors
        num_bytes_buffered = 0

        def note_increase_buffered(num_bytes):
            nonlocal num_bytes_buffered
            num_bytes_buffered += num_bytes
            if num_bytes_buffered > max_buffer_size:
                raise ValueError('SQLite file requires a larger max_buffer_size')

        def note_decrease_buffered(num_bytes):
            nonlocal num_bytes_buffered
            num_bytes_buffered -= num_bytes

        def process_initial_payload(initial_payload_size,
                                    full_payload_size, full_payload_processor,
                                    cell_num_reader, cell_varint_reader):
            if initial_payload_size == full_payload_size:
                yield from full_payload_processor(cell_num_reader, cell_varint_reader)

            else:
                initial_payload = cell_num_reader(initial_payload_size)
                overflow_page, = unsigned_long.unpack(cell_num_reader(4))
                payload_chunks = deque()
                note_increase_buffered(len(initial_payload))
                payload_chunks.append(initial_payload)
                payload_remainder = full_payload_size - initial_payload_size

                yield from process_if_buffered_or_remember(partial(
                    process_overflow_page,
                    full_payload_processor, payload_chunks, payload_remainder
                ), overflow_page)

        def process_overflow_page(full_payload_processor, payload_chunks, payload_remainder, page_bytes, page_reader):
            next_overflow_page, = unsigned_long.unpack(page_reader(4))
            num_this_page = min(payload_remainder, len(page_bytes) - 4)
            payload_remainder -= num_this_page
            note_increase_buffered(num_this_page)
            payload_chunks.append(page_reader(num_this_page))

            if not next_overflow_page:
                payload = b''.join(payload_chunks)
                note_decrease_buffered(len(payload))
                yield from full_payload_processor(*get_chunk_readers(payload))

            else:
                yield from process_if_buffered_or_remember(partial(
                    process_overflow_page,
                    full_payload_processor, payload_chunks, payload_remainder
                ), next_overflow_page)

        def process_table_page(table_name, table_info, row_constructor, page_bytes, page_reader):

            def get_table_initial_payload_size(p):
                u = len(page_bytes)
                x = u - 35
                m = 32 * (u - 12) // 255 - 23
                k = m + (p - m) % (u - 4)

                return \
                    p if p <= x else \
                    k if k <= x else \
                    m

            def read_table_row(rowid, cell_num_reader, cell_varint_reader):

                def serial_types(header_remaining, cell_varint_reader):
                    while header_remaining:
                        h, v_size = cell_varint_reader()
                        yield h
                        header_remaining -= v_size

                header_remaining, header_varint_size = cell_varint_reader()
                header_remaining -= header_varint_size

                return row_constructor(rowid, *tuple(
                    parser(cell_num_reader(length))
                    for serial_type in tuple(serial_types(header_remaining, cell_varint_reader))
                    for (length, parser) in (
                        (0, lambda _: None) if serial_type == 0 else \
                        (1, lambda raw: int.from_bytes(raw, byteorder='big', signed=True)) if serial_type == 1 else \
                        (2, lambda raw: int.from_bytes(raw, byteorder='big', signed=True)) if serial_type == 2 else \
                        (3, lambda raw: int.from_bytes(raw, byteorder='big', signed=True)) if serial_type == 3 else \
                        (4, lambda raw: int.from_bytes(raw, byteorder='big', signed=True)) if serial_type == 4 else \
                        (6, lambda raw: int.from_bytes(raw, byteorder='big', signed=True)) if serial_type == 5 else \
                        (8, lambda raw: int.from_bytes(raw, byteorder='big', signed=True)) if serial_type == 6 else \
                        (8, lambda raw: double.unpack(raw)[0]) if serial_type == 7 else \
                        (0, lambda _: 0) if serial_type == 8 else \
                        (0, lambda _: 1) if serial_type == 9 else \
                        (int((serial_type - 12)/2), lambda raw: raw) if serial_type % 2 == 0 else \
                        (int((serial_type - 13)/2), lambda raw: raw.decode()) if serial_type % 2 == 1 else \
                        (None, None),
                    )
                ))

            def process_table_leaf_master():

                def table_info_and_row_constructor(cur, master_row):
                    name, sql = \
                        ('_' + master_row.name, 'CREATE TABLE _' + master_row.name + master_row.sql[13 + len(master_row.name):]) if master_row.name.startswith('sqlite_') else \
                        (master_row.name, master_row.sql)
                    cur.execute(sql)
                    cur.execute("PRAGMA table_info('" + name.replace("'","''") + "');")
                    columns = tuple(column_constructor(*column) for column in cur.fetchall())
                    integer_primary_key_indexes = tuple(i for i, column in enumerate(columns) if column.pk and column.type.lower() == 'integer')
                    rowid_alias_index = integer_primary_key_indexes[0] if len(integer_primary_key_indexes) == 1 else None
                    row_namedtuple = namedtuple('Row', tuple(column.name for column in columns))
                    row_constructor = lambda rowid, *values: row_namedtuple(*tuple((rowid if i == rowid_alias_index else value) for i, value in enumerate(values)))

                    return columns, row_constructor

                def process_master_leaf_row(rowid, cell_num_reader, cell_varint_reader):
                    master_row = read_table_row(rowid, cell_num_reader, cell_varint_reader)
                    yield from \
                        process_if_buffered_or_remember(partial(process_table_page, master_row.name, *table_info_and_row_constructor(cur, master_row)), master_row.rootpage) if master_row.type == 'table' else \
                        process_if_buffered_or_remember(process_index_page, master_row.rootpage) if master_row.type == 'index' else \
                        ()

                _, num_cells, _, _ = leaf_header.unpack(page_reader(7))

                with connect(':memory:') as con:
                    cur = con.cursor()

                    pointers = unsigned_short.iter_unpack(page_reader(num_cells * 2))

                    for pointer, in pointers:
                        cell_num_reader, cell_varint_reader = get_chunk_readers(page_bytes, pointer)
                        full_payload_size, _ = cell_varint_reader()
                        rowid, _ = cell_varint_reader()
                        initial_payload_size = get_table_initial_payload_size(full_payload_size)
                        yield from process_initial_payload(
                            initial_payload_size, full_payload_size, partial(process_master_leaf_row, rowid),
                            cell_num_reader, cell_varint_reader,
                        )

            def process_table_leaf_non_master():

                def process_non_master_leaf_row(rowid, cell_num_reader, cell_varint_reader):
                    yield table_name, table_info, read_table_row(rowid, cell_num_reader, cell_varint_reader)

                _, num_cells, _, _ = leaf_header.unpack(page_reader(7))

                pointers = unsigned_short.iter_unpack(page_reader(num_cells * 2))

                for pointer, in pointers:
                    cell_num_reader, cell_varint_reader = get_chunk_readers(page_bytes, pointer)
                    full_payload_size, _ = cell_varint_reader()
                    rowid, _ = cell_varint_reader()
                    initial_payload_size = get_table_initial_payload_size(full_payload_size)

                    yield from process_initial_payload(
                        initial_payload_size, full_payload_size, partial(process_non_master_leaf_row, rowid),
                        cell_num_reader, cell_varint_reader,
                    )

            def process_table_interior():
                _, num_cells, _, _, right_most_pointer = \
                    interior_header.unpack(page_reader(11))

                pointers = unsigned_short.iter_unpack(page_reader(num_cells * 2))

                for pointer, in pointers:
                    cell_num_reader, cell_varint_reader = get_chunk_readers(page_bytes, pointer)
                    page_number, =  unsigned_long.unpack(cell_num_reader(4))
                    yield from process_if_buffered_or_remember(
                        partial(process_table_page, table_name, table_info, row_constructor), page_number)

                yield from process_if_buffered_or_remember(
                    partial(process_table_page, table_name, table_info, row_constructor), right_most_pointer)

            page_type = page_reader(1)
            yield from \
                process_table_leaf_master() if page_type == LEAF_TABLE and table_name == 'sqlite_schema' else \
                process_table_leaf_non_master() if page_type == LEAF_TABLE else \
                process_table_interior()

        def process_index_page(page_bytes, page_reader):

            def get_index_initial_payload_size(p):
                u = len(page_bytes)
                x = 64 * (u - 12) // 255 - 23
                m = 32 * (u - 12) // 255 - 23
                k = m + (p - m) % (u - 4)

                return \
                    p if p <= x else \
                    k if k <= x else \
                    m

            def process_index_leaf():

                def process_index_leaf_row(cell_num_reader, cell_varint_reader):
                    yield from ()

                _, num_cells, _, _ = leaf_header.unpack(page_reader(7))

                pointers = unsigned_short.iter_unpack(page_reader(num_cells * 2))

                for pointer, in pointers:
                    cell_num_reader, cell_varint_reader = get_chunk_readers(page_bytes, pointer)
                    full_payload_size, _ = cell_varint_reader()
                    initial_payload_size = get_index_initial_payload_size(full_payload_size)

                    yield from process_initial_payload(
                        initial_payload_size, full_payload_size, process_index_leaf_row,
                        cell_num_reader, cell_varint_reader,
                    )

            def process_index_interior():

                def process_index_interior_row(cell_num_reader, cell_varint_reader):
                    yield from ()

                _, num_cells, _, _, right_most_pointer = \
                    interior_header.unpack(page_reader(11))

                pointers = unsigned_short.iter_unpack(page_reader(num_cells * 2))

                for pointer, in pointers:
                    cell_num_reader, cell_varint_reader = get_chunk_readers(page_bytes, pointer)
                    page_num, =  unsigned_long.unpack(cell_num_reader(4))
                    yield from process_if_buffered_or_remember(process_index_page, page_num)

                    full_payload_size, _ = cell_varint_reader()
                    initial_payload_size = get_index_initial_payload_size(full_payload_size)
                    yield from process_initial_payload(
                        initial_payload_size, full_payload_size, process_index_interior_row,
                        cell_num_reader, cell_varint_reader,
                    )

                yield from process_if_buffered_or_remember(process_index_page, right_most_pointer)

            page_type = page_reader(1)
            yield from \
                process_index_leaf() if page_type == LEAF_INDEX else \
                process_index_interior()

        def process_freelist_trunk_page(page_bytes, page_reader):
            next_trunk, num_leaves = freelist_trunk_header.unpack(page_reader(8))
            leaf_pages = unsigned_long.iter_unpack(page_reader(num_leaves * 4))

            for page_num, in leaf_pages:
                yield from process_if_buffered_or_remember(process_freelist_leaf_page, page_num)

            if next_trunk != 0:
                yield from process_if_buffered_or_remember(process_freelist_trunk_page, next_trunk)

        def process_freelist_leaf_page(page_bytes, page_reader):
            yield from ()

        def process_if_buffered_or_remember(process, page_num):
            try:
                page_bytes, page_reader = page_buffer.pop(page_num)
            except KeyError:
                page_processors[page_num] = process
            else:
                note_decrease_buffered(len(page_bytes))
                yield from process(page_bytes, page_reader)

        page_processors[1] = partial(process_table_page, 'sqlite_schema', (), master_row_constructor)

        if first_freelist_trunk_page:
            page_processors[first_freelist_trunk_page] = process_freelist_trunk_page

        for page_num, page_bytes, page_reader in page_nums_pages_readers:
            try:
                process_page = page_processors.pop(page_num)
            except KeyError:
                note_increase_buffered(len(page_bytes))
                page_buffer[page_num] = (page_bytes, page_reader)
            else:
                yield from process_page(page_bytes, page_reader)

        if num_bytes_buffered != 0:
            raise ValueError('Bytes remain in cache')

        if len(page_processors) != 0:
            raise ValueError("Expected a page that wasn't processed")

    get_bytes, finish = get_byte_reader(sqlite_chunks)
    page_size, num_pages_expected, first_freelist_trunk_page, incremental_vacuum = parse_header(get_bytes(100))

    page_nums_pages_readers = yield_page_nums_pages_readers(get_bytes, page_size, num_pages_expected, incremental_vacuum)
    table_rows_including_internal = yield_table_rows(page_nums_pages_readers, first_freelist_trunk_page)
    table_rows = ((table_name, table_info, row) for table_name, table_info, row in table_rows_including_internal if not table_name.startswith('sqlite_'))
    grouped_by_table = groupby(table_rows, key=lambda name_info_row: (name_info_row[0], name_info_row[1]))

    for (name, info), single_table_rows in grouped_by_table:
        yield name, info, (row for (_, _, row) in single_table_rows)

    finish()
