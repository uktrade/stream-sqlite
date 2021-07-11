from struct import Struct


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

    yield_all, yield_num, get_num, return_unused = get_byte_readers(sqlite_chunks)

    header = get_num(100)
    total_bytes = len(header)

    if header[:16] != b'SQLite format 3\0':
        raise ValueError('Magic SQLite header string not found')

    yield header

    page_size, = unsigned_short.unpack(header[16:18])
    page_size = 65536 if page_size == 1 else page_size
    num_pages_expected, = unsigned_long.unpack(header[28:32])

    return_unused(header)

    for page_num in range(1, num_pages_expected + 1):
        page_bytes = get_num(page_size)
        page_num_reader, _ = get_chunk_readers(page_bytes)
        if page_num == 1:
            page_num_reader(100)

        page_type = page_num_reader(1)
        first_free_block, num_cells, cell_content_start, num_frag_free = \
            Struct('>HHHB').unpack(page_num_reader(7))
        cell_content_start = 65536 if cell_content_start == 0 else cell_content_start
        right_most_pointer = \
            page_num_reader(4) if page_type in (INTERIOR_INDEX, INTERIOR_TABLE) else \
            None

        pointers = Struct('>{}H'.format(num_cells)).unpack(page_num_reader(num_cells * 2)) + (page_size,)

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

                for serial_type in serial_types:
                    if serial_type == 1:
                        length = 1
                        value = cell_num_reader(length)
                        yield value
                    elif serial_type >= 12 and serial_type % 2 == 0:
                        length = int((serial_type - 12)/2)
                        value = cell_num_reader(length)
                        yield value
                    elif serial_type >= 13 and serial_type % 2 == 1:
                        length = int((serial_type - 13)/2)
                        value = cell_num_reader(length)
                        yield value
                    else:
                        raise ValueError('Unsupported type')

        if first_free_block:
            raise ValueError('Freeblock found, but are not supported')

    extra = False
    for _ in yield_all():
        extra = True

    if extra:
        raise Exception('More bytes than expected in SQLite file')
