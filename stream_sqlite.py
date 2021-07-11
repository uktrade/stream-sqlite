from struct import Struct


def stream_sqlite(sqlite_chunks, chunk_size=65536):
    INTERIOR_INDEX = b'\x02'
    INTERIOR_TABLE = b'\x05'
    LEAF_INDEX = b'\x0a'
    LEAF_TABLE = b'\x0d'

    unsigned_short = Struct('>H')
    unsigned_long = Struct('>L')

    def varint(p, page):
        # This probably doesn't work with negative numbers
        value = 0
        high_bit = 1
        i = 0

        while high_bit and i < 9:
            high_bit = page[p] >> 7
            value = \
                ((value << 8) + page[p + i]) if i == 8 else \
                ((value << 7) + (page[p + i] & 0x7F))

            i += 1
            p += 1

        return p, value, i

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

    yield_all, yield_num, get_num = get_byte_readers(sqlite_chunks)

    header = get_num(100)
    total_bytes = len(header)

    if header[:16] != b'SQLite format 3\0':
        raise ValueError('Magic SQLite header string not found')

    yield header

    page_size, = unsigned_short.unpack(header[16:18])
    page_size = 65536 if page_size == 1 else page_size
    num_pages_expected, = unsigned_long.unpack(header[28:32])

    for page_num in range(1, num_pages_expected + 1):
        page_bytes = get_num(page_size - 100 if page_num == 1 else page_size)
        pointer_adjustment = -100 if page_num == 1 else 0

        page_type = page_bytes[0:1]
        first_free_block, num_cells, cell_content_start, num_frag_free = \
            Struct('>HHHB').unpack(page_bytes[1:8])
        cell_content_start = 65536 if cell_content_start == 0 else cell_content_start
        right_most_pointer = \
            page_bytes[8:12] if page_type in (INTERIOR_INDEX, INTERIOR_TABLE) else \
            None

        pointer_array_start_index = \
            12 if page_type in (INTERIOR_INDEX, INTERIOR_TABLE) else \
            8
        pointer_array_end_index = pointer_array_start_index + num_cells * 2
        pointers = Struct('>{}H'.format(num_cells)).unpack(page_bytes[pointer_array_start_index:pointer_array_end_index]) + (page_size,)

        for i in range(0, len(pointers) - 1):
            p_start = pointers[i] + pointer_adjustment
            p_end = pointers[i + 1] + pointer_adjustment

            if page_type == LEAF_TABLE:
                p, payload_size, _ = varint(p_start, page_bytes)
                p, rowid, _ = varint(p, page_bytes)
                payload = page_bytes[p:p+payload_size]

                p, header_remaining, header_varint_size = varint(p, page_bytes)
                header_remaining -= header_varint_size

                serial_types = []
                while header_remaining:
                    p, h, v_size = varint(p, page_bytes)
                    serial_types.append(h)
                    header_remaining -= v_size

                for serial_type in serial_types:
                    if serial_type == 1:
                        length = 1
                        value = page_bytes[p:p+length]
                        p += length
                        yield value
                    elif serial_type >= 12 and serial_type % 2 == 0:
                        length = int((serial_type - 12)/2)
                        value = page_bytes[p:p+length]
                        p += length
                        yield value
                    elif serial_type >= 13 and serial_type % 2 == 1:
                        length = int((serial_type - 13)/2)
                        value = page_bytes[p:p+length]
                        p += length
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
