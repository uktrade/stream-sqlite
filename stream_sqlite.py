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
            yield page_bytes[p_start:p_end]

        if first_free_block:
            raise ValueError('Freeblock found, but are not supported')

    extra = False
    for _ in yield_all():
        extra = True

    if extra:
        raise Exception('More bytes than expected in SQLite file')
