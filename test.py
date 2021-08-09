import collections
import itertools
import os
import sqlite3
import tempfile
import unittest
import zlib

from stream_sqlite import stream_sqlite

column_constructor = collections.namedtuple('Column', ('cid', 'name', 'type', 'notnull', 'dflt_value', 'pk'))


class TestStreamSqlite(unittest.TestCase):

    def test_empty_database(self):
        for page_size, chunk_size in itertools.product(
            [512, 1024, 4096, 8192, 16384, 32768, 65536],
            [1, 2, 3, 5, 7, 32, 131072],
        ):
            with self.subTest(page_size, chunk_size=chunk_size):
                all_chunks = tables_list(stream_sqlite(db(['VACUUM;'], page_size, chunk_size), max_buffer_size=0))
                self.assertEqual([], all_chunks)

    def test_small_table(self):
        for page_size, chunk_size in itertools.product(
            [512, 1024, 4096, 8192, 16384, 32768, 65536],
            [1, 2, 3, 5, 7, 32, 131072],
        ):
            with self.subTest(page_size=page_size, chunk_size=chunk_size):
                sqls = [
                    "CREATE TABLE \"my_table_'1\" (my_text_col_a text, my_text_col_b text);",
                    "CREATE TABLE \"my_table_'2\" (my_text_col_a text, my_text_col_b text);",
                    "INSERT INTO \"my_table_'1\" VALUES ('some-text-a', 'some-text-b')",
                ]
                all_chunks = tables_list(stream_sqlite(db(sqls, page_size, chunk_size), max_buffer_size=0))
                self.assertEqual([(
                    "my_table_'1",
                    (
                        column_constructor(cid=0, name='my_text_col_a', type='text', notnull=0, dflt_value=None, pk=0),
                        column_constructor(cid=1, name='my_text_col_b', type='text', notnull=0, dflt_value=None, pk=0),
                    ),
                    [('some-text-a', 'some-text-b')],
                )], all_chunks)

    def test_overflow_master(self):
        for page_size, chunk_size in itertools.product(
            [512],
            [131072],
        ):
            column_name = "my_text_col_" + ('a' * 10000)
            with self.subTest(page_size=page_size, chunk_size=chunk_size):
                sqls = [
                    "CREATE TABLE my_table_1 ({} text);".format(column_name),
                    "INSERT INTO my_table_1 VALUES ('a')".format(column_name),
                ]
                all_chunks = tables_list(stream_sqlite(db(sqls, page_size, chunk_size), max_buffer_size=20971520))
                self.assertEqual([(
                    "my_table_1",
                    (
                        column_constructor(cid=0, name=column_name, type='text', notnull=0, dflt_value=None, pk=0),
                    ),
                    [
                        ('a',)
                    ],
                )], all_chunks)

    def test_overflow_non_master(self):
        for page_size, chunk_size in itertools.product(
            [512],
            [131072],
        ):
            with self.subTest(page_size=page_size, chunk_size=chunk_size):
                for i  in range(1, 10001):
                    sqls = [
                        "CREATE TABLE my_table_1 (my_text_col_a text);",
                        "INSERT INTO my_table_1 VALUES ('" + ('-' * i) + "')"
                    ]
                    all_chunks = tables_list(stream_sqlite(db(sqls, page_size, chunk_size), max_buffer_size=20971520))
                    self.assertEqual([(
                        "my_table_1",
                        (
                            column_constructor(cid=0, name='my_text_col_a', type='text', notnull=0, dflt_value=None, pk=0),
                        ),
                        [
                            ('-' * i,)
                        ],
                    )], all_chunks)

    def test_integers(self):
        for page_size, chunk_size in itertools.product(
            [512, 1024, 4096, 8192, 16384, 32768, 65536],
            [1, 2, 3, 5, 7, 32, 131072],
        ):
            with self.subTest(page_size=page_size, chunk_size=chunk_size):
                sqls = [
                    "CREATE TABLE my_table_1 (my_text_col_a integer);",
                    "INSERT INTO my_table_1 VALUES (0),(1),(2),(65536),(16777216),(4294967296),(1099511627776),(281474976710656),(72057594037927936)",
                    "INSERT INTO my_table_1 VALUES (0),(-1),(-2),(-65536),(-16777216),(-4294967296),(-1099511627776),(-281474976710656),(-72057594037927936)",
                ]
                all_chunks = tables_list(stream_sqlite(db(sqls, page_size, chunk_size), max_buffer_size=0))
                self.assertEqual([(
                    'my_table_1',
                    (
                        column_constructor(cid=0, name='my_text_col_a', type='integer', notnull=0, dflt_value=None, pk=0),
                    ),
                    [
                        (0,),
                        (1,),
                        (2,),
                        (65536,),
                        (16777216,),
                        (4294967296,),
                        (1099511627776,),
                        (281474976710656,),
                        (72057594037927936,),
                        (0,),
                        (-1,),
                        (-2,),
                        (-65536,),
                        (-16777216,),
                        (-4294967296,),
                        (-1099511627776,),
                        (-281474976710656,),
                        (-72057594037927936,)]
                )], all_chunks)

    def test_integers_primary_key(self):
        for page_size, chunk_size in itertools.product(
            [512, 1024, 4096, 8192, 16384, 32768, 65536],
            [1, 2, 3, 5, 7, 32, 131072],
        ):
            with self.subTest(page_size=page_size, chunk_size=chunk_size):
                sqls = [
                    "CREATE TABLE my_table_1 (m integer primary key);",
                    "INSERT INTO my_table_1 VALUES (0),(1),(2),(65536),(16777216),(4294967296),(1099511627776),(281474976710656),(72057594037927936)",
                    "INSERT INTO my_table_1 VALUES (-1),(-2),(-65536),(-16777216),(-4294967296),(-1099511627776),(-281474976710656),(-72057594037927936)",
                ]
                all_chunks = tables_list(stream_sqlite(db(sqls, page_size, chunk_size), max_buffer_size=0))
                self.assertEqual([(
                    'my_table_1',
                    (
                        column_constructor(cid=0, name='m', type='integer', notnull=0, dflt_value=None, pk=1),
                    ),
                    [
                        (-72057594037927936,),
                        (-281474976710656,),
                        (-1099511627776,),
                        (-4294967296,),
                        (-16777216,),
                        (-65536,),
                        (-2,),
                        (-1,),
                        (0,),
                        (1,),
                        (2,),
                        (65536,),
                        (16777216,),
                        (4294967296,),
                        (1099511627776,),
                        (281474976710656,),
                        (72057594037927936,),
                    ]
                )], all_chunks)

    def test_floats(self):
        for page_size, chunk_size in itertools.product(
            [512, 1024, 4096, 8192, 16384, 32768, 65536],
            [1, 2, 3, 5, 7, 32, 131072],
        ):
            with self.subTest(page_size=page_size, chunk_size=chunk_size):
                sqls = [
                    "CREATE TABLE my_table_1 (my_col_a real);",
                    "INSERT INTO my_table_1 VALUES (0.5123), (-0.1)",
                ]
                all_chunks = tables_list(stream_sqlite(db(sqls, page_size, chunk_size), max_buffer_size=0))
                self.assertEqual([(
                    'my_table_1',
                    (
                        column_constructor(cid=0, name='my_col_a', type='real', notnull=0, dflt_value=None, pk=0),
                    ),
                    [
                        (0.5123,),
                        (-0.1,),
                    ]
                )], all_chunks)

    def test_many_small_tables(self):
        for page_size, chunk_size in itertools.product(
            [512, 1024, 4096, 8192, 16384, 32768, 65536],
            [1, 2, 3, 5, 7, 32, 131072],
        ):
            with self.subTest(page_size=page_size, chunk_size=chunk_size):
                sqls = flatten((
                    [
                        "CREATE TABLE my_table_{} (my_text_col_a text, my_text_col_b text);".format(i),
                        "INSERT INTO my_table_{} VALUES ('some-text-a', 'some-text-b');".format(i),
                    ]
                    for i in range(1, 101)
                ))
                all_chunks = tables_list(stream_sqlite(db(sqls, page_size, chunk_size), max_buffer_size=1000000))
                self.assertEqual([(
                    'my_table_{}'.format(i),
                    (
                        column_constructor(cid=0, name='my_text_col_a', type='text', notnull=0, dflt_value=None, pk=0),
                        column_constructor(cid=1, name='my_text_col_b', type='text', notnull=0, dflt_value=None, pk=0),
                    ),
                    [('some-text-a', 'some-text-b')],
                ) for i in range(1, 101)], all_chunks)

    def test_large_table(self):
        for page_size, chunk_size in itertools.product(
            [512, 1024, 4096, 8192, 16384, 32768, 65536],
            [1, 2, 3, 5, 7, 32, 131072],
        ):
            with self.subTest(page_size=page_size, chunk_size=chunk_size):
                sqls = [
                    "CREATE TABLE my_table_1 (my_text_col_a text, my_text_col_b text);",
                ] + [
                    "INSERT INTO my_table_1 VALUES ('some-text-a', 'some-text-b')",
                ] * 1000
                all_chunks = tables_list(stream_sqlite(db(sqls, page_size, chunk_size), max_buffer_size=0))

                self.assertEqual(
                    [('some-text-a', 'some-text-b') for i in range (1, 1001)],
                    all_chunks[0][2],
                )

    def test_index(self):
        for page_size, chunk_size in itertools.product(
            [512, 1024, 4096, 8192, 16384, 32768, 65536],
            [1, 2, 3, 5, 7, 32, 131072],
        ):
            with self.subTest(page_size=page_size, chunk_size=chunk_size):
                sqls = (
                    ["CREATE TABLE my_table_1 (my_col_a integer);"] +
                    [
                        "INSERT INTO my_table_1 VALUES ({});".format(i)
                        for i in range(0, 1024)
                    ] +
                    ["CREATE INDEX my_index ON my_table_1(my_col_a);"]
                )
                all_chunks = tables_list(stream_sqlite(db(sqls, page_size, chunk_size), max_buffer_size=0))
                self.assertEqual([(
                    "my_table_1",
                    (
                        column_constructor(cid=0, name='my_col_a', type='integer', notnull=0, dflt_value=None, pk=0),
                    ),
                    [(i,) for i in range(0, 1024)],
                )], all_chunks)

    def test_index_overflow_few(self):
        for page_size, chunk_size in itertools.product(
            [512, 1024, 4096, 8192, 16384, 32768, 65536],
            [1, 2, 3, 5, 7, 32, 131072],
        ):
            with self.subTest(page_size=page_size, chunk_size=chunk_size):
                sqls = (
                    ["CREATE TABLE my_table_1 (my_col_a text);"] +
                    [
                        "INSERT INTO my_table_1 VALUES ('{}'), ('{}');".format('a' * 20000, 'b' * 20000)
                    ] +
                    ["CREATE INDEX my_index ON my_table_1(my_col_a);"]
                )
                all_chunks = tables_list(stream_sqlite(db(sqls, page_size, chunk_size), max_buffer_size=1048576))
                self.assertEqual([(
                    "my_table_1",
                    (
                        column_constructor(cid=0, name='my_col_a', type='text', notnull=0, dflt_value=None, pk=0),
                    ),
                    [
                        ('a' * 20000,),
                        ('b' * 20000,),
                    ],
                )], all_chunks)

    def test_index_interior_overflow_many(self):
        for page_size, chunk_size in itertools.product(
            [512, 1024, 4096, 8192, 16384, 32768, 65536],
            [1, 2, 3, 5, 7, 32, 131072],
        ):
            with self.subTest(page_size=page_size, chunk_size=chunk_size):
                sqls = (
                    ["CREATE TABLE my_table_1 (my_col_a text);"] +
                    [
                        "INSERT INTO my_table_1 VALUES ('{}');".format(str(i) * 2000)
                        for i in range(0, 100)
                    ] +
                    ["CREATE INDEX my_index ON my_table_1(my_col_a);"]
                )
                all_chunks = tables_list(stream_sqlite(db(sqls, page_size, chunk_size), max_buffer_size=1048576))
                self.assertEqual([(
                    "my_table_1",
                    (
                        column_constructor(cid=0, name='my_col_a', type='text', notnull=0, dflt_value=None, pk=0),
                    ),
                    [
                        (str(i) * 2000,)
                        for i in range(0, 100)
                    ],
                )], all_chunks)

    def test_integer_primary_key(self):
        for page_size, chunk_size in itertools.product(
            [512, 1024, 4096, 8192, 16384, 32768, 65536],
            [1, 2, 3, 5, 7, 32, 131072],
        ):
            with self.subTest(page_size=page_size, chunk_size=chunk_size):
                sqls = (
                    ["CREATE TABLE my_table_1 (my_col_a integer primary key);"] +
                    [
                        "INSERT INTO my_table_1 VALUES ({});".format(i)
                        for i in range(0, 100)
                    ]
                )
                all_chunks = tables_list(stream_sqlite(db(sqls, page_size, chunk_size), max_buffer_size=1048576))
                self.assertEqual([(
                    "my_table_1",
                    (
                        column_constructor(cid=0, name='my_col_a', type='integer', notnull=0, dflt_value=None, pk=1),
                    ),
                    [
                        (i,)
                        for i in range(0, 100)
                    ],
                )], all_chunks)

    def test_analyze(self):
        for page_size, chunk_size in itertools.product(
            [65536],
            [131072],
        ):
            with self.subTest(page_size=page_size, chunk_size=chunk_size):
                sqls = (
                    ["CREATE TABLE my_table_1 (my_col_a integer primary key);"] +
                    [
                        "INSERT INTO my_table_1 VALUES ({});".format(i)
                        for i in range(0, 100)
                    ] + [
                        "ANALYZE;"
                    ]
                )
                all_chunks = tables_list(stream_sqlite(db(sqls, page_size, chunk_size), max_buffer_size=1048576))
                self.assertEqual([(
                    "my_table_1",
                    (
                        column_constructor(cid=0, name='my_col_a', type='integer', notnull=0, dflt_value=None, pk=1),
                    ),
                    [
                        (i,)
                        for i in range(0, 100)
                    ],
                )], all_chunks)

    def test_with_pointermap_pages(self):
        string = '-' * 100000

        for page_size, chunk_size in itertools.product(
            [512, 1024, 4096, 8192],
            [512],
        ):
            with self.subTest(page_size=page_size, chunk_size=chunk_size):
                sqls = (
                    ["PRAGMA auto_vacuum = FULL;"] +
                    ["CREATE TABLE my_table_1 (my_col_a text);"] +
                    [
                        "INSERT INTO my_table_1 VALUES ('{}');".format(string)
                        for i in range(0, 200)
                    ]
                )
                all_chunks = tables_list(stream_sqlite(db(sqls, page_size, chunk_size), max_buffer_size=20971520))
                self.assertEqual([(
                    "my_table_1",
                    (
                        column_constructor(cid=0, name='my_col_a', type='text', notnull=0, dflt_value=None, pk=0),
                    ),
                    [(string,) for i in range(0, 200)],
                )], all_chunks)

    def test_iterable_finishes(self):
        for page_size, chunk_size in itertools.product(
            [65536],
            [131072],
        ):
            with self.subTest(page_size=page_size, chunk_size=chunk_size):
                finished = False
                def with_finished(db):
                    nonlocal finished
                    yield from db
                    finished = True

                sqls = (
                    ["CREATE TABLE my_table_1 (my_col_a integer primary key);"] +
                    [
                        "INSERT INTO my_table_1 VALUES ({});".format(i)
                        for i in range(0, 100)
                    ]
                )
                all_chunks = tables_list(stream_sqlite(with_finished(db(sqls, page_size, chunk_size)), max_buffer_size=1048576))
                self.assertEqual([(
                    "my_table_1",
                    (
                        column_constructor(cid=0, name='my_col_a', type='integer', notnull=0, dflt_value=None, pk=1),
                    ),
                    [
                        (i,)
                        for i in range(0, 100)
                    ],
                )], all_chunks)
                self.assertTrue(finished)

    def test_freelist(self):
        sqls = [
            "CREATE TABLE my_table_1 (my_text_col_a text, my_text_col_b text);",
        ] + [
            "INSERT INTO my_table_1 VALUES " + ','.join(["('some-text-a', 'some-text-b')"] * 500),
        ] * 1000 + [
            "DELETE FROM my_table_1",
        ]
        all_chunks = tables_list(stream_sqlite(db(sqls, page_size=1024, chunk_size=131072), max_buffer_size=20971520))
        self.assertEqual([], all_chunks)

        with self.assertRaises(ValueError):
            all_chunks = tables_list(stream_sqlite(db(sqls, page_size=1024, chunk_size=131072), max_buffer_size=1048576))

    def test_truncated(self):
        with self.assertRaises(ValueError):
            next(stream_sqlite([b'too-short'], max_buffer_size=20971520))

    def test_bad_header(self):
        with self.assertRaises(ValueError):
            next(stream_sqlite([b'0123456789'] * 10, max_buffer_size=20971520))

    def test_bad_encoding(self):
        sqls = [
            "CREATE TABLE my_table_1 (my_text_col_a text, my_text_col_b text);",
        ] + [
            "INSERT INTO my_table_1 VALUES ('some-text-a', 'some-text-b')",
        ]
        db_bytes = bytearray(b''.join(db(sqls, page_size=1024, chunk_size=131072)))
        db_bytes[56] = 99
        with self.assertRaises(ValueError):
            next(tables_list(stream_sqlite([db_bytes], max_buffer_size=20971520)))

    def test_bad_usable_space(self):
        sqls = [
            "CREATE TABLE my_table_1 (my_text_col_a text, my_text_col_b text);",
        ] + [
            "INSERT INTO my_table_1 VALUES ('some-text-a', 'some-text-b')",
        ]
        db_bytes = bytearray(b''.join(db(sqls, page_size=1024, chunk_size=131072)))
        db_bytes[20] = 1
        with self.assertRaises(ValueError):
            next(stream_sqlite([db_bytes], max_buffer_size=20971520))

    def test_unused_page(self):
        sqls = [
            "CREATE TABLE my_table_1 (my_text_col_a text, my_text_col_b text);",
        ] + [
            "INSERT INTO my_table_1 VALUES " + ','.join(["('some-text-a', 'some-text-b')"] * 500),
        ] * 1000 + [
            "DELETE FROM my_table_1",
        ]
        db_bytes = bytearray(b''.join(db(sqls, page_size=1024, chunk_size=131072)))
        db_bytes[32:36] = b'\x99\x00\x00\x00'
        with self.assertRaises(ValueError):
            next(tables_list(stream_sqlite([db_bytes], max_buffer_size=20971520)))

    def test_expected_page_unprocessed(self):
        sqls = [
            "CREATE TABLE my_table_1 (my_text_col_a text, my_text_col_b text);",
        ]
        db_bytes = bytearray(b''.join(db(sqls, page_size=1024, chunk_size=131072)))
        db_bytes[32:36] = b'\x99\x00\x00\x00'
        with self.assertRaises(ValueError):
            next(tables_list(stream_sqlite([db_bytes], max_buffer_size=20971520)))

    def test_lock_byte_page(self):
        def get_file_bytes():
            with open('fixtures/large.sqlite.gz', 'rb') as f:
                while True:
                    chunk = f.read(131072)
                    if not chunk:
                        break
                    yield chunk

        def get_sqlite_bytes(file_bytes):
            obj = zlib.decompressobj(32 + zlib.MAX_WBITS)
            for gzipped_chunk in file_bytes:
                chunk = obj.decompress(gzipped_chunk)
                if chunk:
                    yield chunk

        file_bytes = get_file_bytes()
        sqlite_bytes = get_sqlite_bytes(file_bytes)

        count = 0
        for table_row, table_info, rows in stream_sqlite(sqlite_bytes, max_buffer_size=838860800):
            for row in rows:
                count += 1

        self.assertEqual(count, 1200000)

    def test_lock_byte_page_with_autovaccum(self):
        def get_file_bytes():
            with open('fixtures/large-autovacuum-1024.sqlite.gz', 'rb') as f:
                while True:
                    chunk = f.read(131072)
                    if not chunk:
                        break
                    yield chunk

        def get_sqlite_bytes(file_bytes):
            obj = zlib.decompressobj(32 + zlib.MAX_WBITS)
            for gzipped_chunk in file_bytes:
                chunk = obj.decompress(gzipped_chunk)
                if chunk:
                    yield chunk

        file_bytes = get_file_bytes()
        sqlite_bytes = get_sqlite_bytes(file_bytes)

        count = 0
        for table_row, table_info, rows in stream_sqlite(sqlite_bytes, max_buffer_size=209715200):
            for row in rows:
                count += 1

        self.assertEqual(count, 1200000)

def db(sqls, page_size, chunk_size):
    with tempfile.NamedTemporaryFile() as fp:
        with sqlite3.connect(fp.name, isolation_level=None) as con:
            cur = con.cursor()
            cur.execute('PRAGMA page_size = {};'.format(page_size))
            for sql in sqls:
                cur.execute(sql)

        with open(fp.name, 'rb') as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                yield chunk

def tables_list(table_iter):
    return [
        (table_name, table_info, list(table_rows))
        for table_name, table_info, table_rows in table_iter
    ]


def flatten(l):
    return [
        item
        for sublist in l
        for item in sublist
    ]
