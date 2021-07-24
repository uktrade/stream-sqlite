import sqlite3
import tempfile
import unittest

from stream_sqlite import stream_sqlite


class TestStreamSqlite(unittest.TestCase):

    def test_empty_database(self):
        for chunk_size in [1, 2, 3, 5, 7, 32, 131072]:
            with self.subTest(chunk_size=chunk_size):
                chunks = stream_sqlite(db(['VACUUM;'], chunk_size))
                all_chunks = [chunk for chunk in chunks]
                self.assertEqual([], all_chunks)

    def test_small_table(self):
        for chunk_size in [1, 2, 3, 5, 7, 32, 131072]:
            with self.subTest(chunk_size=chunk_size):
                sqls = [
                    "CREATE TABLE my_table_1 (my_text_col_a text, my_text_col_b text);",
                    "CREATE TABLE my_table_2 (my_text_col_a text, my_text_col_b text);",
                    "INSERT INTO my_table_1 VALUES ('some-text-a', 'some-text-b')",
                ]
                chunks = stream_sqlite(db(sqls, chunk_size))
                all_chunks = [chunk for chunk in chunks]
                self.assertEqual([(
                    'my_table_1',
                    [
                        {'cid': 0, 'name': 'my_text_col_a', 'type': 'text', 'notnull': 0, 'dflt_value': None, 'pk': 0},
                        {'cid': 1, 'name': 'my_text_col_b', 'type': 'text', 'notnull': 0, 'dflt_value': None, 'pk': 0},
                    ],
                    [{'my_text_col_a': b'some-text-a', 'my_text_col_b': b'some-text-b'}],
                ),(
                    'my_table_2',
                    [
                        {'cid': 0, 'name': 'my_text_col_a', 'type': 'text', 'notnull': 0, 'dflt_value': None, 'pk': 0},
                        {'cid': 1, 'name': 'my_text_col_b', 'type': 'text', 'notnull': 0, 'dflt_value': None, 'pk': 0},
                    ],
                    [],
                )], all_chunks)

    def test_many_small_tables(self):
        for chunk_size in [1, 2, 3, 5, 7, 32, 131072]:
            with self.subTest(chunk_size=chunk_size):
                sqls = [
                    "CREATE TABLE my_table_{} (my_text_col_a text, my_text_col_b text);".format(i)
                    for i in range(1, 101)
                ] + ["INSERT INTO my_table_1 VALUES ('some-text-a', 'some-text-b')"]
                chunks = stream_sqlite(db(sqls, chunk_size))
                all_chunks = [chunk for chunk in chunks]
                self.assertEqual([(
                    'my_table_1',
                    [
                        {'cid': 0, 'name': 'my_text_col_a', 'type': 'text', 'notnull': 0, 'dflt_value': None, 'pk': 0},
                        {'cid': 1, 'name': 'my_text_col_b', 'type': 'text', 'notnull': 0, 'dflt_value': None, 'pk': 0},
                    ],
                    [{'my_text_col_a': b'some-text-a', 'my_text_col_b': b'some-text-b'}],
                )] + [(
                    'my_table_{}'.format(i),
                    [
                        {'cid': 0, 'name': 'my_text_col_a', 'type': 'text', 'notnull': 0, 'dflt_value': None, 'pk': 0},
                        {'cid': 1, 'name': 'my_text_col_b', 'type': 'text', 'notnull': 0, 'dflt_value': None, 'pk': 0},
                    ],
                    [],
                ) for i in range(2, 101)], all_chunks)

def db(sqls, chunk_size):
    with tempfile.NamedTemporaryFile() as fp:
        with sqlite3.connect(fp.name, isolation_level=None) as con:
            cur = con.cursor()
            for sql in sqls:
                cur.execute(sql)

        with open(fp.name, 'rb') as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                yield chunk
