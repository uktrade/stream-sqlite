import sqlite3
import tempfile
import unittest

from stream_sqlite import stream_sqlite


class TestStreamSqlite(unittest.TestCase):

    def test_stream_sqlite(self):
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
                    b'my_table_1',
                    [
                        {'cid': 0, 'name': 'my_text_col_a', 'type': 'text', 'notnull': 0, 'dflt_value': None, 'pk': 0},
                        {'cid': 1, 'name': 'my_text_col_b', 'type': 'text', 'notnull': 0, 'dflt_value': None, 'pk': 0},
                    ],
                    [{'my_text_col_a': b'some-text-a', 'my_text_col_b': b'some-text-b'}],
                ),(
                    b'my_table_2',
                    [
                        {'cid': 0, 'name': 'my_text_col_a', 'type': 'text', 'notnull': 0, 'dflt_value': None, 'pk': 0},
                        {'cid': 1, 'name': 'my_text_col_b', 'type': 'text', 'notnull': 0, 'dflt_value': None, 'pk': 0},
                    ],
                    [],
                )], all_chunks)

def db(sqls, chunk_size):
    with tempfile.NamedTemporaryFile() as fp:
        with sqlite3.connect(fp.name) as con:
            cur = con.cursor()
            for sql in sqls:
                cur.execute(sql)
            con.commit()

        with open(fp.name, 'rb') as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                yield chunk
