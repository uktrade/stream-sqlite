import sqlite3
import tempfile
import unittest

from stream_sqlite import stream_sqlite


class TestStreamSqlite(unittest.TestCase):

    def test_stream_sqlite(self):
        chunks = stream_sqlite(small_db_bytes())
        all_chunks = [chunk for chunk in chunks]
        self.assertEqual([(
            b'my_table_a',
            [{'my_text_col_a': b'some-text-a', 'my_text_col_b': b'some-text-b'}],
        ),(
            b'my_table_b',
            [],
        )], all_chunks)

def small_db_bytes():
    with tempfile.NamedTemporaryFile() as fp:
        with sqlite3.connect(fp.name) as con:
            cur = con.cursor()
            cur.execute('''
                CREATE TABLE my_table_a (my_text_col_a text, my_text_col_b text);
            ''')
            cur.execute('''
                CREATE TABLE my_table_b (my_text_col_a text, my_text_col_b text);
            ''')
            cur.execute("INSERT INTO my_table_a VALUES ('some-text-a', 'some-text-b')")
            con.commit()

        with open(fp.name, 'rb') as f:
            while True:
                chunk = f.read(1)
                if not chunk:
                    break
                yield chunk
