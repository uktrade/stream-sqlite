import sqlite3
import tempfile
import unittest

from stream_sqlite import stream_sqlite


class TestStreamSqlite(unittest.TestCase):

    def test_stream_sqlite(self):
        chunks = stream_sqlite(small_db_bytes())
        first = next(chunks)
        self.assertEqual(len(first), 100)
        self.assertEqual(first[:15], b'SQLite format 3')
        for chunk in chunks:
            pass

def small_db_bytes():
    with tempfile.NamedTemporaryFile() as fp:
        with sqlite3.connect(fp.name) as con:
            cur = con.cursor()
            cur.execute('''
                CREATE TABLE my_table (my_text_col text)
            ''')
            cur.execute("INSERT INTO my_table VALUES ('some-text')")
            con.commit()

        with open(fp.name, 'rb') as f:
            while True:
                chunk = f.read(1)
                if not chunk:
                    break
                yield chunk
