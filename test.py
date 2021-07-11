import sqlite3
import tempfile
import unittest

from stream_sqlite import stream_sqlite


class TestStreamSqlite(unittest.TestCase):

    def test_stream_sqlite(self):
        for chunk in stream_sqlite(small_db_bytes()):
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
