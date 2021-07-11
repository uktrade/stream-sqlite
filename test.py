import unittest

from stream_sqlite import stream_sqlite


class TestStreamSqlite(unittest.TestCase):

    def test_stream_sqlite(self):
        stream_sqlite()
