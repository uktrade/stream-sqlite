# stream-sqlite [![CircleCI](https://circleci.com/gh/uktrade/stream-sqlite.svg?style=shield)](https://circleci.com/gh/uktrade/stream-sqlite) [![Test Coverage](https://api.codeclimate.com/v1/badges/b665c7634e8194fe6878/test_coverage)](https://codeclimate.com/github/uktrade/stream-sqlite/test_coverage)

Python function to extract all the rows from a SQLite database file concurrently with iterating over its bytes, without needing random access to the file.

Note that the [SQLite file format](https://www.sqlite.org/fileformat.html) is not designed to be streamed; the data is arranged in _pages_ of a fixed number of bytes, and the information to identify a page often comes _after_ the page in the stream (sometimes a great deal after). Therefore, pages are buffered in memory until they can be identified.


## Installation

```bash
pip install stream-sqlite
```


## Usage

```python
from stream_sqlite import stream_sqlite
import httpx

# Iterable that yields the bytes of a sqlite file
def sqlite_bytes():
    with httpx.stream('GET', 'http://www.parlgov.org/static/stable/2020/parlgov-stable.db') as r:
        yield from r.iter_bytes(chunk_size=65_536)

# If there is a single table in the file, there will be exactly one iteration of the outer loop.
# If there are multiple tables, each can appear multiple times.
for table_name, pragma_table_info, rows in stream_sqlite(sqlite_bytes(), max_buffer_size=1_048_576):
    for row in rows:
        print(row)
```


## Recommendations

If you have control over the SQLite file, `VACUUM;` should be run on it before streaming. In addition to minimising the size of the file, `VACUUM;` arranges the pages in a way that often reduces the buffering required when streaming. This is especially true if it was the target of intermingled `INSERT`s and/or `DELETE`s over multiple tables.

Also, indexes are not used for extracting the rows while streaming. If streaming is the only use case of the SQLite file, and you have control over it, indexes should be removed, and `VACUUM;` then run.

Some tests suggest that if the file is written in autovacuum mode, i.e. `PRAGMA auto_vacuum = FULL;`, then the pages are arranged in a way that reduces the buffering required when streaming. Your mileage may vary.
