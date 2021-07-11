# stream-sqlite [![CircleCI](https://circleci.com/gh/uktrade/stream-sqlite.svg?style=shield)](https://circleci.com/gh/uktrade/stream-sqlite) [![Test Coverage](https://api.codeclimate.com/v1/badges/b665c7634e8194fe6878/test_coverage)](https://codeclimate.com/github/uktrade/stream-sqlite/test_coverage)

> Work in progress. This README serves as a rough design spec


## Usage

```python
from stream_sqlite import stream_sqlite
import httpx

def sqlite_bytes():
    # Iterable that yields the bytes of a sqlite file
    with httpx.stream('GET', 'https://www.example.com/my.sqlite') as r:
        yield from r.iter_bytes(chunk_size=65536)

# A table is not guarenteed to be contiguous in a sqlite file, so can appear
# multiple times while iterating
for table_name, rows in stream_sqlite(stream_sqlite()):
    for row in rows:
        print(row)
```


It can read a table from sqlite page(s)
Hardcoded database path for convenience

Missing:
Parsing sql in sqlite_master to identify the table name and the starting page
no checks for consistency between record size and space allocated to the record
record overflow
handling of NO ROWID tables. For the moment, I always output the row index at the beginning of the record
TESTS


Inefficient:
use recursion to read a page
several useless seek and read in the routines



Wrong:
reading of varint using more than 2 bytes (?)
use hardcoded page size