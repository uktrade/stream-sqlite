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
