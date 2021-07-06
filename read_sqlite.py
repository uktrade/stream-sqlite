from read_database_parts import (
    read_schema,
    read_page,
)


def test_database_routines(name, page):
    if page == 1:
        read_schema(name)
    else:
        read_page(name, page)
