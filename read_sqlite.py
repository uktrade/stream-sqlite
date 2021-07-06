from read_database_parts import (
    read_schema,
    read_page,
)


def test_database_routines(name):
    read_schema(name)
    # read_page(name, 31)
    # read_page(name, 61)
