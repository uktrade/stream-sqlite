import io

from read_database_parts import (
    read_page_header_from_file,
    read_database_header_from_file,
    read_btree_leaf_from_file,
)

def test_database_routines(name):
    with open(name, "rb") as fptr:
        page_size = read_database_header_from_file(fptr)
        print(fptr.tell())
        page_type, \
        content_start, \
        number_of_cells, \
        right_most_pointer, \
        schema_format \
            = read_page_header_from_file(fptr)
        print(f"page_size = {page_size}")
        print(f"content_start = {content_start}")
        print(f"number_of_cells = {number_of_cells}")
        print(f"right_most_pointer = {right_most_pointer}")
        print(f"schema_format = {schema_format}")

        currpos = fptr.tell()
        fptr.read(content_start-currpos)
        print(f"after page {fptr.tell()}")
        for i in range(0,number_of_cells):
            read_btree_leaf_from_file(fptr)
        # read_btree_leaf_from_file(fptr)
