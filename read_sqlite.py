import io

PAGE_SIZE = 4096

from read_database_parts import (
    INTERIOR_INDEX,
    INTERIOR_TABLE,
    LEAF_TABLE,
    LEAF_INDEX,
    read_page_header_from_file,
    read_database_header_from_file,
    read_cell_index_from_file,
    read_btree_leaf_from_file,
    read_btree_internal_table,
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
        print(f"page_type = {page_type}")
        print(f"content_start = {content_start}")
        print(f"number_of_cells = {number_of_cells}")
        print(f"right_most_pointer = {right_most_pointer}")
        print(f"schema_format = {schema_format}")
        cell_index = read_cell_index_from_file(fptr, number_of_cells)
        print(f"Cells addresses {cell_index}")
        currpos = fptr.tell()
        fptr.read(content_start - currpos)

        if page_type == LEAF_TABLE:
            print(f"after page {fptr.tell()}")
            for i in range(0,number_of_cells):
                row_list = read_btree_leaf_from_file(fptr)
                print(row_list)
        if page_type == INTERIOR_TABLE:
            fptr.seek(0)
            print(f"after page {fptr.tell()}")
            read_btree_internal_table(fptr, cell_index)
