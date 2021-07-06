# sqlite-firebreak

It can read a table from sqlite page(s)

Missing:
Parsing sql in sqlite_master to identify the table name and the starting page
no checks for consistency between record size and space allocated to the record
record overflow
TESTS


Inefficient:
use recursion to read a page
several useless seek and read in the routines

Wrong:
reading of varint using more than 2 bytes (?)
