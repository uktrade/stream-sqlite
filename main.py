import sys

from read_sqlite import test_database_routines

DATABASEPATH = "/Users/stronal/PycharmProjects/sqlite_experiments/DIT210023.sqlite"
# DATABASEPATH = "/Users/stronal/PycharmProjects/sqlite_experiments/test.db"


# Press the green button in the gutter to run the script.
if __name__ == "__main__":
    test_database_routines(DATABASEPATH, int(sys.argv[1]))
