from contextlib import contextmanager
from pathlib import Path

import peewee  # type: ignore


database = peewee.DatabaseProxy()


@contextmanager
def open_database(path, models=None):
    database_exists = path != ":memory:" and Path(path).exists()
    database.initialize(peewee.SqliteDatabase(path, pragmas={"foreign_keys": 1}))
    try:
        database.connect()
        if not database_exists and models is not None:
            database.create_tables(models)
        yield database
    finally:
        database.close()
