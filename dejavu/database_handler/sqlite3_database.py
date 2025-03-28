import sqlite3
import os
from contextlib import contextmanager

from dejavu.base_classes.common_database import CommonDatabase

class Sqlite3Database(CommonDatabase):
    type = "sqlite3"

    CREATE_SONGS_TABLE = """
        CREATE TABLE IF NOT EXISTS songs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            file_path TEXT,
            fingerprinted INTEGER DEFAULT 0
        );
    """

    CREATE_FINGERPRINTS_TABLE = """
        CREATE TABLE IF NOT EXISTS fingerprints (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            song_id INTEGER,
            hash TEXT,
            offset INTEGER,
            FOREIGN KEY(song_id) REFERENCES songs(id)
        );
    """

    CREATE_UNIQUE_CONSTRAINT = """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_hash_offset ON fingerprints (hash, song_id, offset);
    """

    CREATE_PATH_INDEX = """
        CREATE INDEX IF NOT EXISTS idx_file_path ON songs (file_path);
    """

    def __init__(self, db=None, **kwargs):
        self.db_path = db or "dejavu.db"
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.setup()

    @contextmanager
    def cursor(self):
        cur = self.conn.cursor()
        try:
            yield cur
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            raise e
        finally:
            cur.close()

    def setup(self):
        with self.cursor() as cur:
            cur.execute(self.CREATE_SONGS_TABLE)
            cur.execute(self.CREATE_FINGERPRINTS_TABLE)
            cur.execute(self.CREATE_UNIQUE_CONSTRAINT)
            cur.execute(self.CREATE_PATH_INDEX)
