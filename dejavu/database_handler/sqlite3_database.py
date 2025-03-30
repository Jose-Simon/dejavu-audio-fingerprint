import sqlite3
import os
from contextlib import contextmanager

from dejavu.base_classes.common_database import CommonDatabase
from typing import Dict, List, Tuple

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
    def cursor(self, dictionary=False):
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

    def insert_song(self, song_name: str, file_hash: str, total_hashes: int, file_path: str = None) -> int:
        """
        Inserts a new song entry with name and file path (if available) into the database.
    
        :param song_name: Name of the song.
        :param file_hash: Currently unused for SQLite but passed for compatibility.
        :param total_hashes: Currently unused for SQLite but passed for compatibility.
        :return: The inserted row ID.
        """
        with self.cursor() as cur:
            cur.execute(
                "INSERT INTO songs (name, file_path, fingerprinted) VALUES (?, ?, 0)",
                (song_name, song_name)  # using song_name as a proxy for full path
            )
            return cur.lastrowid

    def get_song_by_id(self, song_id: int) -> Dict[str, str]:
        with self.cursor() as cur:
            cur.execute("SELECT id, name, file_path FROM songs WHERE id = ?", (song_id,))
            row = cur.fetchone()
            return dict(row) if row else None
