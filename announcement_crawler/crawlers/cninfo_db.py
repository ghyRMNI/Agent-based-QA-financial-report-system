import sqlite3
from typing import Dict
import os
import logging


class CninfoAnnouncementDB:
    def __init__(self, db_path: str):
        """
        Initialize announcement database
        Args:
            db_path: Database file path
        """
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.db_path = os.path.abspath(db_path)
        self.logger = logging.getLogger("CninfoAnnouncementDB")
        self._init_db()
        self._id_cache = set()
        self._load_id_cache()

    def _init_db(self):
        """
        Initialize database table schema
        Schema:
            - secCode: Stock code
            - secName: Stock name
            - announcementId: Announcement ID (primary key)
            - announcementTitle: Announcement title
            - downloadUrl: Announcement URL
            - pageColumn: Page column
            - announcementTime: Announcement time
        """
        with self._get_connection() as conn:
            conn.execute(
                """
            CREATE TABLE IF NOT EXISTS announcements (
                secCode TEXT NOT NULL,
                secName TEXT NOT NULL,
                announcementId TEXT PRIMARY KEY,
                announcementTitle TEXT NOT NULL,
                downloadUrl TEXT NOT NULL,
                pageColumn TEXT,
                announcementTime TEXT
            )"""
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_secCode ON announcements(secCode)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_announcementId ON announcements(announcementId)"
            )

    def _get_connection(self) -> sqlite3.Connection:
        """
        Get database connection
        Returns:
            sqlite3.Connection: Database connection object
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _load_id_cache(self):
        """
        Load existing announcement IDs into memory cache for fast lookup
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT announcementId FROM announcements")
            self._id_cache = {row["announcementId"] for row in cursor.fetchall()}

    def record_exists(self, announcement_id: str) -> bool:
        """
        Check if an announcement record already exists in the database
        Args:
            announcement_id: Announcement ID
        Returns:
            bool: True if record exists, False otherwise
        """
        return announcement_id in self._id_cache

    def save_record(self, record: Dict) -> bool:
        """
        Save an announcement record to the database
        Args:
            record: Announcement dictionary, must contain the following fields:
                - secCode: Stock code
                - secName: Stock name
                - announcementId: Announcement ID
                - announcementTitle: Announcement title
                - downloadUrl: Announcement URL
                - pageColumn: Page column
                - announcementTime: Announcement time
        Returns:
            bool: True if save successful, False otherwise
        """
        required_fields = [
            "secCode",
            "secName",
            "announcementId",
            "announcementTitle",
            "downloadUrl",
            "pageColumn",
        ]
        if not all(field in record for field in required_fields):
            self.logger.error("Missing required fields in record")
            return False

        try:
            with self._get_connection() as conn:
                conn.execute(
                    """
                INSERT OR REPLACE INTO announcements (
                    secCode, secName, announcementId, 
                    announcementTitle, downloadUrl, pageColumn, announcementTime
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        record["secCode"],
                        record["secName"],
                        record["announcementId"],
                        record["announcementTitle"],
                        record["downloadUrl"],
                        record["pageColumn"],
                        record["announcementTime"],
                    ),
                )
                self._id_cache.add(record["announcementId"])
                return True
        except Exception as e:
            self.logger.error(f"Save record failed: {str(e)}")
            return False

    def get_all_records(self) -> list:
        """
        Get all announcement records from database
        Returns:
            list: List of all announcement records
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM announcements")
            return [dict(row) for row in cursor.fetchall()]

    def delete_record(self, announcement_id: str) -> bool:
        """
        Delete specified announcement record from database
        Args:
            announcement_id: Announcement ID to delete
        Returns:
            bool: True if delete successful, False otherwise
        """
        try:
            with self._get_connection() as conn:
                conn.execute(
                    "DELETE FROM announcements WHERE announcementId = ?",
                    (announcement_id,),
                )
                if announcement_id in self._id_cache:
                    self._id_cache.remove(announcement_id)
                return True
        except Exception as e:
            self.logger.error(f"Delete record failed: {str(e)}")
            return False

    def get_records_by_date(self, date: str) -> list:
        """
        Get all announcement records for a specific date
        Args:
            date: Query date in YYYY-MM-DD format
        Returns:
            list: List of announcement records for that date
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM announcements WHERE date(announcementTime) = ?", (date,)
            )
            return [dict(row) for row in cursor.fetchall()]

    def get_count_by_date(self, date: str) -> int:
        """
        Get total number of announcements for a specific date
        Args:
            date: Query date in YYYY-MM-DD format
        Returns:
            int: Total number of announcements for that date
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT COUNT(*) FROM announcements WHERE date(announcementTime) = ?",
                (date,),
            )
            return cursor.fetchone()[0]
