import datetime
import sqlalchemy
from sqlalchemy import Table, Column, String, DateTime, MetaData, insert, select, update

class ReviewDatabase:
    """
    Manages a SQLite table tracking each track's review status:
      - approved: tracks added or user-approved
      - unapproved: skipped tracks with retry schedule
    """
    def __init__(self, filename='.cache.db'):
        # Initialize DB engine and metadata
        self.engine = sqlalchemy.create_engine(f"sqlite:///{filename}")
        self.meta = MetaData()
        # Define schema for review_log
        self.table = Table(
            'review_log', self.meta,
            Column('track_key', String, primary_key=True),
            Column('status', String),
            Column('insert_time', DateTime),
            Column('next_retry', DateTime),
        )
        # Create table if missing
        self.meta.create_all(self.engine)

    def reset(self):
        """Drop and recreate the table."""
        with self.engine.begin() as conn:
            conn.execute(sqlalchemy.text("DROP TABLE IF EXISTS review_log"))
        self.__init__()

    def _compute_next_retry(self, insert_time):
        """Double the interval since insertion for exponential backoff."""
        elapsed = datetime.datetime.now() - insert_time
        return datetime.datetime.now() + (elapsed * 2)

    def set_approved(self, track_key):
        """Mark as approved and clear retry."""
        now = datetime.datetime.now()
        with self.engine.connect() as conn:
            with conn.begin():
                existing = conn.execute(
                    select(self.table).where(self.table.c.track_key == track_key)
                ).fetchone()
                if existing:
                    conn.execute(
                        update(self.table)
                        .where(self.table.c.track_key == track_key)
                        .values(status='approved', insert_time=now, next_retry=None)
                    )
                else:
                    conn.execute(
                        insert(self.table),
                        {'track_key': track_key, 'status': 'approved', 'insert_time': now, 'next_retry': None}
                    )

    def set_unapproved(self, track_key):
        """Mark as unapproved and schedule retry."""
        now = datetime.datetime.now()
        with self.engine.connect() as conn:
            with conn.begin():
                existing = conn.execute(
                    select(self.table).where(self.table.c.track_key == track_key)
                ).fetchone()
                if existing:
                    next_retry = self._compute_next_retry(existing.insert_time or now)
                    conn.execute(
                        update(self.table)
                        .where(self.table.c.track_key == track_key)
                        .values(status='unapproved', insert_time=now, next_retry=next_retry)
                    )
                else:
                    conn.execute(
                        insert(self.table),
                        {'track_key': track_key, 'status': 'unapproved', 'insert_time': now,
                         'next_retry': now + datetime.timedelta(days=7)}
                    )

    def get_status(self, track_key):
        """Return the review status or 'none'."""
        with self.engine.connect() as conn:
            row = conn.execute(
                select(self.table.c.status).where(self.table.c.track_key == track_key)
            ).fetchone()
        return row.status if row else 'none'

    def should_retry(self, track_key):
        """Check if next_retry has arrived."""
        with self.engine.connect() as conn:
            row = conn.execute(
                select(self.table.c.next_retry).where(self.table.c.track_key == track_key)
            ).fetchone()
        return bool(row and row.next_retry <= datetime.datetime.now())

# Global instance
review_db = ReviewDatabase()
