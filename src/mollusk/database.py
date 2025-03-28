import logging
import os

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

logger = logging.getLogger(__name__)

Base = declarative_base()


class Database:
    """Provides database connectivity and operations."""

    def __init__(self, db_path=None):
        """
        Initialize database connection.

        Args:
            db_path (str, optional): Path to the SQLite database file.
                                    If None, uses default location.
        """
        self.db_path = db_path
        self.engine = create_engine(f"sqlite:///{db_path}", echo=False)
        self.Session = sessionmaker(bind=self.engine)

    def create_tables(self):
        """Create all tables defined in the models."""
        if not os.path.exists(self.db_path):
            logger.info(f"Database not found, creating")
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        Base.metadata.create_all(self.engine)

    def get_session(self):
        """Get a new database session."""
        return self.Session()

    def close(self):
        """Close database connection."""
        self.engine.dispose()
