import datetime

from sqlalchemy import Column, Integer, String, ForeignKey, TIMESTAMP
from sqlalchemy.orm import relationship

from mollusk.database import Base
from mollusk.storage import POSIXStorage, S3Storage


class Item(Base):
    """Database ORM model for Item."""

    __tablename__ = "item"

    item_id = Column(String(255), primary_key=True)
    title = Column(String(255))
    created_date = Column(TIMESTAMP, default=datetime.datetime.utcnow)
    updated_date = Column(
        TIMESTAMP, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow
    )

    # Relationships
    files = relationship("File", back_populates="item")


class File(Base):
    """Database ORM model for File."""

    __tablename__ = "file"

    file_id = Column(String(36), primary_key=True)
    item_id = Column(String(255), ForeignKey("item.item_id"))
    filename = Column(String(255))
    mimetype = Column(String(100))
    created_date = Column(TIMESTAMP, default=datetime.datetime.utcnow)

    # Relationships
    item = relationship("Item", back_populates="files")
    instances = relationship("FileInstance", back_populates="file")


class FileInstance(Base):
    """Database ORM model for FileInstance."""

    __tablename__ = "file_instance"

    file_instance_id = Column(String(36), primary_key=True)
    file_id = Column(String(36), ForeignKey("file.file_id"))
    storage_class = Column(String(50))  # Storage type (posix, s3, etc.)
    uri = Column(String(1024))
    checksum = Column(String(1024))
    size = Column(Integer)
    created_date = Column(TIMESTAMP, default=datetime.datetime.utcnow)

    # Relationships
    file = relationship("File", back_populates="instances")

    _storage = None

    @property
    def storage(self):
        if not self._storage:
            if self.storage_class == "POSIXStorage":
                storage = POSIXStorage(self.uri)
            elif self.storage_class == "S3Storage":
                storage = S3Storage(self.uri)
            else:
                raise ValueError(f"Storage class not recognized: {self.storage_class}")
            self._storage = storage
        return self._storage
