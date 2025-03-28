import mimetypes
import os
import uuid

from mollusk.database import Database
from mollusk.models import File, FileInstance, Item


class Repository:
    """
    Repository for managing digital objects (Items, Files, and FileInstances).

    Provides methods for creating, retrieving, updating, and deleting digital objects,
    as well as for managing file content across different storage backends.
    """

    def __init__(self, db_path=None):
        """Initialize the repository with a database connection."""
        self.db = Database(
            db_path or os.path.join(os.path.expanduser("~"), ".mollusk", "mollusk.sqlite")
        )
        self._session = None

    @property
    def session(self):
        """Get the current session or create a new one if needed."""
        if self._session is None:
            self._session = self.db.get_session()
        return self._session

    def commit(self):
        """Commit the current session."""
        if self._session:
            self._session.commit()

    def flush(self):
        """Flush the current session."""
        if self._session:
            self._session.flush()

    def rollback(self):
        """Rollback the current session."""
        if self._session:
            self._session.rollback()

    def close(self):
        """Close the current session and the database connection."""
        if self._session:
            self._session.close()
            self._session = None
        self.db.close()

    # ------------------------------------------------------------------------------
    # Item operations
    # ------------------------------------------------------------------------------
    def create_item(self, item_id, title, files: list[File] | None = None):
        """Create a new item."""
        item = Item(item_id=item_id, title=title)
        self.session.add(item)

        if files:
            for file in files:
                file.item_id = item.item_id
                self.session.add(file)

        self.commit()
        return item

    def get_item(self, item_id):
        """Get an item by its ID."""
        return self.session.query(Item).filter(Item.item_id == item_id).first()

    def get_items(self, skip=0, limit=100):
        """Get a paginated list of items."""
        return self.session.query(Item).offset(skip).limit(limit).all()

    def update_item(self, item_id, **kwargs):
        """Update an item with the given attributes."""
        item = self.get_item(item_id)
        if not item:
            return None

        for key, value in kwargs.items():
            setattr(item, key, value)

        self.commit()
        return item

    def delete_item(self, item_id):
        """Delete an item and all its files."""
        item = self.get_item(item_id)
        if not item:
            return False

        self.session.delete(item)
        self.commit()
        return True

    # ------------------------------------------------------------------------------
    # File operations
    # ------------------------------------------------------------------------------
    def create_file(
        self,
        item_id,
        filename,
        mimetype=None,
        instances: list[FileInstance] | None = None,
    ):
        """Create a new file associated with an item."""
        file_id = str(uuid.uuid4())
        default_mimetype = "application/octet-stream"
        file = File(
            file_id=file_id,
            item_id=item_id,
            filename=filename,
            mimetype=mimetype or mimetypes.guess_type(filename)[0] or default_mimetype,
        )
        self.session.add(file)

        if instances:
            for instance in instances:
                instance.file_id = file.file_id
                self.session.add(instance)

        self.commit()
        return file

    def get_file(self, file_id):
        """Get a file by its ID."""
        return self.session.query(File).filter(File.file_id == file_id).first()

    def get_files(self, item_id=None, skip=0, limit=100):
        """Get a paginated list of files, optionally filtered by item_id."""
        query = self.session.query(File)
        if item_id:
            query = query.filter(File.item_id == item_id)
        return query.offset(skip).limit(limit).all()

    def update_file(self, file_id, **kwargs):
        """Update a file with the given attributes."""
        file = self.get_file(file_id)
        if not file:
            return None

        for key, value in kwargs.items():
            setattr(file, key, value)

        self.commit()
        return file

    def delete_file(self, file_id):
        """Delete a file and all its instances."""
        file = self.get_file(file_id)
        if not file:
            return False

        self.session.delete(file)
        self.commit()
        return True

    # ------------------------------------------------------------------------------
    # FileInstance operations
    # ------------------------------------------------------------------------------
    def create_file_instance(
        self,
        file_id,
        storage_class,  # TODO: make this str or class
        uri,
        checksum=None,
        size=None,
    ):
        """Create a new file instance associated with a file."""
        file_instance_id = str(uuid.uuid4())
        file_instance = FileInstance(
            file_instance_id=file_instance_id,
            file_id=file_id,
            storage_class=storage_class,
            uri=uri,
            checksum=checksum,
            size=size,
        )

        if not checksum:
            file_instance.checksum = file_instance.storage.calculate_checksum()

        self.session.add(file_instance)
        self.commit()
        return file_instance

    def get_file_instance(self, file_instance_id):
        """Get a file instance by its ID."""
        return (
            self.session.query(FileInstance)
            .filter(FileInstance.file_instance_id == file_instance_id)
            .first()
        )

    def get_file_instances(self, file_id=None, skip=0, limit=100):
        """Get a paginated list of file instances, optionally filtered by file_id."""
        query = self.session.query(FileInstance)
        if file_id:
            query = query.filter(FileInstance.file_id == file_id)
        return query.offset(skip).limit(limit).all()

    def update_file_instance(self, file_instance_id, **kwargs):
        """Update a file instance with the given attributes."""
        file_instance = self.get_file_instance(file_instance_id)
        if not file_instance:
            return None

        for key, value in kwargs.items():
            setattr(file_instance, key, value)

        self.commit()
        return file_instance

    def delete_file_instance(self, file_instance_id):
        """Delete a file instance."""
        file_instance = self.get_file_instance(file_instance_id)
        if not file_instance:
            return False

        self.session.delete(file_instance)
        self.commit()
        return True
