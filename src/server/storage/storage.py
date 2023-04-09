import os
import rocksdb
from enum import Enum

class DBColumn(Enum):
    MetaLog = b"meta_log"
    MetaCommit = b"meta_commit"
    UserLog = b"user_log"
    UserCommit = b"user_commit"
    Private = b"Private"

    def from_bytes(byte_str):
        return DBColumn._byte_to_column_map.get(byte_str, None)

DBColumn._byte_to_column_map = {column.value: column for column in DBColumn}

def is_rocksdb_database(path):
    current_file = os.path.join(path, "CURRENT")
    try:
        with open(current_file, "rb") as f:
            data = f.read()
        return True
    except IOError:
        return False


class Storage:
    def __init__(self, store_path: str):
        if not os.path.isdir(store_path):
            os.makedirs(store_path)

        options = rocksdb.Options(create_if_missing=True)
        if is_rocksdb_database(store_path):
            all_columns = rocksdb.list_column_families(store_path, options)
            column_families = {column_name: rocksdb.ColumnFamilyOptions() for column_name in all_columns}
            self.db_ = rocksdb.DB(store_path, options, column_families=column_families)
        else:
            self.db_ = rocksdb.DB(store_path, options)

        column_families = [handle.name for handle in self.db_.column_families]
        for column in DBColumn:
            if column.value not in column_families:
                self.db_.create_column_family(column.value, rocksdb.ColumnFamilyOptions())

    def Set(self, column: DBColumn, key: str, value: bytes):
        data_column = self.db_.get_column_family(column.value)
        status = self.db_.put((data_column, key.encode('utf-8')), value, sync=True)
        if status and not status.ok():
            raise Exception(f'Failed to put data: {status}')

    def Get(self, column: DBColumn, key: str) -> bytes:
        data_column = self.db_.get_column_family(column.value)
        return self.db_.get((data_column, key.encode('utf-8')), verify_checksums=True)
    
    def Delete(self, column: DBColumn, key: str):
        data_column = self.db_.get_column_family(column.value)
        status = self.db_.delete((data_column, key.encode('utf-8')), sync=True)
        if status and not status.ok():
            raise Exception(f'Failed to delete data: {status}')
    
    class KeyIterator:
        def __init__(self, storage, column_handle, start_key=None):
            self.storage = storage
            if start_key:
                self.iterator = self.storage.db_.iterkeys(column_handle, start_key.encode('utf-8'))
            else:
                self.iterator = self.storage.db_.iterkeys(column_handle)

        def __iter__(self):
            return self

        def __next__(self):
            return self.iterator.__next__().decode('utf-8')

    class ItemIterator:
        def __init__(self, storage, column_handle, start_key=None):
            self.storage = storage
            if start_key:
                self.iterator = self.storage.db_.iteritems(column_handle, start_key.encode('utf-8'))
            else:
                self.iterator = self.storage.db_.iteritems(column_handle)

        def __iter__(self):
            return self

        def __next__(self):
            key, value = self.iterator.__next__()
            return key.decode('utf-8'), value


    def NewKeyIterator(self, column: DBColumn, key: str = None) -> KeyIterator:
        column_handle = self.db_.get_column_family(column.value)
        return Storage.KeyIterator(self, column_handle=column_handle, start_key=key)
    
    def NewItemIterator(self, column: DBColumn, key: str = None) -> ItemIterator:
        column_handle = self.db_.get_column_family(column.value)
        return Storage.ItemIterator(self, column_handle=column_handle, start_key=key)
