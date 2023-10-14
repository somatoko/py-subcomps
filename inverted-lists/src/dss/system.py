import os
import atexit
from pathlib import Path
from .record_storage import RecordStorage
from .block_storage import BlockStorage
from .b_tree import BTree


class System:

    def __init__(self, name, data_root='./'):
        self.name = name.upper()
        self.system_dir = Path(data_root).resolve() / self.name
        self._ensure_system_dir_exists()
        self.records = {}
        self.indexes = {}

        atexit.register(self._cleanup)

    # ----------------- Public DDL API

    def get_record_storage(self, name, header_size=10, block_size=1024):
        name = name.upper()
        if name not in self.records:
            path = self.system_dir / (name + '.bs')
            rs = RecordStorage(path, header_size, block_size)
            self.records[name] = rs

        return self.records[name]

    def open_id_index(self, name):
        name = name.upper()
        if name not in self.indexes:
            path = self.system_dir / (name + '.idx')
            bs = BlockStorage(path, 10, 4096)
            self.indexes[name] = bs

        return BTree(self.indexes[name])

    # ----------------- Public DML API

    def insert_cow(self):
        pass

    def update_cow(self, cow):
        pass

    def delete_cow(self, cow):
        pass

    def cow_find_by_id(self, id):
        pass

    # ----------------- Private methods

    def _ensure_system_dir_exists(self):
        self.system_dir.mkdir(parents=True, exist_ok=True)

    def _cleanup(self):
        for name, recordS in self.records.items():
            recordS.cleanup()
        
        for name, index in self.indexes.items():
            index.cleanup()
