from typing import Dict
from database import Collection

class Transaction:
    def __init__(self, collection: Collection):
        self.collection = collection
        self.original_data = collection.data.copy()
        self.original_indexes = collection.indexes.copy()
        self.operations = []

    def insert(self, record: Dict):
        self.operations.append(("insert", record))

    def update(self, condition: Dict, update_data: Dict):
        self.operations.append(("update", condition, update_data))

    def delete(self, condition: Dict):
        self.operations.append(("delete", condition))

    def commit(self):
        try:
            for op in self.operations:
                op_type = op[0]
                if op_type == "insert":
                    self.collection.insert(op[1])
                elif op_type == "update":
                    self.collection.update(op[1], op[2])
                elif op_type == "delete":
                    self.collection.delete(op[1])
            self.collection.db.debounce_save()
        except Exception as e:
            self.rollback()
            raise e

    def rollback(self):
        self.collection.data = self.original_data.copy()
        self.collection.indexes = self.original_indexes.copy()
