from typing import Dict
from database import Collection
from errors import TransactionError, ValidationError

class Transaction:
    def __init__(self, collection: Collection):
        if not isinstance(collection, Collection):
            raise TransactionError("Invalid collection provided")
        self.collection = collection
        self.original_data = collection.data.copy() if collection.data_loaded else {}
        self.original_indexes = collection.indexes.copy()
        self.operations = []

    def insert(self, record: Dict):
        if not isinstance(record, dict):
            raise ValidationError("Insert record must be a dictionary")
        if not record:
            raise ValidationError("Insert record cannot be empty")
        self.operations.append(("insert", record))

    def update(self, condition: Dict, update_data: Dict):
        if not isinstance(condition, dict) or not isinstance(update_data, dict):
            raise ValidationError("Condition and update data must be dictionaries")
        if not condition:
            raise ValidationError("Update condition cannot be empty")
        if not update_data:
            raise ValidationError("Update data cannot be empty")
        self.operations.append(("update", condition, update_data))

    def delete(self, condition: Dict):
        if not isinstance(condition, dict):
            raise ValidationError("Delete condition must be a dictionary")
        if not condition:
            raise ValidationError("Delete condition cannot be empty")
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
            raise TransactionError(f"Transaction commit failed: {e}")

    def rollback(self):
        try:
            self.collection.data = self.original_data.copy()
            self.collection.indexes = self.original_indexes.copy()
            self.collection.db.debounce_save()
        except Exception as e:
            raise TransactionError(f"Transaction rollback failed: {e}")
