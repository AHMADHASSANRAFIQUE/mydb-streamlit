from typing import Dict, Tuple
from mydb_types import Records, Indexes
from collections import defaultdict
import json
import os

class IndexManager:
    @staticmethod
    def build_index(fields: Tuple[str, ...], data: Records, indexes: Indexes):
        index = defaultdict(set)
        for id_, record in data.items():
            if all(field in record for field in fields):
                # Create a composite key by joining field values with a delimiter
                composite_key = "|".join(str(record[field]) for field in fields)
                index[composite_key].add(id_)
        index_key = ",".join(fields)  # Use comma to represent multi-field index key
        indexes[index_key] = dict(index)
        IndexManager.save_index_to_file(indexes[index_key], index_key)

    @staticmethod
    def save_index_to_file(index: Dict, index_key: str):
        index_file = "index_db.json"
        try:
            existing_index = {}
            if os.path.exists(index_file):
                with open(index_file, 'r') as f:
                    existing_index = json.load(f)
            existing_index[index_key] = {k: list(v) for k, v in index.items()}
            with open(index_file, 'w') as f:
                json.dump(existing_index, f, indent=2)
            print(f"Index for {index_key} saved to {index_file}")
        except Exception as e:
            print(f"Failed to save index to {index_file}: {e}")