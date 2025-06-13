import json
import os
import time
from threading import Lock
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
from mydb_types import Data, Record, Records, Indexes, Conditions
from index import IndexManager
from query import Query, QueryAction
from queryParser import parse_my_query

class MyDB:
    def __init__(self):
        self.collections: Dict[str, 'Collection'] = {}
        self.lock = Lock()
        self.file_path = "database.json"
        self.last_save_time = 0
        self.save_interval = 1.0
        self.cache = {}
        self.collection_metadata = {}
        self.load_metadata()

    def debounce_save(self):
        current_time = time.time()
        if current_time - self.last_save_time >= self.save_interval:
            self.save_to_file()
            self.last_save_time = current_time
            self.cache.clear()

    def save_to_file(self):
        db_state = {}
        for name, collection in self.collections.items():
            db_state[name] = {
                "schema": collection.schema,
                "data": collection.data if collection.data_loaded else self.load_collection_data(name),
                "indexes": {k: {vk: list(vv) for vk, vv in v.items()} for k, v in collection.indexes.items()}
            }
        try:
            with open(self.file_path, 'w') as f:
                json.dump(db_state, f, indent=2)
        except Exception as e:
            raise Exception(f"Failed to save database to {self.file_path}: {e}")

    def load_metadata(self):
        if os.path.exists(self.file_path):
            try:
                with open(self.file_path, 'r') as f:
                    db_state = json.load(f)
                for name, state in db_state.items():
                    collection = Collection(name, state.get("schema", []), self)
                    collection.indexes = {k: {vk: set(vv) for vk, vv in v.items()} for k, v in state.get("indexes", {}).items()}
                    self.collections[name] = collection
                    self.collection_metadata[name] = {
                        "schema": state.get("schema", []),
                        "data_loaded": False
                    }
            except Exception as e:
                raise Exception(f"Failed to load metadata from {self.file_path}: {e}")

    def load_collection_data(self, collection_name: str) -> Records:
        if not os.path.exists(self.file_path):
            return {}
        try:
            with open(self.file_path, 'r') as f:
                db_state = json.load(f)
            collection_data = db_state.get(collection_name, {}).get("data", {})
            return collection_data
        except Exception as e:
            raise Exception(f"Failed to load data for {collection_name}: {e}")

    def create_collection(self, name: str, schema: List[str] = None):
        with self.lock:
            if name not in self.collections:
                self.collections[name] = Collection(name, schema or [], self)
                self.collection_metadata[name] = {"schema": schema or [], "data_loaded": False}
                self.debounce_save()
                return f"Collection {name} created"
            return f"Collection {name} already exists"

class Collection:
    def __init__(self, name: str, schema: List[str], db: 'MyDB'):
        self.name = name
        self.schema = schema
        self.db = db
        self.data: Records = None
        self.data_loaded = False
        self.indexes: Indexes = {}
        self.lock = db.lock

    def load_data(self):
        if not self.data_loaded:
            self.data = self.db.load_collection_data(self.name)
            self.data_loaded = True

    def current_time(self) -> str:
        return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    def parse_time(self, time_str: str) -> datetime:
        return datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S")

    def is_expired(self, record: Record) -> bool:
        if "ttl" not in record or "created_at" not in record:
            return False
        ttl = float(record["ttl"])
        created = self.parse_time(record["created_at"])
        return datetime.now() >= created + timedelta(seconds=ttl)

    def validate_record(self, record: Data) -> bool:
        if "_id" in record or "created_at" in record:
            raise ValueError("Cannot set reserved fields: _id, created_at")
        if self.schema:
            for field in self.schema:
                if field not in record and field not in ["ttl", "updated_at"]:
                    raise ValueError(f"Missing required field {field} in record: {record}")
            for field in record:
                if field not in self.schema and field not in ["ttl", "updated_at"]:
                    raise ValueError(f"Field {field} not in schema: {self.schema}")
                if field == "age":
                    try:
                        float(record[field])
                    except (ValueError, TypeError):
                        raise ValueError(f"Field {field} must be numeric, got {record[field]}")
        return True

    def match_query(self, record: Record, query: Dict, check_ttl: bool = True) -> bool:
        if check_ttl and self.is_expired(record):
            return False
        for key, condition in query.items():
            record_value = record.get(key)
            if record_value is None:
                return False
            if isinstance(condition, str):
                if str(record_value).strip() != condition:
                    return False
            elif isinstance(condition, dict):
                ops = condition
                try:
                    record_num = float(str(record_value).strip())
                except (ValueError, TypeError):
                    return False
                for op, value in ops.items():
                    try:
                        op_value = float(value)
                        if op == "$gt" and not (record_num > op_value):
                            return False
                        elif op == "$gte" and not (record_num >= op_value):
                            return False
                        elif op == "$lt" and not (record_num < op_value):
                            return False
                        elif op == "$lte" and not (record_num <= op_value):
                            return False
                        elif op == "$in" and str(record_value) not in [str(v) for v in value]:
                            return False
                    except (ValueError, TypeError):
                        return False
        return True

    def insert(self, record: Data) -> str:
        with self.lock:
            if not self.data_loaded:
                self.load_data()
            if not self.validate_record(record):
                raise ValueError(f"Invalid record for schema: {self.schema}")
            key = str(len(self.data) + 1)
            record = record.copy()
            record["_id"] = key
            record["created_at"] = self.current_time()
            for field in record:
                if field == "age":
                    try:
                        record[field] = str(float(record[field]))
                    except (ValueError, TypeError):
                        raise ValueError(f"Field {field} must be numeric, got {record[field]}")
                else:
                    record[field] = str(record[field])
            self.data[key] = record
            for index_key in self.indexes:
                fields = index_key.split(",")
                IndexManager.build_index(tuple(fields), self.data, self.indexes)
            self.db.debounce_save()
            return key

    def update(self, operations: Dict, update_data: Data) -> int:
        with self.lock:
            if not self.data_loaded:
                self.load_data()
            count = 0
            for key, record in self.data.items():
                if self.match_query(record, operations):
                    for field, value in update_data.items():
                        if field == "age":
                            try:
                                update_data[field] = str(float(value))
                            except (ValueError, TypeError):
                                raise ValueError(f"Field {field} must be numeric, got {value}")
                        else:
                            update_data[field] = str(value)
                    record.update(update_data)
                    record["updated_at"] = self.current_time()
                    count += 1
            if count > 0:
                for index_key in self.indexes:
                    fields = index_key.split(",")
                    IndexManager.build_index(tuple(fields), self.data, self.indexes)
            self.db.debounce_save()
            return count

    def delete(self, query: Dict) -> int:
        with self.lock:
            if not self.data_loaded:
                self.load_data()
            to_delete = [key for key, record in self.data.items() if self.match_query(record, query)]
            for key in to_delete:
                del self.data[key]
            if to_delete:
                for index_key in self.indexes:
                    fields = index_key.split(",")
                    IndexManager.build_index(tuple(fields), self.data, self.indexes)
            self.db.debounce_save()
            return len(to_delete)

    def aggregate_query(self, aggregate: Dict, conditions: Dict, group_by: str, sort: Dict) -> List[Dict]:
        if not self.data_loaded:
            self.load_data()
        results = []
        if group_by:
            groups = {}
            matched_records = 0
            for key, record in self.data.items():
                if self.match_query(record, conditions):
                    group_key = record.get(group_by, "null")
                    if group_key not in groups:
                        groups[group_key] = []
                    groups[group_key].append(record)
                    matched_records += 1
            
            for group_key, records in groups.items():
                result = {"group": group_key}
                for output_field, op in aggregate.items():
                    actual_field = output_field
                    for prefix in ["avg_", "sum_", "min_", "max_"]:
                        if output_field.startswith(prefix):
                            actual_field = output_field[len(prefix):]
                            break
                    values = []
                    for r in records:
                        if actual_field in r:
                            record_value = r[actual_field]
                            try:
                                val = float(str(record_value).strip())
                                values.append(val)
                            except (ValueError, TypeError):
                                pass
                    if op == "$count":
                        result[output_field] = len(records)
                    elif values:
                        if op == "$avg":
                            result[output_field] = sum(values) / len(values)
                        elif op == "$sum":
                            result[output_field] = sum(values)
                        elif op == "$min":
                            result[output_field] = min(values)
                        elif op == "$max":
                            result[output_field] = max(values)
                    else:
                        result[output_field] = None if op in ["$avg", "$min", "$max"] else 0
                results.append(result)
        else:
            result = {}
            valid_records = [r for r in self.data.values() if self.match_query(r, conditions)]
            for output_field, op in aggregate.items():
                actual_field = output_field
                for prefix in ["avg_", "sum_", "min_", "max_"]:
                    if output_field.startswith(prefix):
                        actual_field = output_field[len(prefix):]
                        break
                values = []
                for r in valid_records:
                    if actual_field in r:
                        try:
                            val = float(str(r[actual_field]).strip())
                            values.append(val)
                        except (ValueError, TypeError):
                            pass
                if op == "$count":
                    result[output_field] = len(valid_records)
                elif values:
                    if op == "$avg":
                        result[output_field] = sum(values) / len(values)
                    elif op == "$sum":
                        result[output_field] = sum(values)
                    elif op == "$min":
                        result[output_field] = min(values)
                    elif op == "$max":
                        result[output_field] = max(values)
                else:
                    result[output_field] = None if op in ["$avg", "$min", "$max"] else 0
            results.append(result)

        if sort:
            field, order = list(sort.items())[0]
            results.sort(key=lambda x: x.get(field, 0) or 0, reverse=(order == "desc"))
        
        return results

    def join_query(self, join: Dict, conditions: Dict) -> List[Dict]:
        if not self.data_loaded:
            self.load_data()
        results = []
        other_collection = self.db.collections.get(join["collection"])
        if not other_collection:
            return results
        
        if not other_collection.data_loaded:
            other_collection.load_data()
        
        field1, field2 = join["on"].split("=")
        for key1, record1 in self.data.items():
            if self.match_query(record1, conditions, check_ttl=True):
                for key2, record2 in other_collection.data.items():
                    if other_collection.match_query(record2, {}, check_ttl=True):
                        if record1.get(field1) == record2.get(field2):
                            joined_record = {**record1, **{f"{join['collection']}_{k}": v for k, v in record2.items()}}
                            results.append(joined_record)
        return results

    def parse_query(self, query_str: str) -> Dict:
        start_time = time.time()
        query_key = f"{self.name}:{query_str}"
        if query_key in self.db.cache:
            end_time = time.time()
            return {"results": self.db.cache[query_key], "execution_time": end_time - start_time}

        query = parse_my_query(query_str)
        results = []
        indexed = False

        if query.action == QueryAction.INSERT:
            key = self.insert(query.data)
            results = [{"_id": key, **query.data}]
        elif query.action == QueryAction.SELECT:
            if query.filter and query.filter["type"] == "compare":
                fields = [query.filter.get("field")]
                if len(query.conditions) > 1:
                    # Check for multi-field index
                    condition_fields = sorted(query.conditions.keys())
                    index_key = ",".join(condition_fields)
                    if index_key in self.indexes:
                        composite_value = "|".join(str(query.conditions[field]) for field in condition_fields)
                        if composite_value in self.indexes.get(index_key, {}):
                            if not self.data_loaded:
                                self.load_data()
                            for key in self.indexes[index_key][composite_value]:
                                record = self.data.get(key)
                                if record and self.match_query(record, query.conditions):
                                    results.append(record)
                            indexed = True
                elif fields[0] in self.indexes and query.filter["operator"] == "=":
                    value = query.filter["value"]
                    if value in self.indexes.get(fields[0], {}):
                        if not self.data_loaded:
                            self.load_data()
                        for key in self.indexes[fields[0]][value]:
                            record = self.data.get(key)
                            if record and self.match_query(record, query.conditions):
                                results.append(record)
                        indexed = True
            if not indexed:
                if not self.data_loaded:
                    self.load_data()
                for key, record in self.data.items():
                    if self.match_query(record, query.conditions):
                        results.append(record)
            if query.sort:
                field, order = list(query.sort.items())[0]
                results.sort(key=lambda x: float(x.get(field, 0)) if str(x.get(field, '')).replace('.','',1).isdigit() else x.get(field, ''), reverse=(order == "desc"))
        elif query.action == QueryAction.UPDATE:
            count = self.update(query.conditions, query.data)
            results = [{"updated": count}]
        elif query.action == QueryAction.DELETE:
            count = self.delete(query.conditions)
            results = [{"deleted": count}]
        elif query.action == QueryAction.INDEX:
            fields = tuple(query.index_field.split(","))  # Support comma-separated fields
            self.create_index(fields)
            results = [{"indexed": ",".join(fields)}]
        elif query.action == QueryAction.TRANSACT:
            from transaction import Transaction
            tx = Transaction(self)
            try:
                for op_type, conditions, data in query.transact_ops:
                    if op_type == "INSERT":
                        tx.insert(data)
                    elif op_type == "UPDATE":
                        tx.update(conditions, data)
                    elif op_type == "DELETE":
                        tx.delete(conditions)
                tx.commit()
                results = [{"transaction": "committed"}]
            except:
                tx.rollback()
                results = [{"transaction": "rolled back"}]
        elif query.action == QueryAction.AGGREGATE:
            results = self.aggregate_query(query.aggregate, query.conditions, query.group_by, query.sort)
        elif query.action == QueryAction.JOIN:
            results = self.join_query(query.join, query.conditions)

        self.db.cache[query_key] = results
        end_time = time.time()
        return {"results": results, "execution_time": end_time - start_time}

    def create_index(self, fields: Tuple[str, ...]):
        if not self.data_loaded:
            self.load_data()
        index_key = ",".join(fields)
        IndexManager.build_index(fields, self.data, self.indexes)
        self.db.debounce_save()