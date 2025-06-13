import json
import os
import time
import re
from threading import Lock
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
from github import Github, GithubException
from mydb_types import Data, Record, Records, Indexes, Conditions
from index import IndexManager
from query import Query, QueryAction
from queryParser import parse_my_query
from transaction import Transaction
from errors import DatabaseError, ValidationError, CollectionError, QueryError, TransactionError, IndexError

class MyDB:
    def __init__(self):
        self.collections: Dict[str, 'Collection'] = {}
        self.lock = Lock()
        self.file_path = "database.json"
        self.last_save_time = 0
        self.save_interval = 1.0
        self.cache = {}
        self.collection_metadata = {}
        self.github_token = os.environ.get("GITHUB_TOKEN")
        self.github_repo = os.environ.get("GITHUB_REPO", "your-username/mydb-streamlit")
        self._validate_github_credentials()
        try:
            self.load_metadata()
        except Exception as e:
            raise DatabaseError(f"Failed to initialize database: {e}")

    def _validate_github_credentials(self):
        """Validate GitHub token and repository name."""
        if not self.github_token:
            raise ValidationError("GITHUB_TOKEN environment variable is required")
        if not isinstance(self.github_token, str) or len(self.github_token.strip()) < 40:
            raise ValidationError("Invalid GitHub token format")
        if not self.github_repo or not re.match(r"^[a-zA-Z0-9_-]+/[a-zA-Z0-9_-]+$", self.github_repo):
            raise ValidationError("Invalid GitHub repository format. Expected 'username/repo'")

    def debounce_save(self):
        try:
            current_time = time.time()
            if current_time - self.last_save_time >= self.save_interval:
                self.save_to_file()
                self.last_save_time = current_time
                self.cache.clear()
        except Exception as e:
            raise DatabaseError(f"Failed to save database: {e}")

    def save_to_file(self):
        db_state = {}
        for name, collection in self.collections.items():
            db_state[name] = {
                "schema": collection.schema,
                "data": collection.data if collection.data_loaded else self.load_collection_data(name),
                "indexes": {k: {vk: list(vv) for vk, vv in v.items()} for k, v in collection.indexes.items()}
            }
        try:
            g = Github(self.github_token)
            repo = g.get_repo(self.github_repo)
            content = json.dumps(db_state, indent=2)
            try:
                file = repo.get_contents(self.file_path, ref="main")
                repo.update_file(
                    self.file_path,
                    f"Update {self.file_path} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                    content,
                    file.sha,
                    branch="main"
                )
            except GithubException as e:
                if e.status == 404:
                    repo.create_file(
                        self.file_path,
                        f"Create {self.file_path} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                        content,
                        branch="main"
                    )
                else:
                    raise DatabaseError(f"GitHub API error: {e}")
        except GithubException as e:
            raise DatabaseError(f"Failed to save database to GitHub: {e}")
        except Exception as e:
            raise DatabaseError(f"Unexpected error saving to GitHub: {e}")

    def load_metadata(self):
        try:
            g = Github(self.github_token)
            repo = g.get_repo(self.github_repo)
            try:
                file = repo.get_contents(self.file_path, ref="main")
                db_state = json.loads(file.decoded_content.decode())
                for name, state in db_state.items():
                    if not isinstance(name, str) or not name.strip():
                        raise ValidationError(f"Invalid collection name in database file: {name}")
                    schema = state.get("schema", [])
                    if not isinstance(schema, list):
                        raise ValidationError(f"Invalid schema for collection {name}")
                    collection = Collection(name, schema, self)
                    collection.indexes = {k: {vk: set(vv) for vk, vv in v.items()} for k, v in state.get("indexes", {}).items()}
                    self.collections[name] = collection
                    self.collection_metadata[name] = {
                        "schema": schema,
                        "data_loaded": False
                    }
            except GithubException as e:
                if e.status == 404:
                    return  # No database file exists yet
                raise DatabaseError(f"Failed to load metadata from GitHub: {e}")
        except Exception as e:
            raise DatabaseError(f"Unexpected error loading metadata from GitHub: {e}")

    def load_collection_data(self, collection_name: str) -> Records:
        if not isinstance(collection_name, str) or not collection_name.strip():
            raise ValidationError("Collection name must be a non-empty string")
        try:
            g = Github(self.github_token)
            repo = g.get_repo(self.github_repo)
            try:
                file = repo.get_contents(self.file_path, ref="main")
                db_state = json.loads(file.decoded_content.decode())
                collection_data = db_state.get(collection_name, {}).get("data", {})
                if not isinstance(collection_data, dict):
                    raise ValidationError(f"Invalid data format for collection {collection_name}")
                return collection_data
            except GithubException as e:
                if e.status == 404:
                    return {}
                raise DatabaseError(f"Failed to load data for {collection_name}: {e}")
        except Exception as e:
            raise DatabaseError(f"Unexpected error loading data for {collection_name}: {e}")

    def create_collection(self, name: str, schema: List[str] = None):
        if not isinstance(name, str) or not name.strip() or not re.match(r"^[a-zA-Z0-9_]{1,50}$", name):
            raise ValidationError("Collection name must be 1-50 characters, alphanumeric or underscore")
        if schema:
            for field in schema:
                if not isinstance(field, str) or not field.strip() or not re.match(r"^[a-zA-Z0-9_]{1,50}$", field):
                    raise ValidationError(f"Invalid schema field: {field}")
                if field in ["_id", "created_at", "updated_at"]:
                    raise ValidationError(f"Schema field '{field}' is reserved")
        with self.lock:
            if name in self.collections:
                raise CollectionError(f"Collection {name} already exists")
            self.collections[name] = Collection(name, schema or [], self)
            self.collection_metadata[name] = {"schema": schema or [], "data_loaded": False}
            try:
                self.debounce_save()
            except DatabaseError as e:
                del self.collections[name]
                del self.collection_metadata[name]
                raise
            return f"Collection {name} created"

class Collection:
    def __init__(self, name: str, schema: List[str], db: 'MyDB'):
        if not isinstance(name, str) or not name.strip():
            raise ValidationError("Collection name must be a non-empty string")
        if not isinstance(db, MyDB):
            raise ValidationError("Invalid database instance")
        self.name = name
        self.schema = schema
        self.db = db
        self.data: Records = None
        self.data_loaded = False
        self.indexes: Indexes = {}
        self.lock = db.lock

    def load_data(self):
        if not self.data_loaded:
            try:
                self.data = self.db.load_collection_data(self.name)
                self.data_loaded = True
            except DatabaseError as e:
                raise CollectionError(f"Failed to load data for collection {self.name}: {e}")

    def current_time(self) -> str:
        return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    def parse_time(self, time_str: str) -> datetime:
        try:
            return datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S")
        except ValueError as e:
            raise ValidationError(f"Invalid timestamp format: {time_str}")

    def is_expired(self, record: Record) -> bool:
        if not isinstance(record, dict):
            raise ValidationError("Record must be a dictionary")
        if "ttl" not in record or "created_at" not in record:
            return False
        try:
            ttl = float(record["ttl"])
            created = self.parse_time(record["created_at"])
            return datetime.now() >= created + timedelta(seconds=ttl)
        except (ValueError, TypeError) as e:
            raise ValidationError(f"Invalid TTL or created_at in record: {e}")

    def validate_record(self, record: Data) -> bool:
        if not isinstance(record, dict):
            raise ValidationError("Record must be a dictionary")
        if "_id" in record or "created_at" in record:
            raise ValidationError("Cannot set reserved fields: _id, created_at")
        if self.schema:
            for field in self.schema:
                if field not in record and field not in ["ttl", "updated_at"]:
                    raise ValidationError(f"Missing required field {field} in record: {record}")
            for field in record:
                if field not in self.schema and field not in ["ttl", "updated_at"]:
                    raise ValidationError(f"Field {field} not in schema: {self.schema}")
                if field == "age":
                    try:
                        float(record[field])
                    except (ValueError, TypeError):
                        raise ValidationError(f"Field {field} must be numeric, got {record[field]}")
        return True

    def match_query(self, record: Record, query: Dict, check_ttl: bool = True) -> bool:
        if not isinstance(record, dict) or not isinstance(query, dict):
            raise ValidationError("Record and query must be dictionaries")
        if check_ttl and self.is_expired(record):
            return False
        for key, condition in query.items():
            if not isinstance(key, str) or not key.strip():
                raise ValidationError(f"Invalid query field: {key}")
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
                    if op not in ['$gt', '$gte', '$lt', '$lte', '$in']:
                        raise ValidationError(f"Invalid operator: {op}")
                    try:
                        if op == "$in":
                            if not isinstance(value, list):
                                raise ValidationError(f"$in operator requires a list, got {type(value)}")
                            if str(record_value) not in [str(v) for v in value]:
                                return False
                        else:
                            op_value = float(value)
                            if op == "$gt" and not (record_num > op_value):
                                return False
                            elif op == "$gte" and not (record_num >= op_value):
                                return False
                            elif op == "$lt" and not (record_num < op_value):
                                return False
                            elif op == "$lte" and not (record_num <= op_value):
                                return False
                    except (ValueError, TypeError):
                        return False
        return True

    def insert(self, record: Data) -> str:
        with self.lock:
            if not self.data_loaded:
                self.load_data()
            try:
                self.validate_record(record)
            except ValidationError as e:
                raise ValidationError(f"Invalid record for insert: {e}")
            key = str(len(self.data) + 1)
            record = record.copy()
            record["_id"] = key
            record["created_at"] = self.current_time()
            for field in record:
                if field == "age":
                    try:
                        record[field] = str(float(record[field]))
                    except (ValueError, TypeError):
                        raise ValidationError(f"Field {field} must be numeric, got {record[field]}")
                else:
                    record[field] = str(record[field])
            self.data[key] = record
            try:
                for index_key in self.indexes:
                    fields = index_key.split(",")
                    IndexManager.build_index(tuple(fields), self.data, self.indexes)
                self.db.debounce_save()
            except IndexError as e:
                del self.data[key]
                raise IndexError(f"Failed to update indexes after insert: {e}")
            return key

    def update(self, operations: Dict, update_data: Data) -> int:
        if not isinstance(operations, dict) or not isinstance(update_data, dict):
            raise ValidationError("Operations and update data must be dictionaries")
        with self.lock:
            if not self.data_loaded:
                self.load_data()
            count = 0
            for key, record in self.data.items():
                if self.match_query(record, operations):
                    for field, value in update_data.items():
                        if field in ["_id", "created_at"]:
                            raise ValidationError(f"Cannot update reserved field: {field}")
                        if field == "age":
                            try:
                                update_data[field] = str(float(value))
                            except (ValueError, TypeError):
                                raise ValidationError(f"Field {field} must be numeric, got {value}")
                        else:
                            update_data[field] = str(value)
                    record.update(update_data)
                    record["updated_at"] = self.current_time()
                    count += 1
            if count > 0:
                try:
                    for index_key in self.indexes:
                        fields = index_key.split(",")
                        IndexManager.build_index(tuple(fields), self.data, self.indexes)
                    self.db.debounce_save()
                except IndexError as e:
                    raise IndexError(f"Failed to update indexes after update: {e}")
            return count

    def delete(self, query: Dict) -> int:
        if not isinstance(query, dict):
            raise ValidationError("Delete query must be a dictionary")
        with self.lock:
            if not self.data_loaded:
                self.load_data()
            to_delete = [key for key, record in self.data.items() if self.match_query(record, query)]
            for key in to_delete:
                del self.data[key]
            if to_delete:
                try:
                    for index_key in self.indexes:
                        fields = index_key.split(",")
                        IndexManager.build_index(tuple(fields), self.data, self.indexes)
                    self.db.debounce_save()
                except IndexError as e:
                    raise IndexError(f"Failed to update indexes after delete: {e}")
            return len(to_delete)

    def aggregate_query(self, aggregate: Dict, conditions: Dict, group_by: str, sort: Dict) -> List[Dict]:
        if not isinstance(aggregate, dict) or not isinstance(conditions, dict):
            raise ValidationError("Aggregate and conditions must be dictionaries")
        if group_by and group_by in ["_id", "created_at", "updated_at"]:
            raise ValidationError(f"Group by field '{group_by}' is reserved")
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
                    if op not in ['$count', '$avg', '$sum', '$min', '$max']:
                        raise ValidationError(f"Invalid aggregate operator: {op}")
                    actual_field = output_field
                    for prefix in ["avg_", "sum_", "min_", "max_"]:
                        if output_field.startswith(prefix):
                            actual_field = output_field[len(prefix):]
                            break
                    if actual_field in ["_id", "created_at", "updated_at"]:
                        raise ValidationError(f"Aggregate field '{actual_field}' is reserved")
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
                if op not in ['$count', '$avg', '$sum', '$min', '$max']:
                    raise ValidationError(f"Invalid aggregate operator: {op}")
                actual_field = output_field
                for prefix in ["avg_", "sum_", "min_", "max_"]:
                    if output_field.startswith(prefix):
                        actual_field = output_field[len(prefix):]
                        break
                if actual_field in ["_id", "created_at", "updated_at"]:
                    raise ValidationError(f"Aggregate field '{actual_field}' is reserved")
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
            if field in ["_id", "created_at", "updated_at"]:
                raise ValidationError(f"Sort field '{field}' is reserved")
            if order not in ["asc", "desc"]:
                raise ValidationError(f"Invalid sort order: {order}")
            results.sort(key=lambda x: x.get(field, 0) or 0, reverse=(order == "desc"))
        
        return results

    def join_query(self, join: Dict, conditions: Dict) -> List[Dict]:
        if not isinstance(join, dict) or not isinstance(conditions, dict):
            raise ValidationError("Join and conditions must be dictionaries")
        if not self.data_loaded:
            self.load_data()
        results = []
        other_collection = self.db.collections.get(join["collection"])
        if not other_collection:
            raise CollectionError(f"Collection {join['collection']} not found")
        
        if not other_collection.data_loaded:
            other_collection.load_data()
        
        field1, field2 = join["on"].split("=")
        if field1 in ["_id", "created_at", "updated_at"] or field2 in ["_id", "created_at", "updated_at"]:
            raise ValidationError(f"Join fields '{field1}' or '{field2}' are reserved")
        for key1, record1 in self.data.items():
            if self.match_query(record1, conditions, check_ttl=True):
                for key2, record2 in other_collection.data.items():
                    if other_collection.match_query(record2, {}, check_ttl=True):
                        if record1.get(field1) == record2.get(field2):
                            joined_record = {**record1, **{f"{join['collection']}_{k}": v for k, v in record2.items()}}
                            results.append(joined_record)
        return results

    def parse_query(self, query_str: str) -> Dict:
        if not isinstance(query_str, str) or not query_str.strip():
            raise ValidationError("Query string must be non-empty")
        start_time = time.time()
        query_key = f"{self.name}:{query_str}"
        if query_key in self.db.cache:
            end_time = time.time()
            return {"results": self.db.cache[query_key], "execution_time": end_time - start_time}

        try:
            query = parse_my_query(query_str)
        except QueryError as e:
            raise QueryError(f"Failed to parse query: {e}")

        results = []
        indexed = False

        if query.action == QueryAction.INSERT:
            try:
                key = self.insert(query.data)
                results = [{"_id": key, **query.data}]
            except (ValidationError, IndexError) as e:
                raise QueryError(f"Insert query failed: {e}")
        elif query.action == QueryAction.SELECT:
            if query.filter and query.filter["type"] == "compare":
                fields = [query.filter.get("field")]
                if len(query.conditions) > 1:
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
                    try:
                        if self.match_query(record, query.conditions):
                            results.append(record)
                    except ValidationError as e:
                        raise QueryError(f"Select query failed: {e}")
            if query.sort:
                field, order = list(query.sort.items())[0]
                try:
                    results.sort(key=lambda x: float(x.get(field, 0)) if str(x.get(field, '')).replace('.','',1).isdigit() else x.get(field, ''), reverse=(order == "desc"))
                except Exception as e:
                    raise QueryError(f"Sort operation failed: {e}")
        elif query.action == QueryAction.UPDATE:
            try:
                count = self.update(query.conditions, query.data)
                results = [{"updated": count}]
            except (ValidationError, IndexError) as e:
                raise QueryError(f"Update query failed: {e}")
        elif query.action == QueryAction.DELETE:
            try:
                count = self.delete(query.conditions)
                results = [{"deleted": count}]
            except (ValidationError, IndexError) as e:
                raise QueryError(f"Delete query failed: {e}")
        elif query.action == QueryAction.INDEX:
            try:
                fields = tuple(query.index_field.split(","))
                self.create_index(fields)
                results = [{"indexed": ",".join(fields)}]
            except (ValidationError, IndexError) as e:
                raise QueryError(f"Index query failed: {e}")
        elif query.action == QueryAction.TRANSACT:
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
            except TransactionError as e:
                tx.rollback()
                results = [{"transaction": "rolled back"}]
                raise QueryError(f"Transaction query failed: {e}")
        elif query.action == QueryAction.AGGREGATE:
            try:
                results = self.aggregate_query(query.aggregate, query.conditions, query.group_by, query.sort)
            except ValidationError as e:
                raise QueryError(f"Aggregate query failed: {e}")
        elif query.action == QueryAction.JOIN:
            try:
                results = self.join_query(query.join, query.conditions)
            except (ValidationError, CollectionError) as e:
                raise QueryError(f"Join query failed: {e}")

        self.db.cache[query_key] = results
        end_time = time.time()
        return {"results": results, "execution_time": end_time - start_time}

    def create_index(self, fields: Tuple[str, ...]):
        if not fields:
            raise ValidationError("Index fields cannot be empty")
        for field in fields:
            if field in ["_id", "created_at", "updated_at"]:
                raise ValidationError(f"Index field '{field}' is reserved")
        if not self.data_loaded:
            self.load_data()
        index_key = ",".join(fields)
        try:
            IndexManager.build_index(fields, self.data, self.indexes)
            self.db.debounce_save()
        except IndexError as e:
            raise IndexError(f"Failed to create index: {e}")
