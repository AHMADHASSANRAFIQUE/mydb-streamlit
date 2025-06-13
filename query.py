from enum import Enum
from typing import Dict, List, Tuple
from mydb_types import Conditions, Data

class QueryAction(Enum):
    INSERT = "INSERT"
    SELECT = "SELECT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    INDEX = "INDEX"
    TRANSACT = "TRANSACT"
    AGGREGATE = "AGGREGATE"
    JOIN = "JOIN"

class Query:
    def __init__(self):
        self.action: QueryAction = None
        self.conditions: Conditions = {}
        self.data: Data = {}
        self.index_field: str = ""
        self.transact_ops: List[Tuple[str, Conditions, Data]] = []
        self.filter: dict = None
        self.aggregate: Dict[str, str] = {}  # e.g., {"age": "$avg", "count": "$count"}
        self.group_by: str = ""
        self.sort: Dict[str, str] = {}  # e.g., {"age": "asc"}
        self.join: Dict[str, str] = {}  # e.g., {"collection": "course", "on": "roll_no=student_id"}
