from enum import Enum
from typing import Dict, List, Tuple
from mydb_types import Conditions, Data
from errors import ValidationError, QueryError

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
        self.aggregate: Dict[str, str] = {}
        self.group_by: str = ""
        self.sort: Dict[str, str] = {}
        self.join: Dict[str, str] = {}

    def validate(self):
        """Validate query parameters before execution."""
        if not self.action:
            raise QueryError("Query action must be specified")

        # Validate index fields
        if self.action == QueryAction.INDEX and not self.index_field:
            raise ValidationError("Index field cannot be empty")

        # Validate data for INSERT and UPDATE
        if self.action in [QueryAction.INSERT, QueryAction.UPDATE]:
            if not self.data:
                raise ValidationError("Data cannot be empty for INSERT or UPDATE")
            for key, value in self.data.items():
                if not isinstance(key, str) or not key.strip():
                    raise ValidationError(f"Invalid field name: {key}")
                if not isinstance(value, str) and value is not None:
                    raise ValidationError(f"Field value must be string or None, got {type(value)} for {key}")

        # Validate conditions
        if self.conditions:
            for key, value in self.conditions.items():
                if not isinstance(key, str) or not key.strip():
                    raise ValidationError(f"Invalid condition field: {key}")
                if isinstance(value, dict):
                    for op, val in value.items():
                        if op not in ['$gt', '$gte', '$lt', '$lte', '$in']:
                            raise ValidationError(f"Invalid operator: {op}")
                        if op == '$in' and not isinstance(val, list):
                            raise ValidationError(f"$in operator requires a list, got {type(val)}")
                        elif op != '$in' and not isinstance(val, (int, float)):
                            raise ValidationError(f"Operator {op} requires numeric value, got {type(val)}")

        # Validate aggregate
        if self.action == QueryAction.AGGREGATE:
            if not self.aggregate:
                raise ValidationError("Aggregate operations cannot be empty")
            valid_ops = ['$count', '$avg', '$sum', '$min', '$max']
            for field, op in self.aggregate.items():
                if op not in valid_ops:
                    raise ValidationError(f"Invalid aggregate operator: {op}")

        # Validate join
        if self.action == QueryAction.JOIN:
            if not self.join.get('collection') or not self.join.get('on'):
                raise ValidationError("Join requires collection and on clause")
            if '=' not in self.join['on']:
                raise ValidationError("Join 'on' clause must contain '='")
