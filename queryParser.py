import re
from query import Query, QueryAction
from mydb_types import Conditions, Data
from errors import QueryError, ValidationError

def parse_my_query(query: str) -> Query:
    if not query or not isinstance(query, str):
        raise ValidationError("Query must be a non-empty string")
    
    q = Query()

    def parse_filter(text: str) -> dict:
        result = {"type": "compare", "field": "", "operator": "", "value": ""}
        match = re.match(r"(\w+)\s*([=><!]+)\s*('[^']*'|[0-9.]+)", text)
        if not match:
            raise QueryError(f"Invalid filter syntax: {text}")
        field, op, value = match.groups()
        if field in ["_id", "created_at", "updated_at"]:
            raise ValidationError(f"Field '{field}' is reserved and cannot be used in filter")
        result["field"] = field
        result["operator"] = op
        result["value"] = value[1:-1] if value.startswith("'") else float(value)
        return result

    def parse_conditions(text: str) -> dict:
        result = {}
        if not text:
            return result
        for pair in re.finditer(r"(\w+)\s*([=><!]+|[$]\w+)\s*('[^']*'|[0-9.]+|\{[^{}]*\})", text):
            key, op, value = pair.groups()
            if key in ["_id", "created_at", "updated_at"]:
                raise ValidationError(f"Field '{key}' is reserved and cannot be used in conditions")
            if value.startswith("'") and value.endswith("'"):
                result[key] = value[1:-1]
            elif value.startswith("{"):
                ops = {}
                inner = value[1:-1]
                for op_match in re.finditer(r"(\w+)\s*:\s*([0-9.]+|\[[^\]]*\])", inner):
                    op_key, op_value = op_match.groups()
                    if op_key not in ['$gt', '$gte', '$lt', '$lte', '$in']:
                        raise ValidationError(f"Invalid operator: {op_key}")
                    if op_value.startswith("["):
                        values = re.findall(r'"([^"]+)"', op_value[1:-1])
                        ops[op_key] = values
                    else:
                        try:
                            ops[op_key] = float(op_value)
                        except ValueError:
                            raise ValidationError(f"Invalid numeric value for {op_key}: {op_value}")
                result[key] = ops
            else:
                try:
                    result[key] = float(value)
                except ValueError:
                    result[key] = value
        return result

    query = query.strip()
    if m := re.match(r"ADD DATA \((.+)\)", query, re.I):
        q.action = QueryAction.INSERT
        q.data = {k: v[1:-1] if v.startswith("'") else v for k, v in re.findall(r"(\w+)=('[^']*'|[0-9.]+)", m.group(1))}
        if not q.data:
            raise ValidationError("Insert data cannot be empty")
    elif m := re.match(r"FETCH(?: FILTER \((.+)\))?", query, re.I):
        q.action = QueryAction.SELECT
        if m.group(1):
            q.filter = parse_filter(m.group(1))
            q.conditions = parse_conditions(m.group(1))
    elif m := re.match(r"MODIFY FILTER \((.+)\) WITH \((.+)\)", query, re.I):
        q.action = QueryAction.UPDATE
        q.conditions = parse_conditions(m.group(1))
        q.data = {k: v[1:-1] if v.startswith("'") else v for k, v in re.findall(r"(\w+)=('[^']*'|[0-9.]+)", m.group(2))}
        if not q.data:
            raise ValidationError("Update data cannot be empty")
    elif m := re.match(r"REMOVE FILTER \((.+)\)", query, re.I):
        q.action = QueryAction.DELETE
        q.conditions = parse_conditions(m.group(1))
        if not q.conditions:
            raise ValidationError("Delete conditions cannot be empty")
    elif m := re.match(r"INDEX FIELD ([\w,]+)", query, re.I):
        q.action = QueryAction.INDEX
        q.index_field = m.group(1)
        fields = q.index_field.split(",")
        for field in fields:
            if not re.match(r"^[a-zA-Z0-9_]{1,50}$", field):
                raise ValidationError(f"Invalid index field: {field}")
            if field in ["_id", "created_at", "updated_at"]:
                raise ValidationError(f"Index field '{field}' is reserved")
    elif m := re.match(r"TRANSACT OPS \((.+)\)", query, re.I):
        q.action = QueryAction.TRANSACT
        ops_str = m.group(1)
        for op in re.finditer(r"(?:ADD DATA \((.+?)\)|MODIFY FILTER \((.+?)\) WITH \((.+?)\)|REMOVE FILTER \((.+?)\))(?:;|$)", ops_str):
            if op.group(1):
                data = {k: v[1:-1] if v.startswith("'") else v for k, v in re.findall(r"(\w+)=('[^']*'|[0-9.]+)", op.group(1))}
                if not data:
                    raise ValidationError("Transaction insert data cannot be empty")
                q.transact_ops.append(("INSERT", {}, data))
            elif op.group(2) and op.group(3):
                conditions = parse_conditions(op.group(2))
                data = {k: v[1:-1] if v.startswith("'") else v for k, v in re.findall(r"(\w+)=('[^']*'|[0-9.]+)", op.group(3))}
                if not conditions or not data:
                    raise ValidationError("Transaction update requires non-empty conditions and data")
                q.transact_ops.append(("UPDATE", conditions, data))
            elif op.group(4):
                conditions = parse_conditions(op.group(4))
                if not conditions:
                    raise ValidationError("Transaction delete conditions cannot be empty")
                q.transact_ops.append(("DELETE", conditions, {}))
    elif m := re.match(r"AGGREGATE \((.+)\)(?: FILTER \((.+)\))?(?: GROUP BY (\w+))?(?: SORT BY (\w+):(\w+))?", query, re.I):
        q.action = QueryAction.AGGREGATE
        q.aggregate = {k: v for k, v in re.findall(r"(\w+)=([$]\w+)", m.group(1))}
        if not q.aggregate:
            raise ValidationError("Aggregate operations cannot be empty")
        if m.group(2):
            q.conditions = parse_conditions(m.group(2))
        if m.group(3):
            if m.group(3) in ["_id", "created_at", "updated_at"]:
                raise ValidationError(f"Group by field '{m.group(3)}' is reserved")
            q.group_by = m.group(3)
        if m.group(4) and m.group(5):
            if m.group(4) in ["_id", "created_at", "updated_at"]:
                raise ValidationError(f"Sort field '{m.group(4)}' is reserved")
            if m.group(5).lower() not in ["asc", "desc"]:
                raise ValidationError(f"Invalid sort order: {m.group(5)}")
            q.sort = {m.group(4): m.group(5).lower()}
    elif m := re.match(r"JOIN (\w+) ON (\w+)=(\w+)(?: FILTER \((.+)\))?", query, re.I):
        q.action = QueryAction.JOIN
        if m.group(1) in ["_id", "created_at", "updated_at"]:
            raise ValidationError(f"Join collection '{m.group(1)}' is reserved")
        if m.group(2) in ["_id", "created_at", "updated_at"] or m.group(3) in ["_id", "created_at", "updated_at"]:
            raise ValidationError(f"Join fields '{m.group(2)}' or '{m.group(3)}' are reserved")
        q.join = {"collection": m.group(1), "on": f"{m.group(2)}={m.group(3)}"}
        if m.group(4):
            q.conditions = parse_conditions(m.group(4))
    else:
        raise QueryError(f"Invalid query syntax: {query}")
    
    q.validate()
    return q
