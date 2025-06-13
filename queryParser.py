import re
from query import Query, QueryAction
from mydb_types import Conditions, Data

def parse_my_query(query: str) -> Query:
    q = Query()

    def parse_filter(text: str) -> dict:
        result = {"type": "compare", "field": "", "operator": "", "value": ""}
        match = re.match(r"(\w+)\s*([=><!]+)\s*('[^']*'|[0-9.]+)", text)
        if match:
            field, op, value = match.groups()
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
            if value.startswith("'") and value.endswith("'"):
                result[key] = value[1:-1]
            elif value.startswith("{"):
                ops = {}
                inner = value[1:-1]
                for op_match in re.finditer(r"(\w+)\s*:\s*([0-9.]+|\[[^\]]*\])", inner):
                    op_key, op_value = op_match.groups()
                    if op_value.startswith("["):
                        values = re.findall(r'"([^"]+)"', op_value[1:-1])
                        ops[op_key] = values
                    else:
                        ops[op_key] = float(op_value)
                result[key] = ops
            else:
                result[key] = value
        return result

    if m := re.match(r"ADD DATA \((.+)\)", query, re.I):
        q.action = QueryAction.INSERT
        q.data = {k: v[1:-1] if v.startswith("'") else v for k, v in re.findall(r"(\w+)=('[^']*'|[0-9.]+)", m.group(1))}
    elif m := re.match(r"FETCH(?: FILTER \((.+)\))?", query, re.I):
        q.action = QueryAction.SELECT
        if m.group(1):
            q.filter = parse_filter(m.group(1))
            q.conditions = parse_conditions(m.group(1))
    elif m := re.match(r"MODIFY FILTER \((.+)\) WITH \((.+)\)", query, re.I):
        q.action = QueryAction.UPDATE
        q.conditions = parse_conditions(m.group(1))
        q.data = {k: v[1:-1] if v.startswith("'") else v for k, v in re.findall(r"(\w+)=('[^']*'|[0-9.]+)", m.group(2))}
    elif m := re.match(r"REMOVE FILTER \((.+)\)", query, re.I):
        q.action = QueryAction.DELETE
        q.conditions = parse_conditions(m.group(1))
    elif m := re.match(r"INDEX FIELD ([\w,]+)", query, re.I):
        q.action = QueryAction.INDEX
        q.index_field = m.group(1)  # Support comma-separated fields
    elif m := re.match(r"TRANSACT OPS \((.+)\)", query, re.I):
        q.action = QueryAction.TRANSACT
        ops_str = m.group(1)
        for op in re.finditer(r"(?:ADD DATA \((.+?)\)|MODIFY FILTER \((.+?)\) WITH \((.+?)\)|REMOVE FILTER \((.+?)\))(?:;|$)", ops_str):
            if op.group(1):
                q.transact_ops.append(("INSERT", {}, {k: v[1:-1] if v.startswith("'") else v for k, v in re.findall(r"(\w+)=('[^']*'|[0-9.]+)", op.group(1))}))
            elif op.group(2) and op.group(3):
                q.transact_ops.append(("UPDATE", parse_conditions(op.group(2)), {k: v[1:-1] if v.startswith("'") else v for k, v in re.findall(r"(\w+)=('[^']*'|[0-9.]+)", op.group(3))}))
            elif op.group(4):
                q.transact_ops.append(("DELETE", parse_conditions(op.group(4)), {}))
    elif m := re.match(r"AGGREGATE \((.+)\)(?: FILTER \((.+)\))?(?: GROUP BY (\w+))?(?: SORT BY (\w+):(\w+))?", query, re.I):
        q.action = QueryAction.AGGREGATE
        q.aggregate = {k: v for k, v in re.findall(r"(\w+)=([$]\w+)", m.group(1))}
        if m.group(2):
            q.conditions = parse_conditions(m.group(2))
        if m.group(3):
            q.group_by = m.group(3)
        if m.group(4) and m.group(5):
            q.sort = {m.group(4): m.group(5).lower()}
    elif m := re.match(r"JOIN (\w+) ON (\w+)=(\w+)(?: FILTER \((.+)\))?", query, re.I):
        q.action = QueryAction.JOIN
        q.join = {"collection": m.group(1), "on": f"{m.group(2)}={m.group(3)}"}
        if m.group(4):
            q.conditions = parse_conditions(m.group(4))
    else:
        raise ValueError("Invalid query")
    return q
