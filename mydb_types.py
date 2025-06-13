from typing import Dict, List, Union

Data = Dict[str, str]
Record = Dict[str, str]
Records = Dict[str, Record]
Index = Dict[str, List[str]]
Indexes = Dict[str, Index]
Conditions = Dict[str, Union[str, Dict[str, Union[float, List[str]]]]]