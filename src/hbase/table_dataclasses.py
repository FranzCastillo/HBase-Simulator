from dataclasses import dataclass
from datetime import datetime
from typing import List


@dataclass
class MetaData:
    name: str
    column_families: List[str]
    id: str
    is_disabled: bool
    created_at: datetime
    updated_at: datetime
    n_rows: int


@dataclass
class RowEntry:
    rowKey: str
    column_family: str
    column_qualifier: str
    value: str
    timestamp: datetime
