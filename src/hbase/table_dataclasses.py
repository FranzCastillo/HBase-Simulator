from dataclasses import dataclass
from datetime import datetime
from typing import List


@dataclass
class ColumnFamily:
    name: str
    data_block_encoding: str = 'NONE'
    bloom_filter_type: str = 'ROW'
    replication_scope: str = '0'
    versions: str = '1'
    compression: str = 'NONE'
    min_versions: str = '0'
    ttl: str = 'FOREVER'
    keep_deleted_cells: str = 'FALSE'
    block_size: str = '65536'
    in_memory: str = 'false'
    block_cache: str = 'true'


@dataclass
class MetaData:
    name: str
    column_families: List[ColumnFamily]
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
