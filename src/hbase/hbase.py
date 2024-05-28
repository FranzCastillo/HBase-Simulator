import os
from typing import List

from src.hbase.table import Table


def load_tables(data_dir: str) -> List[Table]:
    tables = []

    for file in os.listdir(data_dir):
        if file.endswith(".json"):
            table = Table()
            table.load(os.path.join(data_dir, file))
            tables.append(table)

    return tables


class Hbase:
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self.tables: List[Table] = load_tables(data_dir)

    def create_table(self, table_name, column_families) -> None:
        new_table = Table(table_name, column_families)

        # Save the table to the data directory
        new_table.save(self.data_dir)

        self.tables.append(new_table)

    def list_tables(self) -> List[str]:
        return [table.metadata.name for table in self.tables]