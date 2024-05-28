import os
import re
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

    def list_tables(self, regex: str = None) -> List[str]:
        table_names = []
        for table in self.tables:
            if not regex or re.match(regex, table.metadata.name):
                table_names.append(table.metadata.name)

        return table_names

    def get_table(self, table_name: str) -> Table:
        for table in self.tables:
            if table.metadata.name == table_name:
                return table

        raise Exception(f"Table '{table_name}' not found")

    def disable_table(self, table_name: str) -> None:
        table = self.get_table(table_name)
        if table.metadata.is_disabled:
            print(f"Table '{table_name}' is already disabled")
            return

        table.disable()
        table.save(self.data_dir)

    def is_table_disabled(self, table_name: str) -> bool:
        table = self.get_table(table_name)
        return table.metadata.is_disabled

    def enable_table(self, table_name: str) -> None:
        table = self.get_table(table_name)
        if not table.metadata.is_disabled:
            print(f"Table '{table_name}' is already enabled")
            return

        table.enable()
        table.save(self.data_dir)

    def is_table_enabled(self, table_name: str) -> bool:
        table = self.get_table(table_name)
        return not table.metadata.is_disabled
