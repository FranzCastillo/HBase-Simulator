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

    def drop_table(self, table_name: str) -> None:
        table = self.get_table(table_name)

        if not table.metadata.is_disabled:
            raise Exception(f"Table '{table_name}' must be disabled before it can be dropped")

        self.tables.remove(table)  # Remove it from the list

        os.remove(os.path.join(self.data_dir, f"{table_name}.json"))  # Remove the file

    def drop_all_tables(self, regex: str) -> int:
        tables = self.list_tables(regex)

        can_be_disabled = True
        # Print table names
        for table in tables:
            text = table
            if not self.is_table_disabled(text):
                text += " must be disabled before it can be dropped"
                can_be_disabled = False
            print(text)

        if not can_be_disabled:
            print("Disable the tables before dropping them")
            return -1

        # Ask for confirmation
        print(f"Drop the above {len(tables)} tables? (y/n)")
        answer = input()
        if answer != "y":
            print("Tables not dropped")
            return 0

        # Drop the tables
        for table in tables:
            self.drop_table(table)

        return len(tables)

    def describe_table(self, table_name: str) -> tuple[str, int]:
        table = self.get_table(table_name)

        table_description = f"Table {table.metadata.name} is {'DISABLED' if table.metadata.is_disabled else 'ENABLED'}\n"
        table_description += table.metadata.name
        table_description += "\nCOLUMN FAMILIES DESCRIPTION\n"
        for column_family in table.metadata.column_families:
            table_description += "{"
            table_description += f"NAME => '{column_family.name}', "
            table_description += f"DATA_BLOCK_ENCODING => '{column_family.data_block_encoding}', "
            table_description += f"BLOOMFILTER => '{column_family.bloomfilter}', "
            table_description += f"REPLICATION_SCOPE => '{column_family.replication_scope}', "
            table_description += f"VERSIONS => '{column_family.versions}', "
            table_description += f"COMPRESSION => '{column_family.compression}', "
            table_description += f"MIN_VERSIONS => '{column_family.min_versions}', "
            table_description += f"TTL => '{column_family.ttl}', "
            table_description += f"KEEP_DELETED_CELLS => '{column_family.keep_deleted_cells}', "
            table_description += f"BLOCK_SIZE => '{column_family.block_size}', "
            table_description += f"IN_MEMORY => '{column_family.in_memory}', "
            table_description += f"BLOCK_CACHE => '{column_family.block_cache}'"
            table_description += "}\n"
        table_description = table_description[:-1]  # Remove the last newline

        return table_description, len(table.metadata.column_families)
