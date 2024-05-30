import os
import re
from typing import List

from hbase.table import Table


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

    def create_table(self, table_name: str, column_families: list[str]) -> None:
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
            raise Exception("Tables must be disabled before they can be dropped")

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

    def truncate_table(self, table_name: str) -> None:
        table = self.get_table(table_name)

        cfs = table.metadata.column_families
        print(f"Truncating '{table_name}' (it may take a while):")
        print(f" - Disabling table...")
        self.disable_table(table_name)
        print(f" - Truncating table...")
        self.drop_table(table_name)
        self.create_table(table_name, [cf.name for cf in cfs])

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

    def alter_table(self, table_name: str, properties: dict) -> None:
        table = self.get_table(table_name)
        cf_name = properties.get('name')
        if not cf_name:
            raise Exception("Column family name is required. {name => <cf>}")
        properties.pop('name')

        # Handle Column Family Delete
        if properties.get('method') == 'delete':
            # Remove the column family from the list
            table.delete_column_family(cf_name)

            table.save(self.data_dir)
            return

        cf = self.get_table(table_name).get_column_family(cf_name)

        # Handle create and update
        # If the column family doesn't exist, create it
        if not cf:
            self.get_table(table_name).create_column_family(cf_name, properties)
        else:
            self.get_table(table_name).update_column_family(cf_name, properties)

        table.save(self.data_dir)

    def put(self, table_name: str, row_key: str, column_family: str, column_qualifier: str, value: str) -> None:
        table = self.get_table(table_name)

        table.put(row_key, column_family, column_qualifier, value)

        table.save(self.data_dir)

    def delete(self, table_name: str, row_key: str, column_family: str, column_qualifier: str) -> None:
        table = self.get_table(table_name)

        table.delete(row_key, column_family, column_qualifier)

        table.save(self.data_dir)

    def delete_all(self, table_name: str, row_key: str) -> int:
        table = self.get_table(table_name)

        n_rows = table.delete_all(row_key)

        table.save(self.data_dir)

        return n_rows

    def scan(self, table_name: str) -> str:
        table = self.get_table(table_name)

        return table.scan()

    def count(self, table_name: str) -> int:
        return self.get_table(table_name).count()

    def get_row(self, table_name: str, row_key: str, column_family: str = None, column_qualifier: str = None) -> tuple[str, int]:
        table = self.get_table(table_name)
        if table.metadata.is_disabled:
            raise Exception("Failed 1 action: NotServingRegionException: 1 time,")

        n_rows = 0
        return_str = "COLUMN\t\t\t\t\t\tCELL\n"
        if not column_family or not column_qualifier:  # Show all columns
            for entry in table.data:
                if entry.row_key == row_key:
                    for cq, values in entry.column_qualifiers.items():
                        last_version = f"version{values['n_versions']}"
                        for version, value in values.items():
                            if version == "n_versions" or version != last_version:
                                continue
                            return_str += f"{entry.column_family}:{cq}\t\t\t\ttimestamp={value['timestamp']}, value={value['value']}\n"
                            n_rows += 1

            return return_str, n_rows

        # Show only the specified column
        for entry in table.data:
            if entry.row_key == row_key and entry.column_family == column_family:
                if column_qualifier in entry.column_qualifiers:
                    values = entry.column_qualifiers[column_qualifier]
                    last_version = f"version{values['n_versions']}"
                    for version, value in values.items():
                        if version == "n_versions" or version != last_version:
                            continue
                        return_str += f"{entry.column_family}:{column_qualifier}\t\t\t\ttimestamp={value['timestamp']}, value={value['value']}\n"

        return return_str, 1
