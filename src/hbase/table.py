import bisect
import json
import os
import uuid
from datetime import datetime
from typing import List, Union, Optional, Dict

from hbase.table_dataclasses import MetaData, RowEntry, ColumnFamily
from hbase.table_decorators import update_timestamp


def parse_data_to_dict(data: List[RowEntry]) -> dict:
    new_dict = {}
    for entry in data:
        if entry.row_key not in new_dict:
            new_dict[entry.row_key] = {}
        new_dict[entry.row_key][entry.column_family] = entry.column_qualifiers
    return new_dict


def load_data(data: dict) -> List[RowEntry]:
    new_data = []
    for row_key, column_families in data.items():
        for column_family, column_qualifiers in column_families.items():
            new_data.append(
                RowEntry(
                    row_key=row_key,
                    column_family=column_family,
                    column_qualifiers=column_qualifiers
                )
            )
    return new_data


class Table:
    def __init__(self, table_name: Optional[str] = None, column_families: Optional[List[str]] = None):
        self.metadata = MetaData(
            name=table_name,
            column_families=[ColumnFamily(name=cf) for cf in (column_families or [])],
            id=str(uuid.uuid4()),
            is_disabled=False,
            created_at=datetime.now(),
            updated_at=datetime.now(),
            n_rows=0,
        )
        self.data: List[RowEntry] = []

    def load(self, file_path: str) -> None:
        with open(file_path, "r") as f:
            data = json.load(f)
        
        self.metadata = MetaData(
            name=data["metadata"]["name"],
            column_families=[ColumnFamily(**cf) for cf in data["metadata"]["column_families"]],
            id=data["metadata"]["id"],
            is_disabled=data["metadata"]["is_disabled"],
            created_at=datetime.fromisoformat(data["metadata"]["created_at"]),
            updated_at=datetime.fromisoformat(data["metadata"]["updated_at"]),
            n_rows=data["metadata"]["n_rows"],
        )
        self.data = load_data(data["data"])

    def to_json(self) -> str:
        metadata_dict = self.metadata.__dict__.copy()
        metadata_dict["column_families"] = [cf.__dict__ for cf in metadata_dict["column_families"]]
        metadata_dict["created_at"] = self.metadata.created_at.isoformat()
        metadata_dict["updated_at"] = self.metadata.updated_at.isoformat()
        json_str = {"metadata": metadata_dict, "data": parse_data_to_dict(self.data)}
        return json.dumps(json_str, indent=4)

    def save(self, save_dir: str) -> None:
        os.makedirs(save_dir, exist_ok=True)
        path = os.path.join(save_dir, f"{self.metadata.name}.json")
        with open(path, "w") as f:
            f.write(self.to_json())

    @update_timestamp
    def enable(self) -> None:
        self.metadata.is_disabled = False

    @update_timestamp
    def disable(self) -> None:
        self.metadata.is_disabled = True

    def get_column_family(self, column_family_name: str) -> Optional[ColumnFamily]:
        for cf in self.metadata.column_families:
            if cf.name == column_family_name:
                return cf
        return None

    @update_timestamp
    def create_column_family(self, column_family_name: str, properties: Optional[dict] = None) -> None:
        if self.metadata.is_disabled:
            raise Exception("Failed to create column family: Table is disabled.")
        if self.get_column_family(column_family_name):
            raise Exception(f"Column family '{column_family_name}' already exists")

        properties = properties or {}
        self.metadata.column_families.append(ColumnFamily(name=column_family_name, **properties))

    @update_timestamp
    def update_column_family(self, column_family_name: str, properties: dict) -> None:
        if self.metadata.is_disabled:
            raise Exception("Failed to update column family: Table is disabled.")
        
        cf = self.get_column_family(column_family_name)
        if not cf:
            raise Exception(f"Column family '{column_family_name}' not found")

        valid_keys = ColumnFamily.get_valid_keys()
        for key, value in properties.items():
            if key not in valid_keys:
                raise Exception(f"Invalid property '{key}' for a column family")
            setattr(cf, key, value)

    @update_timestamp
    def delete_column_family(self, column_family_name: str) -> None:
        cf = self.get_column_family(column_family_name)
        if not cf:
            raise Exception(f"Column family '{column_family_name}' not found")

        self.metadata.column_families.remove(cf)

    @update_timestamp
    def put(self, row_key: str, column_family: str, column_qualifier: str, value: str) -> None:
        if self.metadata.is_disabled:
            raise Exception("Failed to put data: Table is disabled.")

        if not self.get_column_family(column_family):
            raise Exception(f"Column family '{column_family}' not found")

        # Si el row_key y column_family ya existen, actualiza la entrada
        for entry in self.data:
            if entry.row_key == row_key:
                if entry.column_family == column_family:
                    if column_qualifier in entry.column_qualifiers:
                        entry.column_qualifiers[column_qualifier]["n_versions"] += 1
                        entry.column_qualifiers[column_qualifier][
                            f"version{entry.column_qualifiers[column_qualifier]['n_versions']}"
                        ] = {
                            "timestamp": datetime.now().isoformat(),
                            "value": value
                        }
                    else:
                        entry.column_qualifiers[column_qualifier] = {
                            "n_versions": 1,
                            "version1": {
                                "timestamp": datetime.now().isoformat(),
                                "value": value
                            }
                        }
                    return
                # Si el row_key existe pero con otra column_family, agregar nueva entrada
                elif column_family not in [e.column_family for e in self.data if e.row_key == row_key]:
                    new_entry = RowEntry(
                        row_key=row_key,
                        column_family=column_family,
                        column_qualifiers={
                            column_qualifier: {
                                "n_versions": 1,
                                "version1": {
                                    "timestamp": datetime.now().isoformat(),
                                    "value": value
                                }
                            }
                        }
                    )
                    self.data.append(new_entry)
                    self.metadata.n_rows += 1
                    return

        # Si el row_key no existe, crea una nueva entrada
        new_entry = RowEntry(
            row_key=row_key,
            column_family=column_family,
            column_qualifiers={
                column_qualifier: {
                    "n_versions": 1,
                    "version1": {
                        "timestamp": datetime.now().isoformat(),
                        "value": value
                    }
                }
            }
        )
        i = bisect.bisect_left([e.row_key for e in self.data], new_entry.row_key)
        self.data.insert(i, new_entry)
        self.metadata.n_rows += 1

    @update_timestamp
    def delete(self, row_key: str, column_family: str, column_qualifier: str) -> None:
        if self.metadata.is_disabled:
            raise Exception("Failed to delete data: Table is disabled.")
        if not self.get_column_family(column_family):
            raise Exception(f"Column family '{column_family}' not found")

        for entry in self.data:
            if entry.row_key == row_key and entry.column_family == column_family:
                if column_qualifier in entry.column_qualifiers:
                    del entry.column_qualifiers[column_qualifier]

                    if not entry.column_qualifiers:
                        self.data.remove(entry)
                        self.metadata.n_rows -= 1
                    return

        raise Exception(f"Row key '{row_key}' not found")

    @update_timestamp
    def delete_all(self, row_key: str) -> int:
        if self.metadata.is_disabled:
            raise Exception("Failed to delete all data: Table is disabled.")

        entries_to_delete = [entry for entry in self.data if entry.row_key == row_key]

        for entry in entries_to_delete:
            self.data.remove(entry)
            self.metadata.n_rows -= 1

        return len(entries_to_delete)

    def scan(self) -> str:
        if self.metadata.is_disabled:
            raise Exception("Failed to scan data: Table is disabled.")

        scan_str = "ROW \t\t\t COLUMN+CELL\n"
        for entry in self.data:
            for cq, val in entry.column_qualifiers.items():
                for version, data in val.items():
                    if version == "n_versions":
                        continue
                    scan_str += f"{entry.row_key}\t\t\t column={entry.column_family}:{cq}, timestamp={data['timestamp']}, value={data['value']}\n"
        return scan_str

    def count(self) -> int:
        if self.metadata.is_disabled:
            raise Exception("Failed to count rows: Table is disabled.")
        return self.metadata.n_rows
