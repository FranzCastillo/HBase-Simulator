import bisect  # To insert elements in a sorted list
import json
import os
import uuid  # To create unique IDs

from src.hbase.table_dataclasses import *
from src.hbase.table_decorators import update_timestamp


def parse_data_to_dict(data: list[RowEntry]) -> dict:
    new_dict = {}
    for entry in data:
        new_dict[entry.row_key] = {entry.column_family: entry.column_qualifiers}

    return new_dict


class Table:
    def __init__(self, table_name: str = None, column_families: list[str] = None):
        self.metadata = MetaData(  # Create a Metadata object
            name=table_name,
            column_families=[ColumnFamily(name=cf) for cf in column_families or []],
            # Create a list of ColumnFamily objects
            id=str(uuid.uuid4()), is_disabled=False,
            created_at=datetime.now(),
            updated_at=datetime.now(),
            n_rows=0,
        )

        self.data: List[RowEntry] = []  # Create a list of RowEntry objects

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

        # TODO: Load data as RowEntries
        self.data = []

    def to_json(self) -> str:
        # Convert the datetime objects to strings
        metadata_dict = self.metadata.__dict__.copy()
        metadata_dict["column_families"] = [cf.__dict__ for cf in metadata_dict["column_families"]]
        metadata_dict["created_at"] = self.metadata.created_at.isoformat()
        metadata_dict["updated_at"] = self.metadata.updated_at.isoformat()
        json_str = {"metadata": metadata_dict, "data": parse_data_to_dict(self.data)}

        return json.dumps(json_str, indent=4)

    def save(self, save_dir: str) -> None:
        # Check if directory exists, if not, create it
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

    def get_column_family(self, column_family_name: str) -> ColumnFamily | None:
        for cf in self.metadata.column_families:
            if cf.name == column_family_name:
                return cf

        return None

    @update_timestamp
    def create_column_family(self, column_family_name: str, properties: dict) -> None:
        if self.get_column_family(column_family_name):
            raise Exception(f"Column family '{column_family_name}' already exists")

        if not properties:
            self.metadata.column_families.append(ColumnFamily(name=column_family_name))
        else:
            self.metadata.column_families.append(ColumnFamily(name=column_family_name, **properties))

    @update_timestamp
    def update_column_family(self, column_family_name: str, properties: dict) -> None:
        cf = self.get_column_family(column_family_name)
        if not cf:
            raise Exception(f"Column family '{column_family_name}' not found")

        valid_keys = ColumnFamily.get_valid_keys()
        for key, value in properties.items():
            if key not in valid_keys:  # To avoid setting invalid properties
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
        # Check if the column family exists
        if not self.get_column_family(column_family):
            raise Exception(f"Column family '{column_family}' not found")

        # If the row key already exists, update the entry
        for entry in self.data:
            if entry.row_key == row_key and entry.column_family == column_family:
                # If the column qualifier already exists, update the value
                if column_qualifier in entry.column_qualifiers:
                    entry.column_qualifiers[column_qualifier]["n_timestmaps"] += 1
                    entry.column_qualifiers[column_qualifier][
                        f"timestamp{entry.column_qualifiers[column_qualifier]['n_timestmaps']}"
                    ] = value
                    return

                # If the column qualifier does not exist, create it
                entry.column_qualifiers[column_qualifier] = {
                    "n_timestmaps": 1,
                    "timestamp1": value
                }
                return

        # If the row key does not exist, create a new entry
        entry = RowEntry(
            row_key=row_key,
            column_family=column_family,
            column_qualifiers={
                column_qualifier: {
                    "n_timestmaps": 1,
                    "timestamp1": value
                }
            }
        )

        # Use bisect to find the insertion point for the new entry
        i = bisect.bisect_left([e.row_key for e in self.data], entry.row_key)
        self.data.insert(i, entry)

        self.metadata.n_rows += 1
